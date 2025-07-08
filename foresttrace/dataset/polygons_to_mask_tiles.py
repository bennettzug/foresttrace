import argparse
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import mercantile
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from rasterio.warp import transform_bounds
from shapely.geometry import box
from tqdm import tqdm


def load_polygons(geojson_path: Path) -> gpd.GeoDataFrame:
    """Load polygons and ensure they're in EPSG:4326 (WGS84)"""
    gdf = gpd.read_file(geojson_path)
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs(epsg=4326)
    return gdf


def get_tile_bounds_web_mercator(tile: mercantile.Tile) -> Tuple[float, float, float, float]:
    """Get tile bounds in Web Mercator (EPSG:3857)"""
    # Get bounds in WGS84
    bounds_wgs84 = mercantile.bounds(tile)

    # Transform to Web Mercator
    bounds_3857 = transform_bounds(
        "EPSG:4326", "EPSG:3857", bounds_wgs84.west, bounds_wgs84.south, bounds_wgs84.east, bounds_wgs84.north
    )

    return bounds_3857


def rasterize_tile(
    tile: mercantile.Tile, polygons: gpd.GeoDataFrame, out_path: Path, resolution: float = 1.1943285668550503
) -> None:
    """Rasterize polygons for a specific tile"""

    # Get tile bounds in Web Mercator
    west, south, east, north = get_tile_bounds_web_mercator(tile)

    # Create tile geometry in Web Mercator for intersection
    tile_geom_3857 = box(west, south, east, north)

    # Convert polygons to Web Mercator for proper geometric operations
    polygons_3857 = polygons.to_crs(epsg=3857)

    # Find polygons that intersect this tile
    clipped = polygons_3857[polygons_3857.intersects(tile_geom_3857)]

    # Create transform for 256x256 pixels
    transform = from_bounds(west, south, east, north, 256, 256)

    if clipped.empty:
        # No polygons in this tile
        mask = np.zeros((256, 256), dtype=np.uint8)
    else:
        # Clip polygons to tile bounds and rasterize
        clipped_geoms = []
        for geom in clipped.geometry:
            try:
                intersected = geom.intersection(tile_geom_3857)
                if not intersected.is_empty:
                    clipped_geoms.append((intersected, 1))
            except Exception:
                # Skip problematic geometries
                continue

        if clipped_geoms:
            mask = rasterize(
                clipped_geoms,
                out_shape=(256, 256),
                transform=transform,
                fill=0,
                dtype="uint8",
                all_touched=True,  # This helps capture thin polygons
            )
        else:
            mask = np.zeros((256, 256), dtype=np.uint8)

    # Save with proper georeferencing
    profile = {
        "driver": "GTiff",
        "height": 256,
        "width": 256,
        "count": 1,
        "dtype": "uint8",
        "crs": "EPSG:3857",
        "transform": transform,
        "compress": "lzw",  # Add compression to save space
    }

    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(mask, 1)


def main(tile_dir: Path, polygon_path: Path, output_dir: Path) -> None:
    """Main function to process all tiles"""

    print("Loading polygons...")
    polygons = load_polygons(polygon_path)
    print(f"Loaded {len(polygons)} polygons in CRS: {polygons.crs}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all NAIP tile files
    tile_files = list(tile_dir.glob("*.png")) + list(tile_dir.glob("*.jpg"))
    print(f"Found {len(tile_files)} tile files")

    successful = 0
    failed = 0

    for tile_file in tqdm(tile_files, desc="Rasterizing tiles"):
        try:
            # Parse tile coordinates from filename
            parts = tile_file.stem.split("_")
            if len(parts) != 3:
                print(f"Skipping {tile_file.name}: invalid filename format")
                failed += 1
                continue

            z, x, y = map(int, parts)

            if z != 17:
                print(f"Skipping {tile_file.name}: not zoom level 17")
                failed += 1
                continue

            tile = mercantile.Tile(x=x, y=y, z=z)
            out_path = output_dir / f"{z}_{x}_{y}_mask.tif"

            rasterize_tile(tile, polygons, out_path)
            successful += 1

        except Exception as e:
            print(f"Failed to process tile {tile_file.name}: {e}")
            failed += 1

    print(f"\n✓ Successfully processed {successful} tiles")
    if failed > 0:
        print(f"⚠ Failed to process {failed} tiles")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rasterize forest polygons to mask tiles aligned with NAIP imagery.",
    )
    parser.add_argument(
        "--tiles",
        type=str,
        required=True,
        help="Directory with NAIP tile images",
    )
    parser.add_argument(
        "--polygons",
        type=str,
        required=True,
        help="GeoJSON file with forest polygons",
    )
    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Directory to save raster mask tiles",
    )
    args = parser.parse_args()

    main(Path(args.tiles), Path(args.polygons), Path(args.out))
