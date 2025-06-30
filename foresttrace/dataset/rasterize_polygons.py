# foresttrace/foresttrace/dataset/rasterize_polygons.py

import argparse
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import mercantile
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds
from shapely.geometry import box
from tqdm import tqdm


def load_polygons(geojson_path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(geojson_path)
    return gdf.to_crs(epsg=3857)  # Web Mercator (EPSG:3857)


def get_tile_bounds_and_transform(tile: mercantile.Tile, size: int = 256) -> Tuple[box, rasterio.Affine]:
    bounds = mercantile.bounds(tile)
    geom = box(bounds.west, bounds.south, bounds.east, bounds.north)
    transform = from_bounds(bounds.west, bounds.south, bounds.east, bounds.north, size, size)
    return geom, transform


def rasterize_tile(tile: mercantile.Tile, polygons: gpd.GeoDataFrame, out_path: Path) -> None:
    tile_geom, transform = get_tile_bounds_and_transform(tile)

    clipped = polygons[polygons.intersects(tile_geom)]

    if clipped.empty:
        mask = np.zeros((256, 256), dtype=np.uint8)
    else:
        mask = rasterize(
            [(geom, 1) for geom in clipped.geometry.intersection(tile_geom)],
            out_shape=(256, 256),
            transform=transform,
            fill=0,
            dtype="uint8",
        )

    profile = {
        "driver": "GTiff",
        "height": 256,
        "width": 256,
        "count": 1,
        "dtype": "uint8",
        "crs": "EPSG:3857",
        "transform": transform,
    }

    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(mask, 1)


def main(tile_dir: Path, polygon_path: Path, output_dir: Path) -> None:
    polygons = load_polygons(polygon_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    tile_files = list(tile_dir.glob("*.png")) + list(tile_dir.glob("*.jpg"))

    for tile_file in tqdm(tile_files, desc="Rasterizing tiles"):
        try:
            z, x, y = map(int, tile_file.stem.split("_"))
            tile = mercantile.Tile(x=x, y=y, z=z)
            out_path = output_dir / f"{z}_{x}_{y}_mask.tif"
            rasterize_tile(tile, polygons, out_path)
        except Exception as e:
            print(f"Failed to process tile {tile_file.name}: {e}")


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
