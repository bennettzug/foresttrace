import argparse
from pathlib import Path

import geopandas as gpd
from pyproj import Transformer
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds


def rasterize_to_mask(
    polygon_path: Path,
    output_path: Path,
    bbox_wgs84: tuple[float, float, float, float],
    resolution: float = 1.0,
) -> None:
    # Load polygons and reproject to EPSG:3857
    polygons = gpd.read_file(polygon_path).to_crs(epsg=3857)

    # Transform bbox from WGS84 to EPSG:3857
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    west_m, south_m = transformer.transform(bbox_wgs84[0], bbox_wgs84[1])
    east_m, north_m = transformer.transform(bbox_wgs84[2], bbox_wgs84[3])

    # Compute output raster shape
    width = int((east_m - west_m) / resolution)
    height = int((north_m - south_m) / resolution)
    transform = from_bounds(west_m, south_m, east_m, north_m, width, height)

    print(f"Raster size: {width} x {height}")
    print(f"Bounds (m): {west_m}, {south_m}, {east_m}, {north_m}")

    # Rasterize to binary mask
    mask = rasterize(
        [(geom, 1) for geom in polygons.geometry],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        dtype="uint8",
        all_touched=False,
    )

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "uint8",
        "crs": "EPSG:3857",
        "transform": transform,
    }

    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(mask, 1)

    print(f"✓ Saved rasterized mask to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rasterize forest polygons into a single GeoTIFF mask.")
    parser.add_argument("--polygons", required=True, help="Path to GeoJSON with forest polygons")
    parser.add_argument("--out", required=True, help="Output raster .tif path")
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        required=True,
        metavar=("WEST", "SOUTH", "EAST", "NORTH"),
        help="Bounding box in WGS84",
    )
    parser.add_argument("--resolution", type=float, default=1.0, help="Pixel size in meters (default: 1.0)")
    args = parser.parse_args()

    rasterize_to_mask(
        Path(args.polygons),
        Path(args.out),
        tuple(args.bbox),
        args.resolution,
    )
