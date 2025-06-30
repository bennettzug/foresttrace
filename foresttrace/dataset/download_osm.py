# foresttrace/foresttrace/dataset/download_osm.py

import argparse
from pathlib import Path

import osmnx as ox


def main(bbox: tuple[float, float, float, float], tag_key: str, tag_value: str, output_path: Path) -> None:
    tags = {tag_key: tag_value}

    gdf = ox.features.features_from_bbox(bbox, tags=tags)
    gdf = gdf[gdf["geometry"].type.isin(["Polygon", "MultiPolygon"])]

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_path, driver="GeoJSON")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download OpenStreetMap data within a bounding box.")
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("left", "bottom", "right", "top"),
        help="Bounding box coordinates (left/west, bottom/south, right/east, top/north)",
        required=True,
    )
    parser.add_argument(
        "--tag",
        type=str,
        default="natural=wood",
        help="OSM tag to query (e.g. 'natural=wood')",
    )
    parser.add_argument(
        "--out",
        type=str,
        help="Output GeoJSON file path",
        required=True,
    )

    args = parser.parse_args()
    key, value = args.tag.split("=")
    main(tuple(args.bbox), key, value, args.out)
