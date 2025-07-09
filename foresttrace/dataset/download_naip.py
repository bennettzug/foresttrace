# foresttrace/foresttrace/dataset/download_naip.py

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import mercantile
from PIL import Image
import requests
from tqdm import tqdm

TMS_URL = "https://gis.apfo.usda.gov/arcgis/rest/services/NAIP/USDA_CONUS_PRIME/ImageServer/tile/{z}/{y}/{x}"


def download_tile(tile: mercantile.Tile, out_dir: Path) -> tuple[bool, mercantile.Tile]:
    url = TMS_URL.format(z=tile.z, x=tile.x, y=tile.y)
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            img.save(out_dir / f"{tile.z}_{tile.x}_{tile.y}.png")
            return True, tile
    except Exception:
        pass
    return False, tile


def download_naip(bbox: tuple[float, float, float, float], zoom: int, out_path: str, max_workers: int = 16) -> None:
    west, south, east, north = bbox
    tiles = list(mercantile.tiles(west, south, east, north, zoom))

    out_dir = Path(out_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    failed_tiles = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_tile, tile, out_dir) for tile in tiles]
        with tqdm(total=len(futures), desc="Downloading NAIP tiles") as pbar:
            for future in as_completed(futures):
                success, tile = future.result()
                if not success:
                    failed_tiles.append(tile)
                pbar.update(1)
    print(f"✓ {len(tiles) - len(failed_tiles)}/{len(tiles)} tiles downloaded successfully")

    if failed_tiles:
        fail_log = out_dir / "failed_tiles.txt"
        with open(fail_log, "w") as f:
            for tile in failed_tiles:
                f.write(f"{tile.z},{tile.x},{tile.y}\n")
        print(f"⚠ Failed tiles logged to {fail_log}")


def main():
    parser = argparse.ArgumentParser(description="Download NAIP imagery")
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("left,bottom,right,top"),
        help="Bounding box coordinates (left/west, bottom/south, right/east, top/north)",
        required=True,
    )
    parser.add_argument(
        "--zoom",
        type=int,
        default=17,
        help="Zoom level (default: 17)",
    )
    parser.add_argument(
        "--out",
        type=str,
        help="Output directory",
        required=True,
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Maximum number of concurrent workers (default: 16)",
    )
    args = parser.parse_args()

    download_naip(tuple(args.bbox), args.zoom, args.out, max_workers=args.max_workers)


if __name__ == "__main__":
    main()
