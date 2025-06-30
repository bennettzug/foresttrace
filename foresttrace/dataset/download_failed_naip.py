# foresttrace/foresttrace/dataset/download_failed_naip.py

import argparse
from pathlib import Path

import download_naip
import mercantile


def main(failed_file: str, out_dir: Path):
    from tqdm import tqdm

    failed_tiles = []
    with open(failed_file, "r") as f:
        for line in tqdm(f, desc="Processing failed tiles"):
            zoom, x, y = line.strip().split(",")
            tile = mercantile.Tile(int(x), int(y), int(zoom))
            success, tile = download_naip.download_tile(tile, out_dir)
            if not success:
                failed_tiles.append(tile)
    if failed_tiles:
        print(f"Failed to download {len(failed_tiles)} tiles:")
        for tile in failed_tiles:
            print(f"  {tile}")
        with open(f"{out_dir}/refailed_tiles.txt", "w") as f:
            for tile in failed_tiles:
                f.write(f"{tile.z},{tile.x},{tile.y}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download failed NAIP tiles")
    parser.add_argument("--failed_file", type=str, help="Path to failed tiles file")
    parser.add_argument("--out_path", type=str, help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.out_path)
    main(args.failed_file, out_dir)
