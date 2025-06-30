# foresttrace/foresttrace/dataset/tile_mask_from_filenames.py

from pathlib import Path

import mercantile
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window
from tqdm import tqdm

TILE_SIZE = 256
ZOOM = 17
RESOLUTION = 1.1943285668550503


def tile_mask_from_naip_tiles(mask_path: Path, naip_tile_dir: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    with rasterio.open(mask_path) as src:
        transform = src.transform
        crs = src.crs
        width = src.width
        height = src.height

        tiles = sorted(list(naip_tile_dir.glob("*.png")))

        for tile_path in tqdm(tiles, desc="Tiling mask"):
            try:
                z, x, y = map(int, tile_path.stem.split("_"))
                assert z == ZOOM
                bounds = mercantile.bounds(x=x, y=y, z=z)
                x_min, y_max = bounds.west, bounds.north

                # Convert world coords to pixel offsets
                col = int((x_min - transform.c) / RESOLUTION)
                row = int((transform.f - y_max) / RESOLUTION)

                if col < 0 or row < 0 or col + TILE_SIZE > width or row + TILE_SIZE > height:
                    continue

                window = Window(col_off=col, row_off=row, width=TILE_SIZE, height=TILE_SIZE)
                data = src.read(1, window=window)

                out_transform = from_origin(x_min, y_max, RESOLUTION, RESOLUTION)
                out_path = out_dir / f"{z}_{x}_{y}_mask.tif"

                with rasterio.open(
                    out_path,
                    "w",
                    driver="GTiff",
                    height=TILE_SIZE,
                    width=TILE_SIZE,
                    count=1,
                    dtype="uint8",
                    crs=crs,
                    transform=out_transform,
                ) as dst:
                    dst.write(data, 1)

            except Exception as e:
                print(f"Failed on tile {tile_path.name}: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Tile forest mask to match existing NAIP tile grid.")
    parser.add_argument("--mask", type=str, required=True, help="Path to full mask GeoTIFF")
    parser.add_argument("--naip_tiles", type=str, required=True, help="Path to NAIP tile directory")
    parser.add_argument("--out", type=str, required=True, help="Directory to save mask tiles")

    args = parser.parse_args()

    tile_mask_from_naip_tiles(
        Path(args.mask),
        Path(args.naip_tiles),
        Path(args.out),
    )
