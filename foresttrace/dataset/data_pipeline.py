# foresttrace/foresttrace/dataset/data_pipeline.py

import argparse
import logging
from pathlib import Path
import sys
import time

from download_failed_naip import download_failed_naip
from download_naip import download_naip
from download_osm import download_osm
from polygons_to_mask_tiles import polygons_to_mask_tiles


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("forest_trace_pipeline.log"),
        ],
    )
    pass


def create_output_subdir(output_dir: Path, bbox: tuple[float, float, float, float]) -> Path:
    """Create output subdirectory based on bbox and timestamp"""
    timestamp = time.strftime("%Y%m%d%H%M%S")
    bbox_str = f"{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}"
    output_subdir = output_dir / f"bbox_{bbox_str}_{timestamp}"
    output_subdir.mkdir(parents=True, exist_ok=True)
    return output_subdir


def validate_bbox(bbox: tuple[float, float, float, float]) -> bool:
    """Validate bounding box"""
    west, south, east, north = bbox

    if not (-180 <= west <= 180 and -180 <= east <= 180):
        logging.error(f"Invalid longitude values: west={west}, east={east}")
        return False

    if not (-90 <= south <= 90 and -90 <= north <= 90):
        logging.error(f"Invalid latitude values: south={south}, north={north}")
        return False

    if west >= east:
        logging.error(f"West longitude ({west}) must be less than east longitude ({east})")
        return False

    if south >= north:
        logging.error(f"South latitude ({south}) must be less than north latitude ({north})")
        return False

    return True


def download_naip_imagery(
    bbox: tuple[float, float, float, float],
    zoom: int,
    output_dir: Path,
    max_workers: int = 16,
    max_retries: int = 2,
) -> bool:
    """Download NAIP imagery, with automatic retries"""
    logging.info(f"Starting NAIP imagery download for bbox={bbox}")
    naip_dir = output_dir / "naip_tiles"

    try:
        # initial download
        logging.info("Initial NAIP download started")
        download_naip(bbox, zoom, naip_dir, max_workers=max_workers)

        failed_file = naip_dir / "failed_tiles.txt"
        retry_count = 0

        while failed_file.exists() and retry_count < max_retries:
            retry_count += 1
            logging.info(f"Retrying NAIP download ({retry_count}/{max_retries})")

            backup_file = naip_dir / f"failed_tiles_backup_{retry_count}.txt"
            failed_file.rename(backup_file)

            download_failed_naip(str(backup_file), naip_dir)

            if not failed_file.exists():
                logging.info("NAIP download completed successfully")
                return True

            if failed_file.exists():
                logging.error(f"Some NAIP tiles still failed after {retry_count} retries. Check {failed_file}")
                return False

        logging.info("NAIP imagery download completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error downloading NAIP imagery: {e}")
        return False


def download_forest_polygons(
    bbox: tuple[float, float, float, float],
    output_dir: Path,
    osm_tag: str = "natural=wood",
) -> bool:
    """Download forest polygons from OpenStreetMap"""

    logging.info(f"Downloading forest polygons for bbox: {bbox}")
    polygons_file = output_dir / "forest_polygons.geojson"

    try:
        tag_key, tag_value = osm_tag.split("=", 1)
        download_osm(bbox, tag_key, tag_value, polygons_file)

        if not polygons_file.exists():
            logging.error("Forest polygons file was not created")
            return False

        logging.info(f"Forest polygons saved to: {polygons_file}")
        return True

    except Exception as e:
        logging.error(f"Error downloading forest polygons: {e}")
        return False


def create_mask_tiles(
    naip_dir: Path,
    polygons_file: Path,
    output_dir: Path,
) -> bool:
    """Create mask tiles from forest polygons"""
    logging.info("Creating mask tiles from forest polygons")
    masks_dir = output_dir / "mask_tiles"

    try:
        polygons_to_mask_tiles(naip_dir, polygons_file, masks_dir)
        mask_files = list(masks_dir.glob("*_mask.tif"))
        if not mask_files:
            logging.error("No mask tiles were created")
            return False
        logging.info(f"Created {len(mask_files)} mask tiles in {masks_dir}")
        return True

    except Exception as e:
        logging.error(f"Error creating mask tiles: {e}")
        return False


def verify_outputs(output_dir: Path) -> bool:
    """Verify and report on pipeline outputs"""
    naip_dir = output_dir / "naip_tiles"
    masks_dir = output_dir / "mask_tiles"
    polygons_file = output_dir / "forest_polygons.geojson"

    # count files
    naip_files = list(naip_dir.glob("*.png")) if naip_dir.exists() else []
    mask_files = list(masks_dir.glob("*_mask.tif")) if masks_dir.exists() else []

    logging.info("Pipeline Results:")
    logging.info(f"  • NAIP tiles: {len(naip_files)}")
    logging.info(f"  • Mask tiles: {len(mask_files)}")
    logging.info(f"  • Forest polygons: {'✓' if polygons_file.exists() else '✗'}")

    if len(naip_files) != len(mask_files):
        logging.warning(f"Mismatch: {len(naip_files)} NAIP tiles vs {len(mask_files)} mask tiles")

    # Check for any remaining failed tiles
    failed_files = list(naip_dir.glob("*failed*.txt")) if naip_dir.exists() else []
    if failed_files:
        logging.warning(f"Found {len(failed_files)} failed tile logs")


def data_pipeline(
    bbox: tuple[float, float, float, float],
    output_dir: Path,
    zoom: int = 17,
    max_workers: int = 16,
    max_retries: int = 2,
    osm_tag: str = "natural=wood",
    log_level: str = "INFO",
    skip_naip: bool = False,
    skip_polygons: bool = False,
    skip_masks: bool = False,
) -> None:
    """Run the Forest Trace Data Pipeline with the given arguments."""
    # Setup logging
    setup_logging(log_level)

    # Validate inputs
    if not validate_bbox(bbox):
        raise ValueError("Invalid bounding box coordinates")

    output_dir = create_output_subdir(output_dir, bbox)

    # Log pipeline start
    logging.info("=" * 60)
    logging.info("🌲 Forest Trace Data Pipeline Starting")
    logging.info("=" * 60)
    logging.info(f"Bounding box: {bbox}")
    logging.info(f"Zoom level: {zoom}")
    logging.info(f"Output directory: {output_dir.absolute()}")
    logging.info(f"OSM tag: {osm_tag}")

    start_time = time.time()
    success = True

    try:
        # Step 1: Download NAIP imagery
        if not skip_naip:
            logging.info("\n🛰️  Step 1: Downloading NAIP imagery")
            if not download_naip_imagery(bbox, zoom, output_dir, max_workers, max_retries):
                success = False
        else:
            logging.info("⏭️  Skipping NAIP imagery download")

        # Step 2: Download forest polygons
        if not skip_polygons and success:
            logging.info("\n🌳 Step 2: Downloading forest polygons")
            if not download_forest_polygons(bbox, output_dir, osm_tag):
                success = False
        else:
            logging.info("⏭️  Skipping forest polygons download")

        # Step 3: Create mask tiles
        if not skip_masks and not skip_polygons and success:
            logging.info("\n🎭 Step 3: Creating mask tiles")
            naip_dir = output_dir / "naip_tiles"
            polygons_file = output_dir / "forest_polygons.geojson"

            if not naip_dir.exists():
                logging.error("NAIP tiles directory not found. Run with NAIP download first.")
                success = False
            elif not polygons_file.exists():
                logging.error("Forest polygons file not found. Run with polygon download first.")
                success = False
            else:
                if not create_mask_tiles(naip_dir, polygons_file, output_dir):
                    success = False
        else:
            logging.info("⏭️  Skipping mask tile creation")

        # Final verification
        if success:
            verify_outputs(output_dir)

    except KeyboardInterrupt:
        logging.info("\n⏹️  Pipeline interrupted by user")
        success = False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        success = False

    # Summary
    end_time = time.time()
    duration = end_time - start_time

    logging.info("\n" + "=" * 60)
    if success:
        logging.info("✅ Forest Trace Pipeline COMPLETED successfully!")
    else:
        logging.error("❌ Forest Trace Pipeline FAILED!")

    logging.info(f"Total runtime: {duration:.1f} seconds")
    logging.info(f"Output directory: {output_dir.absolute()}")
    logging.info("=" * 60)

    if not success:
        raise RuntimeError("Pipeline execution failed")


def main():
    parser = argparse.ArgumentParser(
        description="Forest Trace Data Pipeline - Complete workflow for NAIP imagery and forest masks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  uv run data_pipeline.py --bbox -80.87501765 37.54651655 -80.44783835 37.79956715 --out ../../data/raw

  # With custom settings
  uv run data_pipeline.py --bbox -80.87501765 37.54651655 -80.44783835 37.79956715 --out ../../data/raw \\
    --zoom 18 --max-workers 32 --osm-tag "landuse=forest"

  # Skip certain steps
  uv run data_pipeline.py --bbox -80.87501765 37.54651655 -80.44783835 37.79956715 --out ../../data/raw \\
    --skip-naip --skip-polygons
        """,
    )

    # Required arguments
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("WEST", "SOUTH", "EAST", "NORTH"),
        help="Bounding box coordinates (west/left, south/bottom, east/right, north/top) in WGS84",
        required=True,
    )

    parser.add_argument("--out", type=str, help="Output directory for all pipeline outputs", required=True)

    # Optional arguments
    parser.add_argument("--zoom", type=int, default=17, help="Zoom level for NAIP tiles (default: 17)")

    parser.add_argument(
        "--max-workers", type=int, default=16, help="Maximum number of concurrent workers for downloads (default: 16)"
    )

    parser.add_argument(
        "--max-retries", type=int, default=2, help="Maximum number of retry attempts for failed tiles (default: 2)"
    )

    parser.add_argument(
        "--osm-tag",
        type=str,
        default="natural=wood",
        help="OpenStreetMap tag to query for forest polygons. Common tags include landuse=forest, natural=wood. (default: natural=wood)",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    # Skip options
    parser.add_argument("--skip-naip", action="store_true", help="Skip NAIP imagery download")

    parser.add_argument(
        "--skip-polygons", action="store_true", help="Skip forest polygons download. Also skips mask tile creation."
    )

    parser.add_argument("--skip-masks", action="store_true", help="Skip mask tile creation")

    args = parser.parse_args()

    # Call the data_pipeline function
    data_pipeline(
        bbox=tuple(args.bbox),
        output_dir=Path(args.out),
        zoom=args.zoom,
        max_workers=args.max_workers,
        max_retries=args.max_retries,
        osm_tag=args.osm_tag,
        log_level=args.log_level,
        skip_naip=args.skip_naip,
        skip_polygons=args.skip_polygons,
        skip_masks=args.skip_masks,
    )


if __name__ == "__main__":
    main()
