import argparse
import concurrent.futures
import logging
import os
import sys
from inspect import currentframe, getframeinfo
from pathlib import Path

filename = getframeinfo(currentframe()).filename
parent = str(Path(filename).resolve().parent)
sys.path.append(parent)

import aaf2

sys.path.append(str(Path.joinpath(Path(parent), "helpers")))
from helpers import exec_ffprobe

SUPPORTED_EXTENSIONS = {".mxf", ".mp4", ".mov"}


def discover_media_files(inputs):
    found = set()

    for item in inputs:
        path = Path(item)
        if not path.exists():
            logging.warning("Input path does not exist: %s", item)
            continue

        if path.is_file():
            if path.suffix.lower() in SUPPORTED_EXTENSIONS:
                found.add(path.resolve())
            continue

        for candidate in path.rglob("*"):
            if candidate.is_file() and candidate.suffix.lower() in SUPPORTED_EXTENSIONS:
                found.add(candidate.resolve())

    return sorted(found, key=lambda p: str(p).lower())


def ensure_output_path(output_dir, output_name):
    output_dir_path = Path(output_dir).resolve()
    output_dir_path.mkdir(parents=True, exist_ok=True)
    return output_dir_path / output_name


def probe_media_files_parallel(media_files, batch_size=20, max_workers=20):
    probe_results = {}

    total = len(media_files)
    for start in range(0, total, batch_size):
        batch = media_files[start:start + batch_size]
        workers = max(1, min(max_workers, len(batch)))
        logging.debug(
            "Running ffprobe batch %s-%s (size=%s, workers=%s)",
            start + 1,
            start + len(batch),
            len(batch),
            workers,
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_file = {
                executor.submit(exec_ffprobe.get_ffprobe_info, str(media_file)): media_file
                for media_file in batch
            }

            for future in concurrent.futures.as_completed(future_to_file):
                media_file = future_to_file[future]
                try:
                    probe_results[media_file] = {
                        "metadata": future.result(),
                        "error": None,
                    }
                except Exception as error:
                    probe_results[media_file] = {
                        "metadata": None,
                        "error": error,
                    }

    return probe_results


def create_allinone_ama(output_file, media_files, ffprobe_batch_size=20, ffprobe_workers=20):
    linked_count = 0
    probe_results = probe_media_files_parallel(
        media_files,
        batch_size=ffprobe_batch_size,
        max_workers=ffprobe_workers,
    )

    with aaf2.open(str(output_file), "w") as aaf_file:
        for media_file in media_files:
            result = probe_results.get(media_file)
            metadata = result["metadata"] if result else None
            probe_error = result["error"] if result else "Unknown ffprobe failure"
            if metadata is None:
                logging.warning("Skipping %s (ffprobe failed: %s)", media_file, probe_error)
                continue

            try:
                aaf_file.content.create_ama_link(str(media_file), metadata)
                linked_count += 1
                logging.info("AMA linked: %s", media_file)
            except Exception as error:
                logging.warning("Skipping %s (AMA link failed: %s)", media_file, error)

    if linked_count == 0:
        raise RuntimeError("No media files were linked to the output AAF.")

    size = os.path.getsize(output_file)
    logging.info("Created AAF: %s", output_file)
    logging.info("Linked files: %s", linked_count)
    logging.info("Output size: %s bytes", size)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Recursively find .mxf/.mp4/.mov files and create one all-in-one AMA-linked AAF."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more files/folders to scan recursively",
    )
    parser.add_argument(
        "--odir",
        required=True,
        help="Output directory for the AAF",
    )
    parser.add_argument(
        "--oname",
        default="allinone_ama.aaf",
        help="Output AAF filename (default: allinone_ama.aaf)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--ffprobe-batch-size",
        type=int,
        default=20,
        help="Number of files to process per ffprobe batch (default: 20)",
    )
    parser.add_argument(
        "--ffprobe-workers",
        type=int,
        default=20,
        help="Maximum parallel ffprobe workers per batch (default: 20)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    media_files = discover_media_files(args.inputs)
    if not media_files:
        logging.error("No supported media files found (.mxf, .mp4, .mov).")
        sys.exit(1)

    if args.ffprobe_batch_size < 1:
        logging.error("--ffprobe-batch-size must be >= 1")
        sys.exit(1)

    if args.ffprobe_workers < 1:
        logging.error("--ffprobe-workers must be >= 1")
        sys.exit(1)

    logging.info("Discovered %s media files", len(media_files))
    for media_file in media_files:
        logging.debug("Found: %s", media_file)

    output_file = ensure_output_path(args.odir, args.oname)

    try:
        create_allinone_ama(
            output_file,
            media_files,
            ffprobe_batch_size=args.ffprobe_batch_size,
            ffprobe_workers=args.ffprobe_workers,
        )
    except Exception as error:
        logging.error("Failed to create all-in-one AMA AAF: %s", error)
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
