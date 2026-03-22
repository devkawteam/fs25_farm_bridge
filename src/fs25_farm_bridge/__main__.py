import argparse
import logging
import sys

from .bridge import run


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FS25 Farm Bridge sync runner"
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--server",
        type=int,
        help="Only sync one configured server ID (for example: 1 or 2)",
    )
    selection.add_argument(
        "--all",
        action="store_true",
        help="Sync all configured servers",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    run(selected_server=args.server, run_all=args.all)


if __name__ == "__main__":
    main()
