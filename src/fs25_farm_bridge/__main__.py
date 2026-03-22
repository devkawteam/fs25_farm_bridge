import logging
import sys

from .bridge import run


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    run()


if __name__ == "__main__":
    main()
