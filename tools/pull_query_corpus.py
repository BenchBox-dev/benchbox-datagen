import argparse
import shutil
from pathlib import Path

CORPUS = Path("tests/fixtures/tpc_query_corpus")
TPCH = Path("benchbox/_binaries/tpc-h/templates")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("benchbox_checkout", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    for source in (CORPUS, TPCH):
        target = args.destination / source
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(args.benchbox_checkout / source, target)


if __name__ == "__main__":
    main()
