import argparse
import sys


def meta(args):
    pass


def fetch(args):
    pass


def ingest(args):
    pass


def dump(args):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    commands = [
        ("meta", "Get and save matchfile ids and urls"),
        ("fetch", "Fetch matchfiles from IPAC and upload to GCS"),
        ("ingest", "Ingest matchfiles to Kowalski"),
        ("dump", "Make MongoDB dumps per readout channel and upload to GCS"),
        ("help", "Print this message"),
    ]

    parsers = {}
    for (cmd, desc) in commands:
        parsers[cmd] = subparsers.add_parser(cmd, help=desc)

    # parsers["meta"].add_argument(
    #     "--save", action="store_true", help="help"
    # )

    args = parser.parse_args()
    if args.command is None or args.command == "help":
        parser.print_help()
    else:
        getattr(sys.modules[__name__], args.command)(args)
