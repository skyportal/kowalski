import argparse
import sys

import yaml


def generate_conf(service):
    if service not in config["kowalski"]["supervisord"].keys():
        raise ValueError(
            f"{service} service def not found in config['kowalski']['supervisord']"
        )

    sc = ""
    for k in config["kowalski"]["supervisord"][service].keys():
        sc += f"[{k}]\n"
        for kk, vv in config["kowalski"]["supervisord"][service][k].items():
            sc += f"{kk} = {vv}\n"

    with open(f"supervisord_{service}.conf", "w") as fsc:
        fsc.write(sc)


def api(args):
    generate_conf("api")


def ingester(args):
    generate_conf("ingester")


with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    commands = [
        ("api", "generate supervisord_api.conf"),
        ("ingester", "generate supervisord_ingester.conf"),
        ("help", "Print this message"),
    ]

    parsers = {}
    for (cmd, desc) in commands:
        parsers[cmd] = subparsers.add_parser(cmd, help=desc)

    args = parser.parse_args()
    if args.command is None or args.command == "help":
        parser.print_help()
    else:
        getattr(sys.modules[__name__], args.command)(args)
