import argparse
from collections import Counter
import os
import sys

import jinja2

from kowalski.utils import load_config, log

config = load_config(config_files=["config.yaml"])


def fill_config_file_values(template_paths):
    log("Compiling configuration templates")

    for template_path in template_paths:
        tpath, tfile = os.path.split(template_path)
        jenv = jinja2.Environment(
            loader=jinja2.FileSystemLoader(tpath),
        )

        template = jenv.get_template(tfile)
        rendered = template.render(config["kowalski"])

        with open(os.path.splitext(template_path)[0], "w") as f:
            f.write(rendered)
            f.write("\n")


def copy_supervisor_configs(services):

    duplicates = [k for k, v in Counter(services.keys()).items() if v > 1]
    if duplicates:
        raise RuntimeError(f"Duplicate service definitions found for {duplicates}")

    log(f"Discovered {len(services)} services")

    services_to_run = set(services.keys())
    log(f"Enabling {len(services_to_run)} services")

    supervisor_configs = []
    for service in services_to_run:
        path = services[service]

        if os.path.exists(path):
            with open(path) as f:
                supervisor_configs.append(f.read())

    with open("conf/supervisord.conf", "w") as f:
        f.write("\n\n".join(supervisor_configs))


def api(args):
    fill_config_file_values(["conf/supervisord_api.conf.template"])


def ingester(args):
    fill_config_file_values(["conf/supervisord_ingester.conf.template"])


def all(args):
    fill_config_file_values(
        [
            "conf/supervisord_api.conf.template",
            "conf/supervisord_ingester.conf.template",
        ]
    )
    copy_supervisor_configs(
        {
            "api": "conf/supervisord_api.conf",
            "ingester": "conf/supervisord_ingester.conf",
        }
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    commands = [
        ("api", "generate supervisord_api.conf"),
        ("ingester", "generate supervisord_ingester.conf"),
        ("all", "generate supervisor.conf for both api and ingester"),
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
