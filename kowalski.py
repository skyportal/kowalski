#!/usr/bin/env python
import argparse
import sys
import subprocess
from pathlib import Path


def check_configs(
        cfgs=('config.defaults.yaml', 'docker-compose.defaults.yaml', 'docker-compose.traefik.defaults.yaml'),
        yes=False,
):
    # use config defaults if configs do not exist
    for cfg in cfgs:
        c = cfg.replace('.defaults', '')
        if not Path(c).exists():
            cd = input(
                f'{c} does not exist, do you want to use {cfg} (not recommended)? [y/N] '
            ) if not yes else 'y'
            if cd.lower() == 'y':
                subprocess.run(["cp", f"{cfg}", f"{c}"])
            else:
                raise IOError(f'{c} does not exist, aborting')


def up(_args):
    """
    Launch Kowalski

    :param _args:
    :return:
    """
    print('Spinning up Kowalski 🚀')

    cfgs = [
        'config.defaults.yaml',
    ]

    if args.traefik:
        cfgs.append('docker-compose.traefik.defaults.yaml')
        command = ["docker-compose", "-f", 'docker-compose.traefik.yaml', "up", "-d"]
    elif args.fritz:
        cfgs.append('docker-compose.fritz.defaults.yaml')
        command = ["docker-compose", "-f", 'docker-compose.fritz.yaml', "up", "-d"]
    else:
        cfgs.append('docker-compose.defaults.yaml')
        command = ["docker-compose", "-f", 'docker-compose.yaml', "up", "-d"]

    if args.build:
        command += ["--build"]

    # check configuration
    print('Checking configuration')
    check_configs(cfgs=cfgs, yes=_args.yes)

    # start up Kowalski
    print('Starting up')
    subprocess.run(command)


def down(_args):
    """
        Shut Kowalski down
    :param _args:
    :return:
    """
    print('Shutting down Kowalski')
    if args.traefik:
        command = ["docker-compose", "-f", 'docker-compose.traefik.yaml', "down"]
    elif args.fritz:
        command = ["docker-compose", "-f", 'docker-compose.fritz.yaml', "down"]
    else:
        command = ["docker-compose", "-f", 'docker-compose.yaml', "down"]

    subprocess.run(command)


def build(_args):
    """
        Build Kowalski's containers
    :param _args:
    :return:
    """
    print('Building Kowalski')

    cfgs = [
        'config.defaults.yaml',
    ]

    if args.traefik:
        cfgs.append('docker-compose.traefik.defaults.yaml')
        command = ["docker-compose", "-f", 'docker-compose.traefik.yaml', "build"]
    elif args.fritz:
        cfgs.append('docker-compose.fritz.defaults.yaml')
        command = ["docker-compose", "-f", 'docker-compose.fritz.yaml', "build"]
    else:
        cfgs.append('docker-compose.defaults.yaml')
        command = ["docker-compose", "-f", 'docker-compose.yaml', "build"]

    # check configuration
    print('Checking configuration')
    check_configs(cfgs=cfgs, yes=_args.yes)

    subprocess.run(command)


def test(_args):
    print('Running the test suite')

    print('Testing ZTF alert ingestion')
    command = ["docker", "exec", "-it", "kowalski_ingester_1", "python", "-m", "pytest", "-s", "test_ingester.py"]
    subprocess.run(command)

    print('Testing API')
    command = ["docker", "exec", "-it", "kowalski_api_1", "python", "-m", "pytest", "-s", "test_api.py"]
    subprocess.run(command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--yes", action="store_true", help="Answer yes for all questions"
    )

    commands = [
        ("up", "🐧🚀 Launch Kowalski"),
        ("down", "✋ Shut Kowalski down"),
        ("build", "Build Kowalski's containers"),
        ("test", "Run the test suite"),
        ("help", "Print this message"),
    ]

    parsers = {}
    for (cmd, desc) in commands:
        parsers[cmd] = subparsers.add_parser(cmd, help=desc, parents=[parent_parser])

    parsers["up"].add_argument(
        "--build", action="store_true", help="Force (re)building Kowalski's containers"
    )
    parsers["up"].add_argument(
        "--traefik", action="store_true", help="Deploy Kowalski behind traefik"
    )
    parsers["up"].add_argument(
        "--fritz", action="store_true", help="Deploy Kowalski alongside locally-running SkyPortal"
    )
    parsers["down"].add_argument(
        "--traefik", action="store_true", help="Shut down Kowalski running behind traefik"
    )
    parsers["down"].add_argument(
        "--fritz", action="store_true", help="Shut down Kowalski running alongside locally-running SkyPortal"
    )
    parsers["build"].add_argument(
        "--traefik", action="store_true", help="Build Kowalski to run behind traefik"
    )
    parsers["build"].add_argument(
        "--fritz", action="store_true", help="Build Kowalski to run alongside locally-running SkyPortal"
    )

    args = parser.parse_args()
    if args.command is None or args.command == "help":
        parser.print_help()
    else:
        getattr(sys.modules[__name__], args.command)(args)
