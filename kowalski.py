#!/usr/bin/env python
import argparse
import sys
import subprocess
from pathlib import Path


def check_configs(cfgs=('secrets.defaults.json',
                        'kowalski/config_api.defaults.json',
                        'kowalski/config_ingester.defaults.json',
                        'kowalski/supervisord_api.defaults.conf',
                        'kowalski/supervisord_ingester.defaults.conf',
                        'docker-compose.defaults.yaml',
                        'docker-compose.traefik.defaults.yaml',
                        )
                  ):
    # use config defaults if configs do not exist
    for cfg in cfgs:
        c = cfg.replace('.defaults', '')
        if not Path(c).exists():
            cd = input(f'{c} does not exist, do you want to use {cfg} (not recommended)? [y/N] ')
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
        'secrets.defaults.json',
        'kowalski/config_api.defaults.json',
        'kowalski/config_ingester.defaults.json',
        'kowalski/supervisord_api.defaults.conf',
        'kowalski/supervisord_ingester.defaults.conf',
    ]

    if args.traefik:
        cfgs.append('docker-compose.traefik.defaults.yaml')
        cmd = ["docker-compose", "-f", 'docker-compose.traefik.yaml', "up", "-d"]
    elif args.fritz:
        cfgs.append('docker-compose.fritz.defaults.yaml')
        cmd = ["docker-compose", "-f", 'docker-compose.fritz.yaml', "up", "-d"]
    else:
        cfgs.append('docker-compose.defaults.yaml')
        cmd = ["docker-compose", "-f", 'docker-compose.yaml', "up", "-d"]

    if args.build:
        cmd += ["--build"]

    # check configuration
    print('Checking configuration')
    check_configs(cfgs=cfgs)

    # start up Kowalski
    print('Starting up')
    subprocess.run(cmd)


def down(_args):
    """
        Shut Kowalski down
    :param _args:
    :return:
    """
    print('Shutting down Kowalski')
    if args.traefik:
        cmd = ["docker-compose", "-f", 'docker-compose.traefik.yaml', "down"]
    elif args.fritz:
        cmd = ["docker-compose", "-f", 'docker-compose.fritz.yaml', "down"]
    else:
        cmd = ["docker-compose", "-f", 'docker-compose.yaml', "down"]

    subprocess.run(cmd)


def build(_args):
    """
        Build Kowalski's containers
    :param _args:
    :return:
    """
    print('Building Kowalski')
    if args.traefik:
        cmd = ["docker-compose", "-f", 'docker-compose.traefik.yaml', "build"]
    elif args.fritz:
        cmd = ["docker-compose", "-f", 'docker-compose.fritz.yaml', "build"]
    else:
        cmd = ["docker-compose", "-f", 'docker-compose.yaml', "build"]

    subprocess.run(cmd)


def test(_args):
    print('Running the test suite')

    print('Testing ZTF alert ingestion')
    cmd = ["docker", "exec", "-it", "kowalski_ingester_1", "python", "-m", "pytest", "-s", "test_ingester.py"]
    subprocess.run(cmd)

    print('Testing API')
    cmd = ["docker", "exec", "-it", "kowalski_api_1", "python", "-m", "pytest", "-s", "test_api.py"]
    subprocess.run(cmd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    commands = [
        ("up", "🚀 Launch Kowalski"),
        ("down", "✋ Shut Kowalski down"),
        ("build", "Build Kowalski's containers"),
        ("test", "Run the test suite"),
        ("help", "Print this message"),
    ]

    parsers = {}
    for (cmd, desc) in commands:
        parsers[cmd] = subparsers.add_parser(cmd, help=desc)

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
