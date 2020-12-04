#!/usr/bin/env python
import argparse
from deepdiff import DeepDiff
import pathlib
from pprint import pprint
import questionary
import subprocess
import sys
import time
import yaml


def check_configs(config_wildcards=("config.*yaml", "docker-compose.*yaml")):
    path = pathlib.Path(__file__).parent.absolute()

    for config_wildcard in config_wildcards:
        config = config_wildcard.replace("*", "")
        # use config defaults if configs do not exist?
        if not (path / config).exists():
            answer = questionary.select(
                f"{config} does not exist, do you want to use one of the following"
                " (not recommended without inspection)?",
                choices=[p.name for p in path.glob(config_wildcard)],
            ).ask()
            subprocess.run(["cp", f"{path / answer}", f"{path / config}"])

        # check contents of config.yaml WRT config.defaults.yaml
        if config == "config.yaml":
            with open(path / config.replace(".yaml", ".defaults.yaml")) as config_yaml:
                config_defaults = yaml.load(config_yaml, Loader=yaml.FullLoader)
            with open(path / config) as config_yaml:
                config_wildcard = yaml.load(config_yaml, Loader=yaml.FullLoader)
            deep_diff = DeepDiff(config_wildcard, config_defaults, ignore_order=True)
            difference = {
                k: v
                for k, v in deep_diff.items()
                if k in ("dictionary_item_added", "dictionary_item_removed")
            }
            if len(difference) > 0:
                print("config.yaml structure differs from config.defaults.yaml")
                pprint(difference)
                raise KeyError("Fix config.yaml before proceeding")


def up(arguments):
    """
    Launch Kowalski

    :param arguments:
    :return:
    """
    print("Spinning up Kowalski 🚀")

    config_wildcards = ["config.*yaml", "docker-compose.*yaml"]

    command = ["docker-compose", "-f", "docker-compose.yaml", "up", "-d"]

    if args.build:
        command += ["--build"]

    # check configuration
    print("Checking configuration")
    check_configs(config_wildcards=config_wildcards)

    # start up Kowalski
    print("Starting up")
    subprocess.run(command)


def down(arguments):
    """
        Shut Kowalski down
    :param arguments:
    :return:
    """
    print("Shutting down Kowalski")
    command = ["docker-compose", "-f", "docker-compose.yaml", "down"]

    subprocess.run(command)


def build(arguments):
    """
        Build Kowalski's containers
    :param arguments:
    :return:
    """
    print("Building Kowalski")

    config_wildcards = ["config.*yaml", "docker-compose.*yaml"]

    # always use docker-compose.yaml
    command = ["docker-compose", "-f", "docker-compose.yaml", "build"]

    # check configuration
    print("Checking configuration")
    check_configs(config_wildcards=config_wildcards)

    subprocess.run(command)


def seed(arguments):
    print("Ingesting catalog dumps into a running Kowalski instance")

    if (not arguments.local) and (not arguments.gcs):
        raise ValueError("Source not set, aborting")

    # check configuration
    print("Checking configuration")
    check_configs(config_wildcards=["config.*yaml"])

    with open(pathlib.Path(__file__).parent.absolute() / "config.yaml") as config_yaml:
        config = yaml.load(config_yaml, Loader=yaml.FullLoader)["kowalski"]

    if arguments.local:
        path = pathlib.Path(arguments.local).absolute()

        dumps = [p.name for p in path.glob("*.dump")]

        if len(dumps) == 0:
            print(f"No dumps found under {path}")
            return False

        answer = questionary.checkbox(
            "Found the following collection dumps. Which ones would you like to ingest?",
            choices=dumps,
        ).ask()

        command = [
            "docker",
            "exec",
            "-i",
            "kowalski_mongo_1",
            "mongorestore",
            f"-u={config['database']['admin_username']}",
            f"-p={config['database']['admin_password']}",
            "--authenticationDatabase=admin",
            "--archive",
        ]

        if arguments.drop:
            command.append("--drop")

        for dump in answer:
            with open(f"{path / dump}") as f:
                subprocess.call(command, stdin=f)

    if arguments.gcs:
        # print("Make sure gsutil is properly configured")
        raise NotImplementedError()


def test(arguments):
    print("Running the test suite")

    # make sure the containers are up and running
    num_retries = 10
    for i in range(num_retries):
        if i == num_retries - 1:
            raise RuntimeError("Kowalski's containers failed to spin up")

        command = ["docker", "ps", "-a"]
        container_list = (
            subprocess.check_output(command, universal_newlines=True)
            .strip()
            .split("\n")
        )
        if len(container_list) == 1:
            print("No containers are running, waiting...")
            time.sleep(2)
            continue

        ingester_is_up = (
            len(
                [
                    container
                    for container in container_list
                    if "kowalski_ingester_1" in container and " Up " in container
                ]
            )
            > 0
        )
        api_is_up = (
            len(
                [
                    container
                    for container in container_list
                    if "kowalski_api_1" in container and " Up " in container
                ]
            )
            > 0
        )
        if (not ingester_is_up) or (not api_is_up):
            print("Kowalski's containers are not up, waiting...")
            time.sleep(2)
            continue

        break

    print("Testing ZTF alert ingestion")

    command = [
        "docker",
        "exec",
        "-i",
        "kowalski_ingester_1",
        "python",
        "-m",
        "pytest",
        "-s",
        "test_ingester.py",
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        sys.exit(1)

    print("Testing API")
    command = [
        "docker",
        "exec",
        "-i",
        "kowalski_api_1",
        "python",
        "-m",
        "pytest",
        "-s",
        "test_api.py",
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        sys.exit(1)


def develop(arguments=None):
    """
    Install developer tools.
    """
    subprocess.run(["pip", "install", "-U", "pre-commit"], check=True)
    subprocess.run(["pre-commit", "install"], check=True)


def lint(arguments):
    try:
        import pre_commit  # noqa: F401
    except ImportError:
        develop()

    try:
        subprocess.run(["pre-commit", "run", "--all-files"], check=True)
    except subprocess.CalledProcessError:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--yes", action="store_true", help="Answer yes for all questions"
    )

    commands = [
        ("up", "🐧🚀 Launch Kowalski"),
        ("down", "✋ Shut down Kowalski"),
        ("build", "Build Kowalski's containers"),
        ("seed", "Ingest catalog dumps into Kowalski"),
        ("test", "Run the test suite"),
        ("develop", "Install tools for developing Fritz"),
        ("lint", "Lint the full code base"),
        ("help", "Print this message"),
    ]

    parsers = {}
    for (cmd, desc) in commands:
        parsers[cmd] = subparsers.add_parser(cmd, help=desc, parents=[parent_parser])

    parsers["up"].add_argument(
        "--build", action="store_true", help="Force (re)building Kowalski's containers"
    )

    parsers["seed"].add_argument(
        "--local", type=str, help="Local path to look for stored collection dumps"
    )
    parsers["seed"].add_argument(
        "--gcs",
        type=str,
        help="Google Cloud Storage bucket name to look for collection dumps",
    )
    parsers["seed"].add_argument(
        "--drop", action="store_true", help="Drop collections before ingestion"
    )

    args = parser.parse_args()
    if args.command is None or args.command == "help":
        parser.print_help()
    else:
        getattr(sys.modules[__name__], args.command)(args)
