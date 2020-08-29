#!/usr/bin/env python
import argparse
import pathlib
import questionary
import sys
import subprocess
import yaml


def check_configs(cfgs=('config.*yaml', 'docker-compose.*yaml')):
    path = pathlib.Path(__file__).parent.absolute()

    # use config defaults if configs do not exist?
    for cfg in cfgs:
        c = cfg.replace('*', '')
        if not (path / c).exists():
            answer = questionary.select(
                f"{c} does not exist, do you want to use one of the following"
                " (not recommended without inspection)?",
                choices=[p.name for p in path.glob(cfg)]
            ).ask()
            subprocess.run(["cp", f"{path / answer}", f"{path / c}"])


def up(arguments):
    """
    Launch Kowalski

    :param arguments:
    :return:
    """
    print('Spinning up Kowalski 🚀')

    cfgs = [
        'config.*yaml',
        'docker-compose.*yaml'
    ]

    command = ["docker-compose", "-f", 'docker-compose.yaml', "up", "-d"]

    if args.build:
        command += ["--build"]

    # check configuration
    print('Checking configuration')
    check_configs(cfgs=cfgs)

    # start up Kowalski
    print('Starting up')
    subprocess.run(command)


def down(arguments):
    """
        Shut Kowalski down
    :param arguments:
    :return:
    """
    print('Shutting down Kowalski')
    command = ["docker-compose", "-f", 'docker-compose.yaml', "down"]

    subprocess.run(command)


def build(arguments):
    """
        Build Kowalski's containers
    :param arguments:
    :return:
    """
    print('Building Kowalski')

    cfgs = [
        'config.*yaml',
        'docker-compose.*yaml'
    ]

    # always use docker-compose.yaml
    command = ["docker-compose", "-f", 'docker-compose.yaml', "build"]

    # check configuration
    print('Checking configuration')
    check_configs(cfgs=cfgs)

    subprocess.run(command)
    
    
def seed(arguments):
    print("Ingesting catalog data into a running Kowalski instance")

    if (not arguments.local) and (not arguments.gcs):
        raise ValueError("Source not set, aborting")

    # check configuration
    print('Checking configuration')
    check_configs(cfgs=["config.*yaml"])

    with open(pathlib.Path(__file__).parent.absolute() / "config.yaml") as cyaml:
        config = yaml.load(cyaml, Loader=yaml.FullLoader)["kowalski"]

    if arguments.local:
        path = pathlib.Path(arguments.local).absolute()

        dumps = [p.name for p in path.glob("*.dump")]

        if len(dumps) == 0:
            print(f"No dumps found under {path}")
            return False

        answer = questionary.checkbox(
            f"Found the following collection dumps. Which ones would you like to ingest?",
            choices=dumps
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
            "--archive"
        ]

        if arguments.drop:
            command.append("--drop")

        for dump in answer:
            with open(f"{path / dump}") as f:
                subprocess.call(
                    command,
                    stdin=f
                )

    if arguments.gcs:
        # print("Make sure gsutil is properly configured")
        raise NotImplementedError()


def test(arguments):
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
        ("seed", "Ingest catalog data into Kowalski"),
        ("test", "Run the test suite"),
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
        "--gcs", type=str, help="Google Cloud Storage bucket name to look for collection dumps"
    )
    parsers["seed"].add_argument(
        "--drop", action="store_true", help="Drop collections before ingestion"
    )

    args = parser.parse_args()
    if args.command is None or args.command == "help":
        parser.print_help()
    else:
        getattr(sys.modules[__name__], args.command)(args)
