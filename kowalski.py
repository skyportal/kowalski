#!/usr/bin/env python
import bz2
from contextlib import contextmanager
from deepdiff import DeepDiff
from distutils.version import LooseVersion as Version
import fire
import pathlib
from pprint import pprint
import questionary
import subprocess
import sys
import time
from typing import Optional, Sequence
import yaml


dependencies = {
    "python": (
        # Command to get version
        ["python", "--version"],
        # Extract *only* the version number
        lambda v: v.split()[1],
        # It must be >= 3.7
        "3.7",
    ),
    "docker": (
        # Command to get version
        ["docker", "--version"],
        # Extract *only* the version number
        lambda v: v.split()[2][:-1],
        # It must be >= 18.06
        "18.06",
    ),
    "docker-compose": (
        # Command to get version
        ["docker-compose", "--version"],
        # Extract *only* the version number
        lambda v: v.split()[2][:-1],
        # It must be >= 1.22.0
        "1.22.0",
    ),
}


@contextmanager
def status(message):
    """
    Borrowed from https://github.com/cesium-ml/baselayer/

    :param message: message to print
    :return:
    """
    print(f"[·] {message}", end="")
    sys.stdout.flush()
    try:
        yield
    except Exception:
        print(f"\r[✗] {message}")
        raise
    else:
        print(f"\r[✓] {message}")


def deps_ok() -> bool:
    """
    Check system dependencies

    Borrowed from https://github.com/cesium-ml/baselayer/
    :return:
    """
    print("Checking system dependencies:")

    fail = []

    for dep, (cmd, get_version, min_version) in dependencies.items():
        try:
            query = f"{dep} >= {min_version}"
            with status(query):
                p = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
                )
                out, err = p.communicate()
                try:
                    version = get_version(out.decode("utf-8").strip())
                    print(f"[{version.rjust(8)}]".rjust(40 - len(query)), end="")
                except Exception:
                    raise ValueError("Could not parse version")

                if not (Version(version) >= Version(min_version)):
                    raise RuntimeError(f"Required {min_version}, found {version}")
        except Exception as e:
            fail.append((dep, e))

    if fail:
        print()
        print("[!] Some system dependencies seem to be unsatisfied")
        print()
        print("    The failed checks were:")
        print()
        for (pkg, exc) in fail:
            cmd, get_version, min_version = dependencies[pkg]
            print(f'    - {pkg}: `{" ".join(cmd)}`')
            print("     ", exc)
        print()
        print(
            "    Please refer to https://github.com/dmitryduev/tails "
            "for installation instructions."
        )
        print()
        return False

    print("-" * 20)
    return True


def check_configs(
    config_wildcards: Sequence = ("config.*yaml", "docker-compose.*yaml")
):
    """
    - Check if config files exist
    - Offer to use the config files that match the wildcards
    - For config.yaml, check its contents against the defaults to make sure nothing is missing/wrong

    :param config_wildcards:
    :return:
    """
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


class Kowalski:
    def __init__(self, yes=False):
        """

        :param yes: answer yes to all possible requests?
        """
        self.yes = yes

    @staticmethod
    def check_containers_up(
        containers: Sequence,
        num_retries: int = 10,
        sleep_for: int = 2,
    ):
        """Check if containers in question are up and running

        :param containers: container name sequence, e.g. ("kowalski_api_1", "kowalski_mongo_1")
        :param num_retries:
        :param sleep_for: number of seconds to sleep for before retrying
        :return:
        """
        for i in range(num_retries):
            if i == num_retries - 1:
                raise RuntimeError(f"{containers} containers failed to spin up")

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

            containers_up = (
                len(
                    [
                        container
                        for container in container_list
                        if container_name in container and " Up " in container
                    ]
                )
                > 0
                for container_name in containers
            )

            if not all(containers_up):
                print(f"{containers} containers are not up, waiting...")
                time.sleep(sleep_for)
                continue

            break

    @classmethod
    def up(cls, build: bool = False, init: bool = False):
        """
        🐧🚀 Launch Kowalski

        :param build: build the containers first?
        :param init: attempt to initiate mongo replica set?
        :return:
        """
        print("Spinning up Kowalski 🐧🚀")

        config_wildcards = ["config.*yaml", "docker-compose.*yaml"]

        # check configuration
        with status("Checking configuration"):
            check_configs(config_wildcards=config_wildcards)

        with open(
            pathlib.Path(__file__).parent.absolute() / "config.yaml"
        ) as config_yaml:
            config = yaml.load(config_yaml, Loader=yaml.FullLoader)["kowalski"]

        if init:
            # spin up mongo container
            command = [
                "docker-compose",
                "-f",
                "docker-compose.yaml",
                "up",
                "-d",
                "mongo",
            ]
            subprocess.run(command, check=True)
            cls.check_containers_up(containers=("kowalski_mongo_1",))
            print("Attempting MongoDB replica set initiation")
            command = [
                "docker",
                "exec",
                "-i",
                "kowalski_mongo_1",
                "mongo",
                f"-u={config['database']['admin_username']}",
                f"-p={config['database']['admin_password']}",
                "--authenticationDatabase=admin",
                "--eval",
                "'rs.initiate()'",
            ]
            subprocess.run(command, check=True)

        command = ["docker-compose", "-f", "docker-compose.yaml", "up", "-d"]

        if build:
            command += ["--build"]

        # start up Kowalski
        print("Starting up")
        subprocess.run(command)

    @staticmethod
    def down():
        """
        ✋ Shut down Kowalski

        :return:
        """
        print("Shutting down Kowalski")
        command = ["docker-compose", "-f", "docker-compose.yaml", "down"]

        subprocess.run(command)

    @staticmethod
    def build():
        """
        Build Kowalski's containers

        :return:
        """
        print("Building Kowalski")

        config_wildcards = ["config.*yaml", "docker-compose.*yaml"]

        # always use docker-compose.yaml
        command = ["docker-compose", "-f", "docker-compose.yaml", "build"]

        # check configuration
        with status("Checking configuration"):
            check_configs(config_wildcards=config_wildcards)

        subprocess.run(command)

    @staticmethod
    def seed(source: str = "./", drop: Optional[bool] = False):
        """
        Ingest catalog dumps into Kowalski

        :param source: where to look for the dumps;
                       can be a local path or a Google Cloud Storage bucket address, e.g. gs://kowalski-catalogs
        :param drop: drop existing collections with same names before ingesting?
        :return:
        """
        print("Ingesting catalog dumps into a running Kowalski instance")

        # check configuration
        with status("Checking configuration"):
            check_configs(config_wildcards=["config.*yaml"])

        with open(
            pathlib.Path(__file__).parent.absolute() / "config.yaml"
        ) as config_yaml:
            config = yaml.load(config_yaml, Loader=yaml.FullLoader)["kowalski"]

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

        if drop:
            command.append("--drop")

        if "gs://" not in source:
            # ingesting from a local path
            path = pathlib.Path(source).absolute()

            dumps = [p.name for p in path.glob("*.dump")]

            if len(dumps) == 0:
                print(f"No catalog dumps found under {path}")
                return False

            answer = questionary.checkbox(
                "Found the following collection dumps. Which ones would you like to ingest?",
                choices=dumps,
            ).ask()

            for dump in answer:
                with open(f"{path / dump}") as f:
                    subprocess.call(command, stdin=f)

        else:
            # ingesting from Google Cloud
            path_tmp = pathlib.Path(__file__).parent / ".catalog_dumps"
            if not path_tmp.exists():
                path_tmp.mkdir(parents=True, exist_ok=True)

            ls_command = ["gsutil", "ls", source]
            catalog_list = (
                subprocess.check_output(ls_command, universal_newlines=True)
                .strip()
                .split("\n")
            )
            dumps = [dump for dump in catalog_list if "dump" in dump]

            answer = questionary.checkbox(
                "Found the following collection dumps. Which ones would you like to ingest?",
                choices=dumps,
            ).ask()

            for dump in answer:
                cp_command = [
                    "gsutil",
                    "-m",
                    "cp",
                    "-n",
                    dump,
                    str(path_tmp),
                ]
                p = subprocess.run(cp_command, check=True)
                if p.returncode != 0:
                    raise RuntimeError(f"Failed to fetch {dump}")

                path_dump = f"{path_tmp / pathlib.Path(dump).name}"
                if dump.endswith(".bz2"):
                    with bz2.BZ2File(path_dump) as f:
                        subprocess.call(command, stdin=f)
                elif dump.endswith(".gz"):
                    with open(path_dump) as f:
                        subprocess.call(command + ["--gzip"], stdin=f)
                else:
                    with open(path_dump) as f:
                        subprocess.call(command, stdin=f)

                rm_fetched = questionary.confirm(f"Remove {path_dump}?").ask()
                if rm_fetched:
                    pathlib.Path(path_dump).unlink()

    @classmethod
    def test(cls):
        """
        Run the test suite

        :return:
        """
        print("Running the test suite")

        # make sure the containers are up and running
        cls.check_containers_up(containers=("kowalski_ingester_1", "kowalski_api_1"))

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

        print("Testing TNS monitoring")
        command = [
            "docker",
            "exec",
            "-i",
            "kowalski_ingester_1",
            "python",
            "-m",
            "pytest",
            "-s",
            "test_tns_watcher.py",
        ]
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError:
            sys.exit(1)

    @staticmethod
    def develop():
        """
        Install developer tools
        """
        subprocess.run(["pip", "install", "-U", "pre-commit"], check=True)
        subprocess.run(["pre-commit", "install"], check=True)

    @classmethod
    def lint(cls):
        """
        Lint the full code base

        :return:
        """
        try:
            import pre_commit  # noqa: F401
        except ImportError:
            cls.develop()

        try:
            subprocess.run(["pre-commit", "run", "--all-files"], check=True)
        except subprocess.CalledProcessError:
            sys.exit(1)


if __name__ == "__main__":
    # check environment
    env_ok = deps_ok()
    if not env_ok:
        raise RuntimeError("Halting because of unsatisfied system dependencies")

    fire.Fire(Kowalski)
