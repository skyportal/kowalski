#!/usr/bin/env python
import bz2
import datetime
import pathlib
import re
import secrets
import string
import subprocess
import time
from typing import Optional, Sequence

import fire

from kowalski.config import load_config
from kowalski.log import log

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
        lambda v: re.search(r"\s*([\d.]+)", v).group(0).strip(),
        # It must be >= 1.22.0
        "1.22.0",
    ),
}


def get_git_hash_date():
    """Get git date and hash

    Borrowed from SkyPortal https://skyportal.io

    :return:
    """
    hash_date = dict()
    try:
        p = subprocess.Popen(
            ["git", "log", "-1", '--format="%h %aI"'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=pathlib.Path(__file__).parent.absolute(),
        )
    except FileNotFoundError:
        pass
    else:
        out, err = p.communicate()
        if p.returncode == 0:
            git_hash, git_date = (
                out.decode("utf-8")
                .strip()
                .replace('"', "")
                .split("T")[0]
                .replace("-", "")
                .split()
            )
            hash_date["hash"] = git_hash
            hash_date["date"] = git_date

    return hash_date


class DockerKowalski:
    def __init__(self, yes=False):
        """

        :param yes: answer yes to all possible requests?
        """
        self.yes = yes

    @staticmethod
    def check_containers_up(
        containers: Sequence,
        num_retries: int = 10,
        sleep_for_seconds: int = 10,
    ):
        """Check if containers in question are up and running

        :param containers: container name sequence, e.g. ("kowalski_api_1", "kowalski_mongo_1")
        :param num_retries:
        :param sleep_for_seconds: number of seconds to sleep for before retrying
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
            print(container_list)
            if len(container_list) == 1:
                print("No containers are running, waiting...")
                time.sleep(sleep_for_seconds)
                continue

            containers_up = (
                len(
                    [
                        container
                        for container in container_list
                        if (
                            (container_name in container)
                            and (" Up " in container)
                            and ("unhealthy" not in container)
                            and ("health: starting" not in container)
                        )
                    ]
                )
                > 0
                for container_name in containers
            )

            if not all(containers_up):
                print(f"{containers} containers are not up, waiting...")
                time.sleep(sleep_for_seconds)
                continue

            break

    @staticmethod
    def check_keyfile():
        """Check if MongoDB keyfile for replica set authorization exists; generate one if not"""
        mongodb_keyfile = pathlib.Path("mongo_key.yaml")
        if not mongodb_keyfile.exists():
            print("Generating MongoDB keyfile")
            # generate a random key that is required to be able to use authorization with replica set
            key = "".join(
                secrets.choice(string.ascii_lowercase + string.digits)
                for _ in range(32)
            )
            with open(mongodb_keyfile, "w") as f:
                f.write(key)
            command = ["chmod", "400", "mongo_key.yaml"]
            subprocess.run(command)

    def load_docker_config():
        """Log docker configuration"""
        config = "docker-compose.yaml"
        if not pathlib.Path(config).exists():
            log(
                "Warning: docker-compose.yaml is missing. You are running on the default docker-compose configuration. To configure your system, "
                "please copy `docker-compose.defaults.yaml` to `docker-compose.yaml` and modify it as you see fit."
            )
            config = "docker-compose.defaults.yaml"
        return config

    @classmethod
    def up(cls, build: bool = False):
        """
        🐧🚀 Launch Kowalski

        :param build: build the containers first?
        :return:
        """
        print("Spinning up Kowalski 🐧🚀")

        cls.check_keyfile()

        if build:
            cls.build()

        docker_config = cls.load_docker_config()

        command = ["docker-compose", "-f", docker_config, "up", "-d"]

        # start up Kowalski
        print("Starting up")
        p = subprocess.run(command)
        if p.returncode != 0:
            exit(1)

    @staticmethod
    def down():
        """
        ✋ Shut down Kowalski

        :return:
        """
        print("Shutting down Kowalski")
        docker_config = "docker-compose.yaml"
        command = ["docker-compose", "-f", docker_config, "down"]

        subprocess.run(command)

    @classmethod
    def build(cls):
        """
        Build Kowalski's containers

        :return:
        """
        print("Building Kowalski")

        docker_config = cls.load_docker_config()
        command = ["docker-compose", "-f", docker_config, "build"]

        # load config
        config = load_config(["config.yaml"])["kowalski"]

        # get git version:
        git_hash_date = get_git_hash_date()
        version = (
            f"v{config['server']['version']}"
            f"+git{git_hash_date.get('date', datetime.datetime.utcnow().strftime('%Y%m%d'))}"
            f".{git_hash_date.get('hash', 'unknown')}"
        )
        with open(pathlib.Path("version.txt"), "w") as version_file:
            version_file.write(f"{version}\n")

        # check MongoDB keyfile
        cls.check_keyfile()

        p = subprocess.run(command)
        if p.returncode != 0:
            exit(1)

    @staticmethod
    def seed(
        source: str = "./", drop: Optional[bool] = False, ignore_checks: bool = False
    ):
        """
        Ingest catalog dumps into Kowalski

        :param source: where to look for the dumps;
                       can be a local path or a Google Cloud Storage bucket address, e.g. gs://kowalski-catalogs
        :param drop: drop existing collections with same names before ingesting?
        :return:
        """
        print("Ingesting catalog dumps into a running Kowalski instance")

        # load config
        config = load_config(["config.yaml"])["kowalski"]

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

            for dump in dumps:
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

            for dump in dumps:
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

                pathlib.Path(path_dump).unlink()


if __name__ == "__main__":
    fire.Fire(DockerKowalski)
