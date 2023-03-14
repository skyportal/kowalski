#!/usr/bin/env python
from pathlib import Path
import subprocess
import sys
import tarfile

import requests
from distutils.version import LooseVersion as Version
from kowalski.utils import load_config


config = load_config(config_file="config.yaml")["kowalski"]


def output(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = p.communicate()
    success = p.returncode == 0
    return success, out


dependencies = {
    "python": (
        # Command to get version
        ["python", "--version"],
        # Extract *only* the version number
        lambda v: v.split()[1],
        # It must be >= 3.7
        "3.8.0",
    ),
    # "docker": (
    #     # Command to get version
    #     ["docker", "--version"],
    #     # Extract *only* the version number
    #     lambda v: v.split()[2][:-1],
    #     # It must be >= 18.06
    #     "18.06",
    # ),
    # "docker-compose": (
    #     # Command to get version
    #     ["docker-compose", "--version"],
    #     # Extract *only* the version number
    #     lambda v: re.search(r"\s*([\d.]+)", v).group(0).strip(),
    #     # It must be >= 1.22.0
    #     "1.22.0",
    # ), TODO: check only when outside of docker
}

print("Checking system dependencies:")

fail = []

for dep, (cmd, get_version, min_version) in dependencies.items():
    try:
        query = f"{dep} >= {min_version}"
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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
    print("    Please refer to the README.md " "for installation instructions.")
    print()
    sys.exit(-1)

print()

print("-" * 20)

print()

# now, we check if kafka is present (i.e, that there is an existing directory in the config file)
# if not, we download it and install it
print("Checking Kafka installation:")
kafka_path = config["kafka"]["path"]

path_kafka = Path(kafka_path)
if not path_kafka.exists():
    print("Kafka not found, downloading and installing...")
    scala_version = config["kafka"]["scala_version"]
    kafka_version = config["kafka"]["kafka_version"]

    kafka_url = f"https://downloads.apache.org/kafka/{kafka_version}/kafka_{scala_version}-{kafka_version}.tgz"
    print(f"Downloading Kafka from {kafka_url}")

    r = requests.get(kafka_url)
    with open(f"kafka_{scala_version}-{kafka_version}.tgz", "wb") as f:
        f.write(r.content)

    print("Unpacking Kafka...")

    tar = tarfile.open(f"kafka_{scala_version}-{kafka_version}.tgz", "r:gz")
    tar.extractall(path=path_kafka.parent)
else:
    print("Kafka found!")

# we copy the server.properties file to the kafka config directory
# there is an existing one in the kafka directory, so we need to overwrite it
print("Copying server.properties to Kafka config directory...")
path_server_properties = Path("server.properties")
path_kafka_config = Path(kafka_path) / "config"
path_kafka_config_server_properties = path_kafka_config / "server.properties"

with open(path_server_properties, "r") as f:
    server_properties = f.read()

with open(path_kafka_config_server_properties, "w") as f:
    f.write(server_properties)

print("Done!")

print()

print("-" * 20)

print()
