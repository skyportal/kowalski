#!/usr/bin/env python
import subprocess
import sys
from distutils.version import LooseVersion as Version


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
