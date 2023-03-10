#!/usr/bin/env python
import subprocess
import sys
from packaging import version as Version


def output(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = p.communicate()
    success = p.returncode == 0
    return success, out


deps = {
    "python": (
        # Command to get version
        ["python", "--version"],
        # Extract *only* the version number
        lambda v: v.split()[1],
        # It must be >= 3.7
        "3.7",
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

for dep, (cmd, get_version, min_version) in deps.items():
    try:
        query = f"{dep} >= {min_version}"
        success, out = output(cmd)
        try:
            version = get_version(out.decode("utf-8").strip())
            print(f"[{version.rjust(8)}]".rjust(40 - len(query)), end="")
        except:  # noqa: E722
            raise ValueError("Could not parse version")

        if not (Version.parse(version) >= Version.parse(min_version)):
            raise RuntimeError(f"Required {min_version}, found {version}")
    except ValueError:
        print(
            f"\n[!] Sorry, but our script could not parse the output of "
            f'`{" ".join(cmd)}`; please file a bug, or see '
            f"`check_app_environment.py`\n"
        )
        raise
    except Exception as e:
        fail.append((dep, e))

if fail:
    print()
    print("[!] Some system dependencies seem to be unsatisfied")
    print()
    print("    The failed checks were:")
    print()
    for (pkg, exc) in fail:
        cmd, get_version, min_version = deps[pkg]
        print(f'    - {pkg}: `{" ".join(cmd)}`')
        print("     ", exc)
    print()
    print("    Please refer to the README.md " "for installation instructions.")
    print()
    sys.exit(-1)

print()

print("-" * 20)

print()
