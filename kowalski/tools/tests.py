import argparse

import subprocess


def test(use_docker=False):
    """
    Run the test suite

    :return:
    """
    print("Running the test suite")

    # make sure the containers are up and running

    tests = [
        {
            "part": "TURBO alert broker components",
            "container": "kowalski-ingester-1",
            "location": "brokers/components/test_alert_broker_turbo.py",
            "flaky": False,
        },
        {
            "part": "PGIR alert broker components",
            "container": "kowalski-ingester-1",
            "location": "brokers/components/test_alert_broker_pgir.py",
            "flaky": False,
        },
        {
            "part": "WINTER alert broker components",
            "container": "kowalski-ingester-1",
            "location": "brokers/components/test_alert_broker_wntr.py",
            "flaky": False,
        },
        {
            "part": "ZTF alert broker components",
            "container": "kowalski-ingester-1",
            "location": "brokers/components/test_alert_broker_ztf.py",
            "flaky": False,
        },
        {
            "part": "TURBO alert ingestion",
            "container": "kowalski-ingester-1",
            "location": "brokers/ingestion/test_ingester_turbo.py",
            "flaky": False,
        },
        {
            "part": "PGIR alert ingestion",
            "container": "kowalski-ingester-1",
            "location": "brokers/ingestion/test_ingester_pgir.py",
            "flaky": False,
        },
        {
            "part": "WINTER alert ingestion",
            "container": "kowalski-ingester-1",
            "location": "brokers/ingestion/test_ingester_wntr.py",
            "flaky": False,
        },
        {
            "part": "ZTF alert ingestion",
            "container": "kowalski-ingester-1",
            "location": "brokers/ingestion/test_ingester_ztf.py",
            "flaky": False,
        },
        {
            "part": "API",
            "container": "kowalski-api-1",
            "location": "api",
            "flaky": False,
        },
        {
            "part": "Tools",
            "container": "kowalski-ingester-1",
            "location": "misc/test_tools.py",
            "flaky": False,
        },
        {
            "part": "TNS monitoring",
            "container": "kowalski-ingester-1",
            "location": "misc/test_tns_watcher.py",
            "flaky": True,
        },
    ]

    failed_tests = []

    for t in tests:
        if t["part"] == "API" and use_docker:
            # this is to help github actions, we do not need
            # any of the ingester related processes to run anymore
            try:
                subprocess.run(
                    [
                        "docker",
                        "exec",
                        "-i",
                        "kowalski-ingester-1",
                        "bash",
                        "-c",
                        "export VIRTUAL_ENV=/usr/local && source env/bin/activate && make stop_ingester",
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError:
                print("Failed to stop the ingester processes")

        print(f"Testing {t['part']}")
        command = [
            "python",
            "-m",
            "pytest",
            "-s",
            f"kowalski/tests/{t['location']}",
        ]
        if use_docker:
            command = [
                "docker",
                "exec",
                "-i",
                t["container"],
                "bash",
                "-c",
                "export VIRTUAL_ENV=/usr/local && source env/bin/activate && "
                + " ".join(command),
            ]
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError:
            if not t.get("flaky", False):
                failed_tests.append(t["part"])
            else:
                print(f"{t['part']} test, marked as flaky, failed.")
            continue

    if failed_tests:
        print(f"Failed tests: {failed_tests}")
        # for the github action to fail, return non-zero exit code
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--use_docker",
        default=False,
        action="store_true",
        help="Run tests, with or without docker",
    )
    args = parser.parse_args()

    test(args.use_docker)
