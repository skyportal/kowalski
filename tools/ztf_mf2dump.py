import argparse
import pathlib
import subprocess
import time

logfile = ""


def log(msg):
    with open(logfile, "a+") as f:
        f.write(msg + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # parser.add_argument('--keepall', action='store_true', help='keep all fields from the matchfiles?')
    # parser.add_argument('--test', action='store_true', help='test')
    parser.add_argument(
        "--np", type=int, default=96, help="number of processes for parallel ingestion"
    )
    parser.add_argument("--bs", type=int, default=2048, help="batch size for ingestion")
    parser.add_argument(
        "--tag", type=str, default="20201201", help="mf release time tag"
    )
    parser.add_argument(
        "--path",
        type=str,
        default=str(pathlib.Path.home() / "tmp"),
        help="local tmp path",
    )

    args = parser.parse_args()

    path_tmp = pathlib.Path(args.path)
    if not path_tmp.exists():
        path_tmp.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    subprocess.run(
        [
            "docker",
            "exec",
            "-it",
            "kowalski_ingester_1",  # "/bin/bash", "-c",
            "mkdir",
            "-p",
            f"/_tmp/ztf_matchfiles_{args.tag}/",
        ]
    )
    print(f"Subprocess mkdir took {time.time() - t0}s")

    rc_start, rc_stop = 36, 36

    # cli argument - rc#: [0, 63] ? no, just iterate over range(0, 64) for the stuff below:
    for rc in range(rc_start, rc_stop + 1):
        log(f"Working on rc: {rc}")
        t0 = time.time()
        # fetch matchfiles from gs://ztf-matchfiles-t_tag/rc/ to /_tmp/ztf-matchfiles-t_tag/
        subprocess.run(
            [
                "docker",
                "exec",
                "-it",
                "kowalski_ingester_1",
                "/usr/local/bin/gsutil",
                "-m",
                "cp",
                f"gs://ztf-matchfiles-{args.tag}/{rc}/*.pytable",
                # f"gs://ztf-matchfiles-{args.tag}/{rc}/ztf_000245_zg_c01_q1_match.pytable",  # test
                f"/_tmp/ztf_matchfiles_{args.tag}/",
            ]
        )
        log(f"Subprocess cp matchfiles to local _tmp took {time.time() - t0}s")
        t0 = time.time()
        # run ingest_ztf_matchfiles.py
        subprocess.run(
            [
                "docker",
                "exec",
                "-it",
                "kowalski_ingester_1",
                "python",
                "/app/ingest_ztf_matchfiles.py",
                "--rm",
                "--tag",
                args.tag,
                "--np",
                str(args.np),
                "--bs",
                str(args.bs),
            ]
        )
        log(f"Subprocess ingest_ztf_matchfiles took {time.time() - t0}s")
        t0 = time.time()
        # dump to /_tmp/
        with open(path_tmp / f"ZTF_sources_{args.tag}.rc{rc:02d}.dump", "w") as f:
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "kowalski_mongo_1",
                    "mongodump",
                    "-u=mongoadmin",
                    "-p=mongoadminsecret",
                    "--authenticationDatabase=admin",
                    "--archive",
                    "--db=kowalski",
                    f"--collection=ZTF_sources_{args.tag}",
                ],
                stdout=f,
            )
        log(f"Subprocess mongodump sources took {time.time() - t0}s")
        t0 = time.time()
        # lbzip2 the dump
        subprocess.run(
            [
                "lbzip2",
                "-v",
                "-f",
                "-n",
                str(args.np),
                str(path_tmp / f"ZTF_sources_{args.tag}.rc{rc:02d}.dump"),
            ]
        )
        log(f"Subprocess lbzip2 sources took {time.time() - t0}s")
        t0 = time.time()
        # mv to GCS
        subprocess.run(
            [
                # "docker", "exec", "-it", "kowalski_ingester_1",
                # "/usr/local/bin/gsutil",
                "gsutil",
                "mv",
                # f"/_tmp/ZTF_sources_{args.tag}.rc{rc:02d}.dump.bz2",
                str(path_tmp / f"ZTF_sources_{args.tag}.rc{rc:02d}.dump.bz2"),
                f"gs://test-ztf-sources-{args.tag}/",
            ]
        )
        log(f"Subprocess mv sources to gs took {time.time() - t0}s")
        t0 = time.time()
        # drop the sources collection, keep the exposures collection
        subprocess.run(
            [
                "docker",
                "exec",
                "kowalski_mongo_1",
                "mongo",
                "-u",
                "mongoadmin",
                "-p",
                "mongoadminsecret",
                "--authenticationDatabase",
                "admin",
                "kowalski",
                "--eval",
                f"db.ZTF_sources_{args.tag}.drop()",
            ]
        )
        log(f"Subprocess drop sources collection took {time.time() - t0}s")
        t0 = time.time()

    # export exposures
    # dump to /_tmp/
    with open(
        path_tmp / f"ZTF_exposures_{args.tag}.rc{rc_start:02d}_{rc_stop:02d}.dump", "w"
    ) as f:
        subprocess.run(
            [
                "docker",
                "exec",
                "kowalski_mongo_1",
                "mongodump",
                "-u=mongoadmin",
                "-p=mongoadminsecret",
                "--authenticationDatabase=admin",
                "--archive",
                "--db=kowalski",
                f"--collection=ZTF_exposures_{args.tag}",
            ],
            stdout=f,
        )
    log(f"Subprocess dump exposures took {time.time() - t0}s")
    t0 = time.time()
    # lbzip2 the dump
    subprocess.run(
        [
            "lbzip2",
            "-v",
            "-f",
            "-n",
            str(args.np),
            str(
                path_tmp
                / f"ZTF_exposures_{args.tag}.rc{rc_start:02d}_{rc_stop:02d}.dump"
            ),
        ]
    )
    log(f"Subprocess lbzip2 exposures took {time.time() - t0}s")
    t0 = time.time()
    # mv to gs://ztf-sources-20200401
    subprocess.run(
        [
            # "docker", "exec", "-it", "kowalski_ingester_1",
            # "/usr/local/bin/gsutil",
            "gsutil",
            "mv",
            # f"/_tmp/ZTF_exposures_{args.tag}.rc{rc_start:02d}_{rc_stop:02d}.dump.bz2",
            str(
                path_tmp
                / f"ZTF_exposures_{args.tag}.rc{rc_start:02d}_{rc_stop:02d}.dump.bz2"
            ),
            f"gs://test-ztf-sources-{args.tag}/",
        ]
    )
    log(f"Subprocess mv exposures to gs took {time.time() - t0}s")
    t0 = time.time()
    # drop the exposures collection
    subprocess.run(
        [
            "docker",
            "exec",
            "kowalski_mongo_1",
            "mongo",
            "-u",
            "mongoadmin",
            "-p",
            "mongoadminsecret",
            "--authenticationDatabase",
            "admin",
            "kowalski",
            "--eval",
            f"db.ZTF_exposures_{args.tag}.drop()",
        ]
    )
    log(f"Subprocess drop exposures collection took {time.time() - t0}s")
