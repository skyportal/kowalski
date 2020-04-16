import argparse
import subprocess


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='')

    # parser.add_argument('--keepall', action='store_true', help='keep all fields from the matchfiles?')
    # parser.add_argument('--test', action='store_true', help='test')
    parser.add_argument('--np', type=int, default=96, help='number of processes for parallel ingestion')
    parser.add_argument('--bs', type=int, default=2048, help='batch size for ingestion')
    parser.add_argument('--tag', type=str, default='20200401', help='batch size for ingestion')

    args = parser.parse_args()

    # cli argument - rc#: [0, 63] ? no, just iterate over range(0, 64) for the stuff below:
    for rc in range(0, 1):
        # fetch matchfiles from gs://ztf-matchfiles-t_tag/rc/ to /_tmp/ztf-matchfiles-t_tag/
        subprocess.run([
            "docker", "exec", "-it", "kowalski_ingester_1", #"/bin/bash", "-c",
            "/usr/local/bin/gsutil",
            # "-m", "cp",
            "cp",
            f"gs://ztf-matchfiles-{args.tag}/{rc}/ztf_000245_zg_c01_q1_match.pytable",  # test
            # f"gs://ztf-matchfiles-{t_tag}/{rc}/*.pytable",
            f"/_tmp/ztf_matchfiles_{args.tag}/",
        ])
        # run ingest_ztf_matchfiles.py
        subprocess.run([
            "docker", "exec", "-it", "kowalski_ingester_1",
            "python",
            "ingest_ztf_matchfiles.py",
            "--rm",
            "--tag", args.tag,
            "--np", str(args.np),
            "--bs", str(args.bs),
        ])
        # dump to /_tmp/
        subprocess.run([
            "docker", "exec", "kowalski_mongo_1", "sh", "-c",
            f"mongodump", "-u=mongoadmin", "-p=mongoadminsecret", "--authenticationDatabase=admin",
            "--archive", "--db=kowalski", "--collection=ZTF_sources_{args.tag}",
            ">", f"/home/dmitryduev/tmp/ZTF_sources_{args.tag}.rc{rc:02d}.dump",
        ])
        # lbzip2 the dump
        subprocess.run([
            "lbzip2", "-v",
            "-n", str(args.np),
            f"/home/dmitryduev/tmp/ZTF_sources_{args.tag}.rc{rc:02d}.dump"
        ])
        # mv to gs://ztf-sources-20200401
        # drop the sources collection, keep the exposures collection
        subprocess.run([
            "docker", "exec", "kowalski_mongo_1", "sh", "-c",
            "mongo", "-u=mongoadmin", "-p=mongoadminsecret", "--authenticationDatabase=admin",
            "kowalski", "--eval", f"'db.ZTF_sources_{args.tag}.drop()'"
        ])
        # export exposures
        pass
