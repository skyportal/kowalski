import argparse
import subprocess
import sys
import yaml


def load_config(config_file='../config.yaml'):
    """
        Load config and secrets
    """
    with open(config_file) as cyaml:
        conf = yaml.load(cyaml, Loader=yaml.FullLoader)

    return conf


def meta(arguments):
    pass
    # todo: dissect fetch_ztf_matchfiles.py


def fetch(arguments):
    pass
    # todo: dissect fetch_ztf_matchfiles.py


def ingest(arguments):
    pass


def dump(arguments):
    if arguments.gcs:
        # fixme: /_tmp must be properly mapped!

        # ingest pre-made dumps from the GCS
        rcs = [0, ]
        # rcs = list(range(0, 64))

        u = config['kowalski']['database']['admin_username']
        p = config['kowalski']['database']['admin_password']

        # restore the sources collection
        for rc in rcs:
            # copy from GCS
            subprocess.run([
                "docker", "exec", "-it", "kowalski_ingester_1",
                "/usr/local/bin/gsutil",
                "-m", "cp",
                f"gs://ztf-sources-{args.tag}/",
                f"/_tmp/ZTF_sources_{args.tag}.rc{rc:02d}.dump.bz2",
            ])

            # restore
            subprocess.run([
                "docker", "exec", "kowalski_mongo_1",
                f"mongorestore", f"-u={u}", f"-p={p}", "--authenticationDatabase=admin",
                "--db=kowalski",
                f"--collection=ZTF_sources_{args.tag}",
                f"--archive=/_tmp/ZTF_sources_{args.tag}.rc{rc:02d}.dump.bz2",
            ])

            # remove dump
            subprocess.run([
                "docker", "exec", "-it", "kowalski_ingester_1",
                "rm", "-f",
                f"/_tmp/ZTF_sources_{args.tag}.rc{rc:02d}.dump.bz2",
            ])

    else:
        # todo: ingest a local copy of the matchfiles
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="command")

    parser.add_argument('--tag', type=str, default='20200401', help='matchfile release time tag')
    parser.add_argument('--config', type=str, default='../config.yaml', help="config and secrets")

    commands = [
        ("meta", "Get and save matchfile ids and urls"),
        ("fetch", "Fetch matchfiles from IPAC and upload to GCS"),
        ("dump", "Make MongoDB dumps per readout channel and upload to GCS"),
        ("ingest", "Ingest matchfiles to Kowalski"),
        ("help", "Print this message"),
    ]

    parsers = {}
    for (cmd, desc) in commands:
        parsers[cmd] = subparsers.add_parser(cmd, help=desc)

    parsers["ingest"].add_argument(
        "--local", action="store_true", help="ingest a local copy of matchfiles"
    )
    parsers["ingest"].add_argument(
        "--gcs", action="store_true", help="ingest pre-made dumps from GCS"
    )

    args = parser.parse_args()
    if args.command is None or args.command == "help":
        parser.print_help()
    else:
        getattr(sys.modules[__name__], args.command)(args)

    config = load_config(args.config)
