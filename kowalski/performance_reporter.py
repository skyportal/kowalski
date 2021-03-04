import datetime
import pathlib
import re
from collections import defaultdict
import argparse
import time
import sys
import traceback

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from utils import load_config, log

""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]

p_base = config["path"]["logs"]

log_dir = pathlib.Path(p_base)
log_files = list(log_dir.glob("dask_cluster*"))
actions = defaultdict(list)

action_patterns = {
    "Mongification": "mongification",
    "ML": "MLing",
    "Ingestion": "Ingesting",
    "Cross-match": "Cross-match of",
    "CLU Cross-match": "CLU cross-match",
    "Ingest Aux": "Aux ingesting",
    "Update Aux": "Aux updating",
    "Filtering": "Filtering",
    "Is Candidate": r"Checking if \w+ is Candidate",
    "Is Source": r"Checking if \w+ is Source",
    "Post Candidate Metadata": "Posting metadata of ",
    "Save Source": r"Saving \w+ \w+ as a Source on SkyPortal",
    "Make Photometry": "Making alert photometry of ",
    "Post Photometry": "Posting photometry of ",
    "Post Annotation": "Posting annotation for ",
    "Get Annotation": "Getting annotations for ",
    "Put Annotation": "Putting annotations for ",
    "Make Thumbnail": r"Making \w+ thumbnail for",
    "Post Thumbnail": r"Posting \w+ thumbnail for",
    "ZTFAlert(alert)": "ZTFAlert(alert)",
    "Prep Features": "Prepping features",
    "braai": "braai",
    "acai_h": "acai_h",
    "acai_v": "acai_v",
    "acai_o": "acai_o",
    "acai_n": "acai_n",
    "acai_b": "acai_b",
    "Alert Decoding": "Decoding alert",
    "Alert Submission": "Submitting alert",
    "Get ZTF Instrument Id": "Getting ZTF instrument_id from SkyPortal",
    "Get Source Groups Info": "Getting source groups info on",
    "Get Group Info": "Getting info on group",
}


def generate_report(output_path, start_date, end_date):

    for log_file in log_files:
        with open(log_file) as f_l:
            lines = f_l.readlines()

        for line in lines:
            if len(line) > 5:
                try:
                    tmp = line.split()
                    t = datetime.datetime.strptime(tmp[0], "%Y%m%d_%H:%M:%S:")
                    if start_date <= t <= end_date:
                        # If there is exactly one 'took' in the line
                        # This gets rid of non-timer logs but also just drops instances
                        # where multiple timer logs were put on the same line by
                        # multiple threads (since those are more work to parse)
                        if len(re.findall("took", line)) == 1:
                            # Figure out which operation is being timed
                            for action, pattern in action_patterns.items():
                                if re.search(pattern, line) is not None:
                                    actions[action].append(float(tmp[-2]))
                                    # We should only have one action per line so break out
                                    # once we find a match
                                    break

                except Exception:
                    continue

    with PdfPages(output_path) as pdf:
        # Add title page showing run parameters (using empty figure)
        firstPage = plt.figure(figsize=(8.5, 11))
        firstPage.clf()
        start_date_str = start_date.strftime("%m / %d / %Y")
        end_date_str = end_date.strftime("%m / %d / %Y")
        params_text = (
            "Kowalski Performance Report\n"
            f"Start Date: {start_date_str}\n"
            f"End Date: {end_date_str}\n"
        )
        firstPage.text(
            0.5, 0.5, params_text, transform=firstPage.transFigure, size=24, ha="center"
        )
        pdf.savefig()
        plt.close()

        for i, (action, values) in enumerate(actions.items()):
            if i % 2 == 0:
                fig, axs = plt.subplots(2, 1)
                fig.set_size_inches(8.5, 11)
                fig.subplots_adjust(hspace=0.2)

            if len(values) > 0:
                actions[action] = np.array(values)
                text = (
                    f"median: {np.median(values):.5g}s, "
                    f"std: {np.std(values):.5g}\n"
                    f"min: {np.min(values):.5g}s, "
                    f"max: {np.max(values):.5g}s\n"
                    f"total: {np.sum(values):.5g} s / {np.sum(values)/60:.5g} min\n"
                    f"total number of calls: {len(values)}\n"
                )
                ax = axs[i % 2]
                ax.hist(
                    values, bins=100, range=(0, np.median(values) + 3 * np.std(values))
                )
                ax.grid(alpha=0.4)
                ax.set_title(action)
                ax.set_xlabel("time (s)")
                ax.set_ylabel("num calls")
                ax.set_box_aspect(0.3)
                ax.text(0.5, -0.7, text, ha="center", transform=ax.transAxes)

            if i % 2 == 1:
                pdf.savefig()
                plt.close()

            # Set report metadata
            d = pdf.infodict()
            d["Title"] = "Kowalski Daily Performance Summary"
            d["Author"] = "kowalski-bot"
            d["ModDate"] = datetime.datetime.today()


def send_report(report_path):
    client = WebClient(token=config["slack"]["slack_bot_token"])
    log(f"Sending: {report_path}")
    try:
        response = client.files_upload(
            channels=config["slack"]["slack_channel_id"],
            file=report_path,
            initial_comment="A new Kowalski performance report is ready!",
        )
        assert response["file"]  # the uploaded file
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        log(f"Got an error: {e.response['error']}")


def main(days_ago, send_to_slack=False):

    while True:
        try:
            # Delete any old reports to save space
            old_reports = list(log_dir.glob("kowalski_perf_report_*"))
            for report in old_reports:
                pathlib.Path.unlink(report)

            # Set up new run
            end_date = datetime.datetime.today()
            start_date = end_date - datetime.timedelta(days=days_ago)
            today = end_date.strftime("%Y%m%d")
            output_file = f"kowalski_perf_report_{today}.pdf"
            output_path = log_dir / output_file

            generate_report(output_path, start_date, end_date)

            if send_to_slack:
                send_report(output_path.as_posix())

        except KeyboardInterrupt:
            log("Aborted by user")
            sys.stderr.write("Aborted by user\n")
            sys.exit()

        except Exception as e:
            log(str(e))
            log(traceback.print_exc())

        # Sleep for a day
        time.sleep(86400)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--days_ago",
        type=int,
        default=1,
        help="Number of days to go back to from today for collecting logs. Default 1.",
    )
    parser.add_argument(
        "--send_to_slack", action="store_true", help="Send generated PDFs to Slack"
    )
    args = parser.parse_args()

    main(args.days_ago, send_to_slack=args.send_to_slack)
