import glob
import os
import threading
import time

BOLD = "\033[1m"
NORMAL = "\033[0;0m"

COLOR_TABLE = [
    "black",
    "red",
    "green",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "white",
    "default",
]

def colorize(s, fg=None, bg=None, bold=False, underline=False, reverse=False):
    """Wraps a string with ANSI color escape sequences corresponding to the
    style parameters given.

    All of the color and style parameters are optional.

    This function is from Robert Kern's grin:

      https://github.com/cpcloud/grin

    Copyright (c) 2007, Enthought, Inc. under a BSD license.

    Parameters
    ----------
    s : str
    fg : str
        Foreground color of the text.  One of (black, red, green, yellow, blue,
        magenta, cyan, white, default)
    bg : str
        Background color of the text.  Color choices are the same as for fg.
    bold : bool
        Whether or not to display the text in bold.
    underline : bool
        Whether or not to underline the text.
    reverse : bool
        Whether or not to show the text in reverse video.

    Returns
    -------
    A string with embedded color escape sequences.
    """

    style_fragments = []
    if fg in COLOR_TABLE:
        # Foreground colors go from 30-39
        style_fragments.append(COLOR_TABLE.index(fg) + 30)
    if bg in COLOR_TABLE:
        # Background colors go from 40-49
        style_fragments.append(COLOR_TABLE.index(bg) + 40)
    if bold:
        style_fragments.append(1)
    if underline:
        style_fragments.append(4)
    if reverse:
        style_fragments.append(7)
    style_start = "\x1b[" + ";".join(map(str, style_fragments)) + "m"
    style_end = "\x1b[0m"
    return style_start + s + style_end

def tail_f(filename, interval=1.0):
    f = None

    while not f:
        try:
            f = open(filename)
            break
        except OSError:
            time.sleep(1)

    # Find the size of the file and move to the end
    st_results = os.stat(filename)
    st_size = st_results[6]
    f.seek(st_size)

    while True:
        where = f.tell()
        line = f.readline()
        if not line:
            time.sleep(interval)
            f.seek(where)
        else:
            yield line.rstrip("\n")


def print_log(filename, color="default", stream=None):
    """
    Print log to stdout; stream is ignored.
    """

    def print_col(line):
        print(colorize(line, fg=color))

    print_col(f"-> {filename}")

    for line in tail_f(filename):
        print_col(line)


def log_watcher(printers=None):
    """Watch for new logs, and start following them.

    Parameters
    ----------
    printers : list of callables
        Functions of form `f(logfile, color=None)` used to print the
        tailed log file.  By default, logs are sent to stdout.  Note
        that the printer is also responsible for following (tailing)
        the log file

    See Also
    --------
    print_log : the default stdout printer

    """
    # Start with a short discovery interval, then back off
    # until that interval is 60s
    interval = 1

    if printers is None:
        printers = [print_log]

    colors = ["default", "green", "yellow", "blue", "magenta", "cyan", "red"]
    watched = set()

    color = 0
    while True:
        all_logs = set(glob.glob("logs/*.log"))
        new_logs = all_logs - watched

        for logfile in sorted(new_logs):
            color = (color + 1) % len(colors)
            for printer in printers:
                thread = threading.Thread(
                    target=printer, args=(logfile,), kwargs={"color": colors[color]}
                )
                thread.start()

        watched = all_logs

        time.sleep(interval)
        interval = max(interval * 2, 60)


if __name__ == "__main__":
    log_watcher()
