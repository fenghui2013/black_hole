import os, sys, time, atexit
from signal import signal
from signal import SIGTERM
from signal import SIGINT
from bh_parse_conf import *
from bh_log import *

logger = get_logger_black_hole("bh_ns_daemon")

running = True
stdin = get_config_daemon_stdin()
stdout = get_config_daemon_stdout()
stderr = get_config_daemon_stderr()
pidfile = get_config_daemon_pidfile()

def daemonize():
    global stdin, stdout, stderr, pidfile

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    os.chdir("/")
    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
        sys.exit(1)

    sys.stdout.flush()
    sys.stderr.flush()
    si = file(stdin, 'r')
    so = file(stdout, 'a+')
    se = file(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    atexit.register(delpid)
    pid = str(os.getpid())
    file(pidfile, 'w+').write("%s\n" % pid)

def delpid():
    os.remove(pidfile)

def handle_signal():
    signal(SIGINT, handler)
    signal(SIGTERM, handler)

def handler(signum, frame):
    global running
    logger.info("get a signal: %s" % str(signum))
    running = False

def start(run):
    global pidfile

    try:
        pf = file(pidfile, 'r')
        pid = int(pf.read().strip())
        pf.close()
    except IOError:
        pid = None

    if pid:
        message = "pidfile %s already exists. Daemon already running!\n"
        sys.stderr.write(message % pidfile)
        sys.exit(1)

    daemonize()
    handle_signal()
    run()

def stop():
    global pidfile
    try:
        pf = file(pidfile, 'r')
        pid = int(pf.read().strip())
        pf.close()
    except IOError:
        pid = None

    if not pid:
        message = "pidfile %s does not exist. Daemon not running!\n"
        sys.stderr.write(message % pidfile)
        return

    try:
        while True:
            os.kill(pid, SIGTERM)
            time.sleep(1)
    except OSError, err:
        err = str(err)
        if err.find("No such process") > 0:
            if os.path.exists(pidfile):
                os.remove(pidfile)
        else:
            print str(err)
            sys.exit(1)

def restart(run):
    stop()
    start(run)
