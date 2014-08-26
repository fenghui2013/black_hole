import threading, sys, time
from src.bh_server import *
from src.bh_daemon import *
from src.bh_parse_conf import *

from src.bh_log import *

logger = get_logger_black_hole("black_hole")

daemon = get_config_blackhole_daemon()

pwd = get_config_pwd()
if pwd.endswith('/'):
    sys.path.append(pwd+"black_hole/")
else:
    sys.path.append(pwd+"/black_hole/")

def run():
    bh_blackhole_start()

def main():
    if len(sys.argv) == 2:
        if "start" == sys.argv[1]:
            if "true" == daemon:
                start(run)
            else:
                run()
        elif "stop" == sys.argv[1]:
            stop()
        elif "restart" == sys.argv[1]:
            restart(run)
        else:
            print "Unknown Command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)


if __name__ == "__main__":
    main()
