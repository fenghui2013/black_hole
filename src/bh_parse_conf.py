import json

BLACK_HOLE_CONF = json.load(open('./conf/bh.conf', "r"))

def get_config_pwd():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["pwd"]

def get_config_blackhole_ip():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["blackhole"]["ip"]

def get_config_blackhole_port():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["blackhole"]["port"]

def get_config_blackhole_daemon():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["blackhole"]["daemon"]

def get_config_log_blackhole():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["log"]["blackhole"]

def get_config_log_translate():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["log"]["translate"]

def get_config_daemon_stdin():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["daemon"]["stdin"]

def get_config_daemon_stdout():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["daemon"]["stdout"]

def get_config_daemon_stderr():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["daemon"]["stderr"]

def get_config_daemon_pidfile():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["daemon"]["pidfile"]

def get_config_translate_table():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["translate_table"]

def get_config_bak():
    global BLACK_HOLE_CONF
    return BLACK_HOLE_CONF["bak"]
