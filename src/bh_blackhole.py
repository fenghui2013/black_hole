from bh_socket import *

from bh_parse_conf import *

BLACKHOLE_IP = get_config_blackhole_ip()
BLACKHOLE_PORT = get_config_blackhole_port()
BLACKHOLE_SOCKET = None

def blackhole_socket_create():
    global BLACKHOLE_IP, BLACKHOLE_PORT, BLACKHOLE_SOCKET
    if BLACKHOLE_SOCKET == None:
        BLACKHOLE_SOCKET = bh_socket_create()
        bh_socket_rcvbuf_set(BLACKHOLE_SOCKET)
        bh_socket_sndbuf_set(BLACKHOLE_SOCKET)
        bh_socket_bind(BLACKHOLE_SOCKET, BLACKHOLE_IP, BLACKHOLE_PORT)
        bh_socket_listen(BLACKHOLE_SOCKET, 1024)
        bh_socket_nonblocking(BLACKHOLE_SOCKET)
        bh_socket_tcpnodelay(BLACKHOLE_SOCKET)
    return BLACKHOLE_SOCKET

def blackhole_socket_close():
    global BLACKHOLE_SOCKET
    bh_socket_close(BLACKHOLE_SOCKET)
    BLACKHOLE_SOCKET = None
