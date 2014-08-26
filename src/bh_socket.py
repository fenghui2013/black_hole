import socket, errno

SEND_BUFFER = 1024*1024
RECV_BUFFER = 1024*1024

def bh_socket_create():
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

def bh_socket_close(socket_obj):
    socket_obj.close()

def bh_socket_nonblocking(socket_obj):
    socket_obj.setblocking(0)

def bh_socket_tcpnodelay(socket_obj):
    socket_obj.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

def bh_socket_bind(socket_obj, ip, port):
    socket_obj.bind((ip, port))

def bh_socket_listen(socket_obj, n):
    socket_obj.listen(n)

def bh_socket_accept(socket_obj):
    connection_list = []
    while 1:
        try:
            conn_tuple = socket_obj.accept()
            connection_list.append(conn_tuple)
        except socket.error, err:
            break
    return connection_list

def bh_socket_connect(socket_obj, ip, port):
    try:
        socket_obj.connect((ip, port))
        return "ok"
    except socket.error:
        return "error"

def bh_socket_sndbuf_get(socket_obj):
    return socket_obj.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)

def bh_socket_sndbuf_set(socket_obj):
    global SEND_BUFFER
    socket_obj.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SEND_BUFFER)


def bh_socket_rcvbuf_get(socket_obj):
    return socket_obj.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)

def bh_socket_rcvbuf_set(socket_obj):
    global RECV_BUFFER
    socket_obj.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECV_BUFFER)

def nonblocking_recv_data(socket_obj):
    data = []
    eof = False
    while 1:
        try:
            temp_buffer = socket_obj.recv(4096)
            if temp_buffer == "":
                eof = True
                break
            data.append(temp_buffer)
        except socket.error, err:
            if err.errno == errno.EAGAIN:
                pass
            else:
                eof = True
            break
    return "".join(data), eof

def nonblocking_send_data(socket_obj, data):
    is_break = False
    is_broken = False
    count = 0
    nsend = 0
    for i in xrange(len(data)):
        send_buffer = data[i]
        send_buffer_length = len(send_buffer)
        nleft = send_buffer_length
        while nleft > 0:
            try:
                nwritten = socket_obj.send(send_buffer)
                send_buffer = send_buffer[nwritten:]
                nleft -= nwritten
            except socket.error, err:
                if err.errno == errno.EAGAIN:
                    is_break = True
                else:
                    is_broken = True
                break
        count = i
        nsend = send_buffer_length - nleft
        if is_break or is_broken:
            break
    return count, nsend, is_broken
