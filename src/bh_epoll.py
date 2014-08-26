import select

EPOLL = None

def bh_epoll_create():
    global EPOLL
    if None == EPOLL:
        EPOLL = select.epoll()
    return EPOLL

def bh_epoll_release():
    global EPOLL
    if None != EPOLL:
        EPOLL.close()

def bh_epoll_poll(timeout=-1, maxevents=-1):
    global EPOLL
    if None != EPOLL:
        return EPOLL.poll(timeout=timeout, maxevents=maxevents)

def bh_epoll_add(socket_obj):
    global EPOLL
    if None != EPOLL:
        EPOLL.register(socket_obj.fileno(), select.EPOLLET | select.EPOLLIN)

def bh_epoll_del(socket_obj):
    global EPOLL
    if None != EPOLL:
        EPOLL.unregister(socket_obj.fileno())

def bh_epoll_write(socket_obj, enable):
    global EPOLL
    if None != EPOLL:
        EPOLL.modify(socket_obj.fileno(), select.EPOLLET | select.EPOLLIN | (enable and select.EPOLLOUT or 0))
