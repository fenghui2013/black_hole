CONNECTIONS_POOL = {}

def _bh_connections_has(fileno):
    return CONNECTIONS_POOL.has_key(fileno)

def bh_connections_put(fileno, connection):
    CONNECTIONS_POOL[fileno] = connection

def bh_connections_get1(fileno):
    if _bh_connections_has(fileno):
        return CONNECTIONS_POOL[fileno]
    else:
        return None

def bh_connections_get2(fileno):
    if _bh_connections_has(fileno):
        connection = CONNECTIONS_POOL[fileno]
        del CONNECTIONS_POOL[fileno]
        return connection
    else:
        return None

def bh_connections_count():
    global CONNECTIONS_POOL
    return len(CONNECTIONS_POOL)
