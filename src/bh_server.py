from bh_epoll import *
from bh_socket import *
from bh_blackhole import *
from bh_message import *
from bh_log import *
import bh_connection_pool as bcp
import bh_translate_table as btt
import bh_daemon as bd
import traceback

blackhole_logger = get_logger_black_hole("bh_server")
translate_logger = get_logger_translate("bh_translate")

def bh_blackhole_start():
    epoll = bh_epoll_create()

    blackhole_socket = _blackhole_start()
    blackhole_logger.info("======translate_table init======")
    translate_table_init()
    blackhole_logger.info("======translate_table init done!======")

    try:
        while bd.running:
            events_list = bh_epoll_poll(timeout=0.01)
            for fileno, events in events_list:
                if fileno == blackhole_socket.fileno():
                    #SOCKET_ACCEPT
                    blackhole_socket = bcp.bh_connections_get1(fileno)["connection"]
                    connection_list = bh_socket_accept(blackhole_socket)
                    for connection, address in connection_list:
                        host = "%s:%s" % address
                        blackhole_logger.info("client: %s connect successfully! count of current connection: %d" % (host, bcp.bh_connections_count()))
                        bh_socket_nonblocking(connection)
                        bh_socket_tcpnodelay(connection)
                        bh_epoll_add(connection)
                        bcp.bh_connections_put(connection.fileno(), {"type":"client", "host":host, "connection":connection, "recv_buffer":[], "send_buffer":[], "state":"open"})
                else:
                    if events & select.EPOLLIN:
                        #SOCKET_READ
                        data, eof = nonblocking_recv_data(bcp.bh_connections_get1(fileno)["connection"])
                        if eof:
                            temp = bcp.bh_connections_get1(fileno)
                            temp["state"] = "close"
                            bcp.bh_connections_put(fileno, temp)
                        temp = bcp.bh_connections_get1(fileno)
                        temp["recv_buffer"].append(data)
                        bcp.bh_connections_put(fileno, temp)
                    if events & select.EPOLLOUT:
                        #SOCKET_WRITE
                        temp = bcp.bh_connections_get1(fileno)
                        while 1:
                            count, send_length, is_broken = nonblocking_send_data(temp["connection"], temp["send_buffer"])
                            if is_broken:
                                temp["state"] = "close"
                                break
                            del temp["send_buffer"][:count]
                            temp["send_buffer"][0] = temp["send_buffer"][0][send_length:]
                            
                            if len(temp["send_buffer"]) < 1000:
                                break
                        if 1==len(temp["send_buffer"]) and ""==temp["send_buffer"][0]:
                            del temp["send_buffer"][0]
                            bh_epoll_write(temp["connection"], False)
                        bcp.bh_connections_put(fileno, temp)
            bh_connections_pool_update()
            translate_table_update()
            bh_request_id_table_update()
            bh_cursor_id_table_update()
    except Exception, e:
        error_msg = traceback.format_exc()
        blackhole_logger.error(error_msg)
    finally:
        blackhole_logger.info("stop black hole!")
        _blackhole_stop()
        bh_epoll_release()

def _blackhole_start():
    blackhole_socket = blackhole_socket_create()
    bh_epoll_add(blackhole_socket)
    bcp.bh_connections_put(blackhole_socket.fileno(), {"type":"blackhole", "host":"", "connection": blackhole_socket, "recv_buffer":[], "send_buffer":[], "state":"open"})
    return blackhole_socket

def _blackhole_stop():
    blackhole_socket = blackhole_socket_create()
    bcp.bh_connections_get2(blackhole_socket.fileno())
    bh_epoll_del(blackhole_socket)
    blackhole_socket_close()

def translate_table_init():
    epoll = bh_epoll_create()
    translate_table = get_config_translate_table()
    for i in range(len(translate_table)):
        temp = translate_table[i]
        host = "%s:%s" % (temp["ip"], temp["port"])
        socket_obj = bh_socket_create()
        bh_socket_rcvbuf_set(socket_obj)
        bh_socket_sndbuf_set(socket_obj)
        status = bh_socket_connect(socket_obj, temp["ip"], temp["port"])
        if "ok" == status:
            blackhole_logger.info("mongodb: %s:%s connect successfully! size of current connection: %d" % (temp["ip"], temp["port"], bcp.bh_connections_count()))
            bh_socket_nonblocking(socket_obj)
            bh_socket_tcpnodelay(socket_obj)
            bh_epoll_add(socket_obj)
            bcp.bh_connections_put(socket_obj.fileno(), {"type":"mongodb", "host":host, "connection":socket_obj, "recv_buffer":[], "send_buffer":[], "state":"open"})
            btt.translate_table_put(socket_obj.fileno(), temp)
        else:
            blackhole_logger.info("mongodb: %s:%s connect failed!" % (temp["ip"], temp["port"]))
            bh_socket_close(socket_obj)
            btt.down_node_list_append(temp)

def translate_table_update():
    epoll = bh_epoll_create()
    temp_list = []
    for i in range(len(btt.DOWN_NODE_LIST)):
        temp = btt.DOWN_NODE_LIST[i]
        host = "%s:%s" % (temp["ip"], temp["port"])
        socket_obj = bh_socket_create()
        bh_socket_rcvbuf_set(socket_obj)
        bh_socket_sndbuf_set(socket_obj)
        status = bh_socket_connect(socket_obj, temp["ip"], temp["port"])
        if "ok" == status:
            blackhole_logger.info("mongodb: %s:%s connect successfully! size of current connection: %d" % (temp["ip"], temp["port"], bcp.bh_connections_count()))
            bh_socket_nonblocking(socket_obj)
            bh_socket_tcpnodelay(socket_obj)
            bh_epoll_add(socket_obj)
            bcp.bh_connections_put(socket_obj.fileno(), {"type":"mongodb", "host":host, "connection":socket_obj, "recv_buffer":[], "send_buffer":[], "state":"open"})
            btt.translate_table_put(socket_obj.fileno(), temp)
            temp_list.append(i)
        else:
            bh_socket_close(socket_obj)
    for i in range(len(temp_list)):
        del btt.DOWN_NODE_LIST[temp_list[i]]

def bh_connections_pool_update():
    temp_list = []
    for fileno in bcp.CONNECTIONS_POOL.keys():
        #print bcp.CONNECTIONS_POOL[fileno]["type"], bcp.CONNECTIONS_POOL[fileno]["host"], len(bcp.CONNECTIONS_POOL[fileno]["recv_buffer"]), len(bcp.CONNECTIONS_POOL[fileno]["send_buffer"])
        _parse_data(fileno)
        temp = bcp.CONNECTIONS_POOL[fileno]
        if ("mongodb"==temp["type"]) and ("close"==temp["state"]):
            blackhole_logger.info("mongodb: %s down!" % temp["host"])
            btt.translate_table_del(fileno)
            temp_list.append(fileno)
        elif ("client"==temp["type"]) and ("close"==temp["state"]) and (0==len(temp["recv_buffer"])):
            blackhole_logger.info("client: %s disconnect!" % temp["host"])
            temp_list.append(fileno)
    for i in xrange(len(temp_list)):
        connection = bcp.bh_connections_get2(temp_list[i])["connection"]
        bh_epoll_del(connection)
        bh_socket_close(connection)
        blackhole_logger.info("size of current connection: %d", bcp.bh_connections_count()-1)

def _parse_data(fileno):
    temp = bcp.bh_connections_get1(fileno)
    if 0 == len(temp["recv_buffer"]):
        return

    temp_buffer = temp["recv_buffer"][0]
    temp_buffer_length = len(temp_buffer)
    
    while temp_buffer_length > 0:
        if temp_buffer_length >= 16:
            length, request_id, response_id, operation = parse_header(temp_buffer[0:16])
            if temp_buffer_length >= length:
                package = temp_buffer[:length]
                temp_buffer = temp_buffer[length:]
                temp_buffer_length -= length
                _send_to_application_layer(fileno, length, request_id, response_id, operation, package)
                continue
        break
    del temp["recv_buffer"][0]
    if temp_buffer_length != 0:
        if 0 == len(temp["recv_buffer"]):
            temp["recv_buffer"].append(temp_buffer)
        else:
            temp["recv_buffer"][0] = temp_buffer + temp["recv_buffer"][0]
    bcp.bh_connections_put(fileno, temp)

def _send_to_application_layer(fileno, length, request_id, response_id, operation, package):
    src_temp = bcp.bh_connections_get1(fileno)
    if 1 == operation:
        #OP_REPLY
        response_flags, cursor_id, starting_from, number_returned = parse_package_reply(fileno, package)
        reply_fileno, package = translate_package_bh_into_mongo(response_id, package)
        des_temp = bcp.bh_connections_get1(reply_fileno)
        if None == des_temp:
            blackhole_logger.warn("client abort! fileno: %s" % reply_fileno)
            return
        des_temp["send_buffer"].append(package)
        bcp.bh_connections_put(reply_fileno, des_temp)
        bh_epoll_write(des_temp["connection"], True)
        translate_logger.debug("reply: %s <<== %s", des_temp["host"], src_temp["host"])
    elif 1000 == operation:
        #OP_MSG
        pass
    elif 2001 == operation:
        #OP_UPDATE
        insert_fileno = btt.translate_table_get_insert()
        if None == insert_fileno:
            insert_fileno = btt.translate_table_get_bak()
        des_temp = bcp.bh_connections_get1(insert_fileno)
        package = translate_package_mongo_into_bh(insert_fileno, package)
        des_temp["send_buffer"].append(package)
        bcp.bh_connections_put(insert_fileno, des_temp)
        bh_epoll_write(des_temp["connection"], True)
        translate_logger.debug("update: %s ==>> %s", src_temp["host"], des_temp["host"])
    elif 2002 == operation:
        #OP_INSERT
        insert_fileno = btt.translate_table_get_insert()
        if None == insert_fileno:
            insert_fileno = btt.translate_table_get_bak()
        des_temp = bcp.bh_connections_get1(insert_fileno)
        package = translate_package_mongo_into_bh(insert_fileno, package)
        des_temp["send_buffer"].append(package)
        bcp.bh_connections_put(insert_fileno, des_temp)
        bh_epoll_write(des_temp["connection"], True)
        translate_logger.debug("insert: %s ==>> %s", src_temp["host"], des_temp["host"])
    elif 2003 == operation:
        #RESERVED
        pass
    elif 2004 == operation:
        #OP_QUERY
        flags, full_collection_name, number_to_skip, number_to_return, query = parse_package_query(package)
        db = full_collection_name.split(".")[0]
        coll = full_collection_name.split(".")[1]
        if ("$cmd"==coll) or ("crawler"==db):
            insert_fileno = btt.translate_table_get_insert()
            if None == insert_fileno:
                insert_fileno = btt.translate_table_get_bak()
            des_temp = bcp.bh_connections_get1(insert_fileno)
            package = translate_package_mongo_into_bh(fileno, package)
            des_temp["send_buffer"].append(package)
            bcp.bh_connections_put(insert_fileno, des_temp)
            bh_epoll_write(des_temp["connection"], True)
            translate_logger.debug("query: %s ==>> %s", src_temp["host"], des_temp["host"])
        else:
            if "_id" in query.keys():
                query_fileno = btt.translate_table_get_read(full_collection_name, "_id", str(query["_id"]))
                if None == query_fileno:
                    query_fileno = btt.translate_table_get_bak()
                des_temp = bcp.bh_connections_get1(query_fileno)
                package = translate_package_mongo_into_bh(fileno, package)
                des_temp["send_buffer"].append(package)
                bcp.bh_connections_put(query_fileno, des_temp)
                bh_epoll_write(des_temp["connection"], True)
                translate_logger.debug("query: %s ==>> %s", src_temp["host"], des_temp["host"])
            elif "filename" in query.keys():
                query_fileno = btt.translate_table_get_read(full_collection_name, "filename", query["filename"])
                if None == query_fileno:
                    query_fileno = btt.translate_table_get_bak()
                des_temp = bcp.bh_connections_get1(query_fileno)
                package = translate_package_mongo_into_bh(fileno, package)
                des_temp["send_buffer"].append(package)
                bcp.bh_connections_put(query_fileno, des_temp)
                bh_epoll_write(des_temp["connection"], True)
                translate_logger.debug("query: %s ==>> %s", src_temp["host"], des_temp["host"])
            elif "files_id" in query.keys():
                #python mongodb driver
                query_fileno = btt.translate_table_get_read(full_collection_name, "files_id", str(query["files_id"]))
                if None == query_fileno:
                    query_fileno = btt.translate_table_get_bak()
                des_temp = bcp.bh_connections_get1(query_fileno)
                package = translate_package_mongo_into_bh(fileno, package)
                des_temp["send_buffer"].append(package)
                bcp.bh_connections_put(query_fileno, des_temp)
                bh_epoll_write(des_temp["connection"], True)
                translate_logger.debug("query: %s ==>> %s", src_temp["host"], des_temp["host"])
            elif "$query" in query.keys():
                #php mongodb driver
                if "files_id" in query["$query"].keys():
                    query_fileno = btt.translate_table_get_read(full_collection_name, "files_id", str(query["$query"]["files_id"]))
                    if None == query_fileno:
                        query_fileno = btt.translate_table_get_bak()
                    des_temp = bcp.bh_connections_get1(query_fileno)
                    package = translate_package_mongo_into_bh(fileno, package)
                    des_temp["send_buffer"].append(package)
                    bcp.bh_connections_put(query_fileno, des_temp)
                    bh_epoll_write(des_temp["connection"], True)
                    translate_logger.debug("query: %s ==>> %s", src_temp["host"], des_temp["host"])
    elif 2005 == operation:
        #OP_GET_MORE
        full_collection_name, number_to_return, cursor_id = parse_package_get_more(package)
        getmore_fileno = get_getmore(cursor_id)
        if None == getmore_fileno:
            getmore_fileno = btt.translate_table_get_bak()
        des_temp = bcp.bh_connections_get1(getmore_fileno)
        package = translate_package_mongo_into_bh(fileno, package)
        des_temp["send_buffer"].append(package)
        bcp.bh_connections_put(getmore_fileno, des_temp)
        bh_epoll_write(des_temp["connection"], True)
        translate_logger.debug("query: %s ==>> %s", src_temp["host"], des_temp["host"])
    elif 2006 == operation:
        #OP_DELETE
        pass
    elif 2007 == operation:
        #OP_KILL_CURSORS
        pass
    else:
        #INVALID OPERATION
        pass
