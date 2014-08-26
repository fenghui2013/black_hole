import struct, time
import bson
from bh_log import *

translate_logger = get_logger_translate("bh_translate")

REQUEST_ID_TABLE = {}
REQUEST_ID_UPDATE_LAST_TIME = 0
REQUEST_ID_UPDATE_INTERVAL = 60 #unit: s
CURSOR_ID_TABLE = {}
CURSOR_ID_UPDATE_LAST_TIME = 0
CURSOR_ID_UPDATE_INTERVAL = 600 #unit: s

MAX_INT32 = 2147483647
MIN_INT32 = -2147483648
REQUEST_ID = MIN_INT32

def _bh_inc_request_id():
    global REQUEST_ID
    if REQUEST_ID == MAX_INT32:
        REQUEST_ID = MIN_INT32
    REQUEST_ID += 1

def bh_request_id_table_update():
    global REQUEST_ID_TABLE, REQUEST_ID_UPDATE_LAST_TIME
    current_time = int(time.time())
    if (current_time-REQUEST_ID_UPDATE_LAST_TIME) > REQUEST_ID_UPDATE_INTERVAL:
        temp = []
        for key in REQUEST_ID_TABLE.keys():
            if (current_time-REQUEST_ID_TABLE[key]["time"]) > REQUEST_ID_UPDATE_INTERVAL:
                temp.append(key)
        for i in range(len(temp)):
            del REQUEST_ID_TABLE[temp[i]]
        REQUEST_ID_UPDATE_LAST_TIME = current_time
        translate_logger.info("request_id_table current size: %d" % len(REQUEST_ID_TABLE))

def bh_cursor_id_table_update():
    global CURSOR_ID_TABLE, CURSOR_ID_UPDATE_LAST_TIME
    current_time = int(time.time())
    if (current_time-CURSOR_ID_UPDATE_LAST_TIME) > CURSOR_ID_UPDATE_INTERVAL:
        temp = []
        for key in CURSOR_ID_TABLE.keys():
            if (current_time-CURSOR_ID_TABLE[key]["time"]) > CURSOR_ID_UPDATE_INTERVAL:
                temp.append(key)
        for i in range(len(temp)):
            del CURSOR_ID_TABLE[temp[i]]
        CURSOR_ID_UPDATE_LAST_TIME = current_time
        translate_logger.info("cursor_id_table current size: %d" % len(CURSOR_ID_TABLE))

def get_getmore(cursor_id):
    global CURSOR_ID_TABLE
    return CURSOR_ID_TABLE[cursor_id]["fileno"]

def translate_package_mongo_into_bh(fileno, package):
    global REQUEST_ID
    REQUEST_ID_TABLE.update({REQUEST_ID:{"fileno":fileno, "request_id":package[4:8], "time":int(time.time())}})
    package = package[0:4] + struct.pack("<i", REQUEST_ID) + package[8:]
    _bh_inc_request_id()
    return package

def translate_package_bh_into_mongo(response_id, package):
    temp = REQUEST_ID_TABLE[response_id]
    del REQUEST_ID_TABLE[response_id]
    package = package[0:8] + temp["request_id"] + package[12:]
    return temp["fileno"], package

def parse_header(package):
    length = struct.unpack("<i", package[0:4])[0]
    request_id = struct.unpack("<i", package[4:8])[0]
    response_id = struct.unpack("<i", package[8:12])[0]
    operation = struct.unpack("<i", package[12:16])[0]
    return length, request_id, response_id, operation

def parse_package_query(package):
    flags = struct.unpack("<i", package[16:20])[0]
    full_collection_name = ""
    count = 0
    for i in range(len(package[20:])):
        if "\x00" == package[20+i]:
            break
        full_collection_name += package[20+i]
        count += 1
    number_to_skip = struct.unpack("<i", package[21+count:25+count])[0]
    number_to_return = struct.unpack("<i", package[25+count:29+count])[0]
    query = bson.BSON.decode(bson.BSON(package[29+count:]))

    return flags, full_collection_name, number_to_skip, number_to_return, query

def parse_package_update(package):
    full_collection_name = ""
    count = 0
    for i in range(len(package[20:])):
        if "\x00" == package[20+i]:
            break
        full_collection_name += package[20+i]
        count += 1
    flags = struct.unpack("<i", package[21+count:25+count])

    return full_collection_name, flags
    

def parse_package_reply(fileno, package):
    global CURSOR_ID_TABLE
    response_flags = struct.unpack("<i", package[16:20])[0]
    cursor_id = struct.unpack("<q", package[20:28])[0]
    starting_from = struct.unpack("<i", package[28:32])[0]
    number_returned = struct.unpack("<i", package[32:36])[0]

    if 0 != cursor_id:
        CURSOR_ID_TABLE.update({cursor_id:{"fileno":fileno, "time":int(time.time())}})

    return response_flags, cursor_id, starting_from, number_returned

def parse_package_get_more(package):
    full_collection_name = ""
    count = 0
    for i in range(len(package[20:])):
        if "\x00" == package[20+i]:
            break
        full_collection_name += package[20+i]
        count += 1
    number_to_return = struct.unpack("<i", package[21+count:25+count])[0]
    cursor_id = struct.unpack("<q", package[25+count:33+count])[0]

    return full_collection_name, number_to_return, cursor_id
