TRANSLATE_TABLE = {}
DOWN_NODE_LIST = []

def down_node_list_append(value):
    global DOWN_NODE_LIST
    DOWN_NODE_LIST.append(value)

def translate_table_put(fileno, value):
    global TRANSLATE_TABLE
    TRANSLATE_TABLE.update({fileno:value})

def translate_table_del(fileno):
    global TRANSLATE_TABLE, DOWN_NODE_LIST
    temp = TRANSLATE_TABLE[fileno]
    DOWN_NODE_LIST.append(temp)
    del TRANSLATE_TABLE[fileno]


def translate_table_get_insert():
    for fileno in TRANSLATE_TABLE.keys():
        temp = TRANSLATE_TABLE[fileno]
        if "normal" == temp["role"] and "true" == temp["writable"]:
            return fileno
    return None

def translate_table_get_bak():
    for fileno in TRANSLATE_TABLE.keys():
        temp = TRANSLATE_TABLE[fileno]
        if "bak" == temp["role"]:
            return fileno
    return None

def translate_table_get_read(db_coll, key, value):
    for fileno in TRANSLATE_TABLE.keys():
        temp = TRANSLATE_TABLE[fileno]
        if "normal" == temp["role"]:
            if "_id" == key:
                if "*" == temp[db_coll]["_id_max"]:
                    if value >= temp[db_coll]["_id_min"]:
                        return fileno
                else:
                    if value >= temp[db_coll]["_id_min"] and value <= temp[db_coll]["_id_max"]:
                        return fileno
            elif "filename" == key:
                if "*" == temp[db_coll]["filename_max"]:
                    if value >= temp[db_coll]["filename_min"]:
                        return fileno
                else:
                    if value >= temp[db_coll]["filename_min"] and value <= temp[db_coll]["filename_max"]:
                        return fileno
            elif "files_id" == key:
                if "*" == temp[db_coll]["files_id_max"]:
                    if value >= temp[db_coll]["files_id_min"]:
                        return fileno
                else:
                    if value >= temp[db_coll]["files_id_min"] and value <= temp[db_coll]["files_id_max"]:
                        return fileno
    return None
