## black_hole
A middleware is basing on mongodb for storing big data, that is any size.

## start and stop
```
python black_hole.py start
python black_hole.py stop
```

## config
```
{
    "pwd":"path/to/black_hole",
    "blackhole": {
        "ip":"10.12.7.55",
        "port":8989,
        "daemon":"true"
    },
    "log":{
        "blackhole":{
            "path":"/tmp/blackhole.log",
            "level":"info"
        },
        "translate":{
            "path":"/tmp/translate.log",
            "level":"info"
        }
    },
    "daemon": {
        "stdin":"/dev/null",
        "stdout":"/dev/null",
        "stderr":"/dev/null",
        "pidfile":"/tmp/bh.pid"
    },
    "translate_table": [
        {
            "ip":"10.12.7.55",
            "port":27017,
            "readable":"true",
            "writable":"true",
            "role":"normal",
            "result.result": {
                "_id_min":"",
                "_id_max":"*"
            },
            "log.log": {
                "_id_min":"",
                "_id_max":"*"
            },
            "audio.fs.files": {
                "filename_min": "",
                "filename_max": "*",
                "_id_min": "",
                "_id_max": "*"
            },
            "audio.fs.chunks": {
                "files_id_min": "",
                "files_id_max": "*"
            }
        },
        {
            "ip":"10.12.7.56",
            "port":30000,
            "readable":"false",
            "writable":"true",
            "role":"bak"
        }
    ]
}
```
