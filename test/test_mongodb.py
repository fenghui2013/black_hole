import pymongo, time
from gridfs import GridFS
from bson.objectid import ObjectId

def connect_db(db_name, coll_name=""):
    conn = pymongo.Connection("127.0.0.1", 8989)
    #conn["admin"].authenticate("test", "test")
    db = conn[db_name]
    coll = None
    if "" != coll_name:
        coll = db[coll_name]
    return db, coll

def test_normal_collection_insert(coll):
    msg = "a"*1024*4
    t1 = time.time()
    for i in range(10000):
        coll.insert({"a":msg})
        print i
    t2 = time.time()
    print "time:", t2-t1
    time.sleep(1)

def test_normal_collection_read(coll):
    for i in range(10000):
        result = coll.find({"_id":ObjectId("53bde13fa668d415fb6a3801")})[0]
        result = coll.find({"_id":ObjectId("53bde089a668d415db4b22ce")})[0]
        result = coll.find({"_id":ObjectId("53bde089a668d415db4b22da")})[0]
        result = coll.find({"_id":ObjectId("53bdfe17a668d41cfc25c70a")})[0]
        result = coll.find({"_id":ObjectId("53bdfe27a668d41d01e25c06")})[0]
        print i
    #time.sleep(1)

def test_gridfs_insert(db):
    fs = GridFS(db)
    msg = "a"*1024*1024*20
    fs.put(msg, filename="a")
    #for i in range(10000):
    #    fs.put(msg, filename="a")
    #    print i

def test_gridfs_read1(db):
    fs = GridFS(db)
    cur = fs.get(ObjectId("53c7468aa668d460fb7918cb"))
    print cur.read()
    #for i in range(1000):
    #    cur = fs.get(ObjectId("53b7d319c3666e0aab5363cb"))
    #    print cur.read()

def test_gridfs_read2(db):
    fs = GridFS(db)
    for i in range(1000):
        cur = fs.get(ObjectId("53b8b771c3666e0ad4f89337"))
        print cur.read()

if __name__ == "__main__":
    #db, coll = connect_db("log", "log")
    #test_normal_collection_insert(coll)
    #test_normal_collection_read(coll)
    #test_normal_collection_read2(coll)
    db, coll = connect_db("audio")
    #test_gridfs_insert(db)
    test_gridfs_read1(db)
    #test_gridfs_read2(db)
