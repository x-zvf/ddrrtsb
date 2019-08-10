import redis, time, os
rdb = redis.Redis()

while True:
    os.system("clear")
    print("------------")
    print("comments: %d" % rdb.llen("rq:queue:comments"))
    print("rants: %d" % rdb.llen("rq:queue:rants"))
    print("standard: %d" % rdb.llen("rq:queue:standard"))
    print("new: %d" % rdb.llen("rq:queue:new"))
    time.sleep(0.2)