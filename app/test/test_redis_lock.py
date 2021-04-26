import os
import sys
import time
from threading import Thread

import redis

# 设置路径包的搜索路径
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
print(curPath)
print(rootPath)

from app.util.redis.redis_lock import RedisLock

redis_pool = redis.ConnectionPool(
    host='47.99.118.183', port=6379, password="wobugaoxing", max_connections=50,
    encoding='utf8', decode_responses=True)


def seckill(i):
    redis_conn = redis.Redis(connection_pool=redis_pool)
    lock_id = RedisLock.try_lock(redis_conn, 'seckill', wait_time=20)
    if lock_id:
        try:
            print("线程:{}--获得了锁".format(i))
            time.sleep(1)
            count = int(redis_conn.get('seckill:tickets'))
            if count < 1:
                print("线程:{}--没抢到，票抢完了".format(i))
            else:
                count -= 1
                redis_conn.set('seckill:tickets', count)
                print("线程:{}--抢到一张票，还剩{}张票".format(i, count))
        finally:
            RedisLock.unlock(redis_conn, 'seckill', lock_id)
    else:
        print("线程:{}--活动太热烈了，可重新试一下".format(i))


def seckill2(i):
    redis_conn = redis.Redis(connection_pool=redis_pool)
    while True:
        lock_id = RedisLock.try_lock(redis_conn, 'seckill')
        if lock_id:
            try:
                print("线程:{}--获得了锁".format(i))
                time.sleep(1)
                count = int(redis_conn.get('seckill:tickets'))
                if count < 1:
                    print("线程:{}--没抢到，票抢完了".format(i))
                else:
                    count -= 1
                    redis_conn.set('seckill:tickets', count)
                    print("线程:{}--抢到一张票，还剩{}张票".format(i, count))
            finally:
                RedisLock.unlock(redis_conn, 'seckill', lock_id)
                return


if __name__ == "__main__":
    redis_conn = redis.Redis(connection_pool=redis_pool)
    redis_conn.set('seckill:tickets', 10)
    for i in range(30):
        t = Thread(target=seckill, args=(i,))
        t.start()
