# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/7/13 17:36
import functools
import redis
from app.util.redis.redis_pool import redis_pool
from app.util.redis.redis_lock import RedisLock
from app.util.result import Result


def distribute_lock(func):
    """
    分布式锁，目标方法返回值默认是Result
    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kw):
        redis_conn = redis.Redis(connection_pool=redis_pool)
        lock_id = RedisLock.try_lock(redis_conn, 'rg_stock_lock', wait_time=20)
        if lock_id:
            try:
                func_return = func(*args, **kw)
            finally:
                RedisLock.unlock(redis_conn, 'rg_stock_lock', lock_id)
        else:
            func_return = Result.error('当前系统繁忙，请稍后再试！')
        return func_return

    return wrapper
