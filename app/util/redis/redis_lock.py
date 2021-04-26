# -*- coding: utf-8 -*-
# Description: redis分布式锁
# Created: shaoluyu 2019/10/10
# Modified: shaoluyu 2019/10/21; shaoluyu 2019/10/24

import time
import uuid
import redis


class RedisLock:
    """
    基于redis的分布式锁
    """

    DEL_SCRIPT = '''
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    '''

    @staticmethod
    def try_lock(redis_client: redis.Redis, lock_name, wait_time=10, lock_expire_time=10):
        """获取一个锁

        :param redis_client: redis连接
        :param lock_name: 锁的名称，加上前缀"util:redis:lock:" 后作为锁的redis key
        :param wait_time: 客户端获取锁的最大等待时间（秒），超时则获取失败
        :param lock_expire_time: 锁的过期时间（秒）
        :return: 锁的编号id，解锁时需要提供该编号。如果获取锁超时，则返回False
        """

        lock_key = "util:redis:lock:" + lock_name
        lock_identifier = str(uuid.uuid4())
        #
        sleep_time = 0.05
        if wait_time < sleep_time:
            wait_time = sleep_time
        end = time.time() + wait_time
        while time.time() < end:
            if redis_client.set(lock_key, lock_identifier, ex=lock_expire_time, nx=True):
                return lock_identifier
            time.sleep(sleep_time)
        #
        return False

    @staticmethod
    def unlock(redis_client, lock_name, lock_identifier, retry_time=0):
        """释放锁

        :param redis_client: redis连接
        :param lock_name: 锁的名称，加上前缀"util:redis:lock:" 后作为锁的redis key
        :param lock_identifier: 锁的编号，只有与redis中该锁的编号值相同时才能解锁
        :param retry_time: 重试超时时间（秒）
        :return:
        """

        lock_key = "util:redis:lock:" + lock_name
        #
        sleep_time = 0.1
        if retry_time < sleep_time:
            retry_time = sleep_time
        end = time.time() + retry_time
        while time.time() < end:
            if redis_client.eval(RedisLock.DEL_SCRIPT, 1, lock_key, lock_identifier):
                return True
            time.sleep(sleep_time)
        #
        return False


