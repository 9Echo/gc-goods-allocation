# -*- coding: utf-8 -*-
# Description: Redis连接池
# Created: shaoluyu 2019/10/08
# Modified: shaoluyu 2019/10/10;

import redis

import config

active_config = config.get_active_config()

redis_pool = None
if hasattr(active_config, 'REDIS_HOST'):
    # 创建redis连接池。
    # 后续redis操作应从连接池获取连接，并在操作完成后关闭归还连接。
    redis_pool = redis.ConnectionPool(
        host=active_config.REDIS_HOST,
        port=active_config.REDIS_PORT,
        password=active_config.REDIS_PASSWD,
        max_connections=active_config.REDIS_MAX_CONNECTIONS,
        encoding='utf-8',
        decode_responses=True)
