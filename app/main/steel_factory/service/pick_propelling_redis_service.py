# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/24

import json

import redis
from flask import current_app
from app.util.redis.redis_pool import redis_pool

pick_propelling_driver_key = 'models:pick_propelling_driver'


def set_pick_propelling_driver_list(pick_propelling_driver_list):
    """
    更新推送司机名单
    """
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        if not pick_propelling_driver_list:
            return False
        json_data = json.dumps(pick_propelling_driver_list)
        redis_conn.set(pick_propelling_driver_key, json_data)
        # # 设置每天结束时过期
        # expire_date = datetime.datetime.combine(datetime.date.today(),
        #                                         datetime.time(23, 59, 59))
        # redis_conn.expireat(pick_propelling_driver_key, expire_date)
        return True
    except Exception as e:
        current_app.logger.info("set pick_propelling_driver list error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()


def get_pick_propelling_driver_list():
    """
    获取推送司机名单
    """
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        json_list = redis_conn.get(pick_propelling_driver_key)
        if json_list:
            pick_propelling_driver_list = json.loads(json_list)
            return pick_propelling_driver_list
        else:
            return []
    except Exception as e:
        current_app.logger.info("get pick_propelling_driver list error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()
