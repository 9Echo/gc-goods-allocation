# -*- coding: utf-8 -*-
# Description: 内存数据库服务
# Created: shaoluyu 2019/12/04
import json
import redis
from flask import current_app
from app.util.redis.redis_pool import redis_pool
from app.main.pipe_factory.entity.delivery_item import DeliveryItem
from app.util.result import Result


def set_delivery_list(delivery_list):
    """
    将推荐装车清单暂存
    :param delivery_list:
    :return:
    """
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        if not delivery_list:
            return Result.error("无数据！")
        batch_no = getattr(delivery_list[0], "batch_no", None)
        if batch_no:
            dict_list = Result.from_entity(delivery_list).data
            json_data = json.dumps(dict_list)
            redis_conn.set(batch_no, json_data, ex=300)
            return Result.info(msg="保存成功")
    except Exception as e:
        current_app.logger.info("set_delivery_list error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()


def get_delivery_list(batch_no):
    """
    获取对应批次号的发货通知单列表
    :param batch_no:
    :return:
    """
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        json_list = redis_conn.get(batch_no)
        if json_list:
            delivery_list = json.loads(json_list)
            items = []
            for i in delivery_list:
                items.extend([DeliveryItem(j) for j in i.get('items')])
            return Result.success(data=items)
        else:
            return None
    except Exception as e:
        current_app.logger.info("get_delivery_list error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()

