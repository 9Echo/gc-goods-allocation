import json
import datetime as datetime

import redis
from flask import current_app
from app.util.redis.redis_pool import redis_pool

hurry_consumer_key = 'hurry_consumer'
priority_key = 'priority'


def set_hurry_consumer_list(hurry_consumer_list):
    """
    更新催货客户名单
    """
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        if not hurry_consumer_list:
            return False

        json_data = json.dumps(hurry_consumer_list)
        redis_conn.set(hurry_consumer_key, json_data)
        # 设置每天结束时过期
        expire_date = datetime.datetime.combine(datetime.date.today(),
                                                datetime.time(23, 59, 59))
        redis_conn.expireat(hurry_consumer_key, expire_date)
        return True
    except Exception as e:
        current_app.logger.info("set hurry consumer list error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()


def get_hurry_consumer_list():
    """
    获取催货客户名单
    """
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        json_list = redis_conn.get(hurry_consumer_key)
        if json_list:
            hurry_consumer_list = json.loads(json_list)
            return hurry_consumer_list
        else:
            return []
    except Exception as e:
        current_app.logger.info("get hurry consumer list error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()


def set_priority_dict(data):
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        if not data:
            return False
        json_data = json.dumps(data)
        redis_conn.set(priority_key, json_data)
        # 设置每天结束时过期
        expire_date = datetime.datetime.combine(datetime.date.today(),
                                                datetime.time(23, 59, 59))
        redis_conn.expireat(priority_key, expire_date)
        return True
    except Exception as e:
        current_app.logger.info("set_priority_dict error" + json.dumps(data))
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()


def get_priority_dict():
    redis_conn = None
    try:
        redis_conn = redis.Redis(connection_pool=redis_pool)
        json_list = redis_conn.get(priority_key)
        if json_list:
            return json.loads(json_list)
        else:
            return {}
    except Exception as e:
        current_app.logger.info("get_priority_dict error")
        current_app.logger.exception(e)
    finally:
        if redis_conn:
            redis_conn.close()

# def set_batch_priority_dict(data):
#     """
#     批量设置
#     :param data: {订单hash:'{报道号：1}'}
#     :return:
#     """
#     redis_conn = None
#     try:
#         redis_conn = redis.Redis(connection_pool=redis_pool)
#         if not data:
#             return False
#         redis_conn.hmset(priority_key, {key: json.dumps(value) for key, value in data.items()})
#         # 设置每天结束时过期
#         expire_date = datetime.datetime.combine(datetime.date.today(),
#                                                 datetime.time(23, 59, 59))
#         redis_conn.expireat(priority_key, expire_date)
#         return True
#     except Exception as e:
#         current_app.logger.info("set_priority_dict error" + " data:" + json.dumps(data))
#         current_app.logger.exception(e)
#     finally:
#         if redis_conn:
#             redis_conn.close()
#
#
# def insert_batch_priority_dict(data):
#     """
#     批量设置
#     :param data: {订单hash:'{报道号：1}'}
#     :return:
#     """
#     redis_conn = None
#     try:
#         redis_conn = redis.Redis(connection_pool=redis_pool)
#         if not data:
#             return False
#         json_dict = redis_conn.hmget(priority_key, [key for key in data.keys()])
#         if json_dict:
#             return {key: json.loads(value) for key, value in json_dict.items()}
#         redis_conn.hmset(priority_key, {key: json.dumps(value) for key, value in data.items()})
#         # 设置每天结束时过期
#         expire_date = datetime.datetime.combine(datetime.date.today(),
#                                                 datetime.time(23, 59, 59))
#         redis_conn.expireat(priority_key, expire_date)
#         return True
#     except Exception as e:
#         current_app.logger.info("set_priority_dict error" + " data:" + json.dumps(data))
#         current_app.logger.exception(e)
#     finally:
#         if redis_conn:
#             redis_conn.close()
#
#
# def get_priority_dict():
#     """
#     获取急发数据
#     """
#     redis_conn = None
#     try:
#         redis_conn = redis.Redis(connection_pool=redis_pool)
#         json_dict = redis_conn.hgetall(priority_key)
#         if json_dict:
#             return {key: json.loads(value) for key, value in json_dict.items()}
#         else:
#             return {}
#     except Exception as e:
#         current_app.logger.info("get_priority_dict error")
#         current_app.logger.exception(e)
#     finally:
#         if redis_conn:
#             redis_conn.close()

# def set_priority_dict(stock_hash, data: Dict):
#     """
#     设置  stock_hash：{报道号：1}键值对
#     :param stock_hash:  订单号、合同号、发货通知单号哈希值
#     :param data: 字典{报道号：1}
#     :return:
#     """
#     redis_conn = None
#     try:
#         redis_conn = redis.Redis(connection_pool=redis_pool)
#         if not stock_hash:
#             return False
#         # json_data = json.dumps(data)
#         # redis_conn.set(stock_hash, json_data)
#         redis_conn.hset(priority_key, stock_hash, data)
#         # 设置每天结束时过期
#         expire_date = datetime.datetime.combine(datetime.date.today(),
#                                                 datetime.time(23, 59, 59))
#         redis_conn.expireat(priority_key, expire_date)
#         return True
#     except Exception as e:
#         current_app.logger.info("set_priority_dict error" + " data:" + json.dumps(data))
#         current_app.logger.exception(e)
#     finally:
#         if redis_conn:
#             redis_conn.close()

# if __name__ == '__main__':
#     print(get_priority_dict())
#     set_batch_priority_dict({'426224508': '{"DD200922000004": 1}', 'fsdaf': '{"DD200922000004": 1}'})
#     print(get_priority_dict())
