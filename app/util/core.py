# -*- coding: utf-8 -*-
# Description: 核心工具包
# Created: shaoluyu 2019/06/27
# Modified: shaoluyu 2019/06/27; shaoluyu 2019/06/27

import datetime
import decimal
import uuid

# import json
from flask.json import JSONEncoder as BaseJSONEncoder
# from flask_restful import fields
# from sqlalchemy.ext.declarative import DeclarativeMeta


class JSONEncoder(BaseJSONEncoder):
    """json编码器

    扩展支持datetime, Decimal, uuid, bytes
    """

    def default(self, o):
        """
        如有其他的需求可直接在下面添加
        :param o:
        :return:
        """
        if isinstance(o, datetime.datetime):
            # 格式化时间
            return o.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(o, datetime.date):
            # 格式化日期
            return o.strftime('%Y-%m-%d')
        if isinstance(o, decimal.Decimal):
            # 格式化高精度数字
            return str(o)
        if isinstance(o, uuid.UUID):
            # 格式化uuid
            return str(o)
        if isinstance(o, bytes):
            # 格式化字节数据
            return o.decode("utf-8")
        return super(JSONEncoder, self).default(o)


# class AlchemyEncoder(json.JSONEncoder):
#     """Alchemy实体的json编码器
#     """
#
#     def default(self, obj):
#         if isinstance(obj.__class__, DeclarativeMeta):
#             # an SQLAlchemy class
#             fields = {}
#             for field in [x for x in dir(obj)
#                           if not x.startswith('_') and x != 'metadata' and
#                           x not in ('as_dict', 'query', 'query_class')]:
#                 data = obj.__getattribute__(field)
#                 try:
#                     if isinstance(data, (str, int, float)):
#                         fields[field] = data
#                     elif isinstance(data, datetime.datetime):
#                         # fields[field] = data.isoformat(sep=' ')
#                         fields[field] = data.strftime("%Y-%m-%d %H:%M:%S")
#                     elif isinstance(data, datetime.date):
#                         # fields[field] = data.isoformat()
#                         fields[field] = data.strftime('%Y-%m-%d')
#                     elif isinstance(data, datetime.timedelta):
#                         fields[field] = (datetime.datetime.min + data).time().\
#                             isoformat()
#                     elif isinstance(data, decimal.Decimal):
#                         # 格式化高精度数字
#                         fields[field] = data.str()
#                     else:
#                         # this will fail on non-encodable values,
#                         # like other classes
#                         json.dumps(data)
#                         fields[field] = data
#                 except TypeError:
#                     fields[field] = None
#             # a json-encodable dict
#             return fields
#
#         return json.JSONEncoder.default(self, obj)
#
#
# def parse_dict_for_sqlalchemy(data):
#     """将sqlalchemy的实体/实体列表转换为字典/字典列表
#
#     同时会将字典中datetime和Decimal类型的值转换为字符串
#     :param: data 实体/实体列表
#     :return: 转换后的字典/字典列表
#     """
#     if isinstance(data, (list, tuple)):
#         model = type(data[0])
#     else:
#         model = type(data)
#
#     if isinstance(model, DeclarativeMeta):
#         if isinstance(data, (list, tuple)):
#             dict_data_list = list(
#                 map(lambda x: {
#                     p.key: getattr(x, p.key)
#                     for p in model.__mapper__.iterate_properties},
#                     data))
#             for i in range(len(dict_data_list)):
#                 encode_dict_data(dict_data_list[i])
#             return dict_data_list
#         else:
#             dict_data = {
#                 p.key: getattr(data, p.key)
#                 for p in model.__mapper__.iterate_properties}
#             encode_dict_data(dict_data)
#             return dict_data
#     else:
#         return None
#
#
# def encode_dict_data(dict_data):
#     """将字典中datetime和Decimal类型的值转换为字符串
#
#     :param: dict_data 字典数据
#     :return:
#     """
#     if isinstance(dict_data, dict):
#         for k, v in dict_data.items():
#             if isinstance(v, datetime.datetime):
#                 dict_data[k] = v.strftime("%Y-%m-%d %H:%M:%S")
#             elif isinstance(v, datetime.date):
#                 dict_data[k] = v.strftime('%Y-%m-%d')
#             elif isinstance(v, decimal.Decimal):
#                 # 格式化高精度数字
#                 dict_data[k] = v.str()
#
#
# class DateTimeField(fields.Raw):
#     """自定义的marshal序列化datetime字段
#     """
#     def format(self, value):
#         return value.strftime("%Y-%m-%d %H:%M:%S")
