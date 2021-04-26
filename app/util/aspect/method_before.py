# -*- coding: utf-8 -*-
# Description: 装饰器
# Created: shaoluyu 2019/03/05
import functools

from app.main.pipe_factory.entity.order import Order
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from app.util.weight_calculator import get_item_a_dict_list
from model_config import ModelConfig
from flask import g


def get_item_a(func):
    """
    加载重量计算基础数据
    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kw):
        items = []
        arg = args[0]
        if isinstance(arg, Order):
            items = arg.items
        elif isinstance(arg, list):
            items = [j for i in arg for j in i.items]
        else:
            raise MyException('未知请求！', ResponseCode.Error)
        item_a_dict = {}
        data = get_item_a_dict_list(items)
        for i in data:
            # 上线时若GBGZL_R有值，则取。无值则取GBGZL
            item_a_dict.setdefault(i['ITEMID'], {'GBGZL': i['GBGZL'], 'GS_PER': i['GS_PER']})
        g.ITEM_A_DICT = item_a_dict
        return func(*args, **kw)

    return wrapper


def param_init(func):
    """
    设置载重范围
    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(*args, **kw):
        # 重量初始化
        if args[0].truck_weight:
            weight = int(float(args[0].truck_weight) * 1000)
            if weight < 20000:
                raise MyException('输入的重量过小，请重新输入！', ResponseCode.Error)
            if weight >= 100000:
                raise MyException('输入的重量过大，请重新输入！', ResponseCode.Error)
            g.MAX_WEIGHT = weight
        else:
            # 设置默认值
            g.MAX_WEIGHT = ModelConfig.STANDARD_MAX_WEIGHT
        # 初始化车次号
        g.LOAD_TASK_ID = 0
        # 初始化单子类型
        g.DOC_TYPE = 't'
        return func(*args, **kw)

    return wrapper
