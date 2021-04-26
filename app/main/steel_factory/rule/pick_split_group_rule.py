# -*- coding: utf-8 -*-
# Description: 将列表stock_list按照属性attr1, attr2分组
# Created: jjunf 2020/09/29
from collections import defaultdict


def split_group(stock_list, attr1, attr2=None):
    """
    将stock_list按照attr1、attr2属性分组
    :param attr2:
    :param attr1:
    :param stock_list:
    :return:
    """
    # 结果字典：{‘attr1+attr2’：[货物列表]}
    stock_dict = defaultdict(list)
    # 两个参数分组
    if attr2:
        for stock in stock_list:
            key = getattr(stock, attr1) + ',' + getattr(stock, attr2)
            stock_dict[key].append(stock)
    # 一个参数分组
    else:
        for stock in stock_list:
            key = getattr(stock, attr1)
            stock_dict[key].append(stock)
    return stock_dict
