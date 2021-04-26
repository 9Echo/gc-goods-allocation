# -*- coding: utf-8 -*-
# Description: 将库存分类为标载和非标载
# Created: jjunf 2020/09/29
from typing import List

from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule.pick_compose_public_method import get_weight
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig


def stock_filter(stock_list: List[Stock]):
    """
    将库存分为两类：标准库存、尾货库存
    :param stock_list:
    :return:
    """
    # 存放标载的库存列表
    huge_list = []
    # 存分非标载的库存列表
    tail_list = []
    for stock in stock_list:

        """####重量配置"""
        # min_weight, max_weight = get_weight(stock)
        # if min_weight <= stock.actual_weight <= max_weight:
        #     huge_list.append(stock)
        # else:
        #     tail_list.append(stock)

        if (stock.city == "滨州市" and stock.big_commodity_name in ModelConfig.RG_J_GROUP and get_lower_limit(
                stock.big_commodity_name) <= stock.actual_weight < ModelConfig.RG_MIN_WEIGHT):
            tail_list.append(stock)
        elif get_lower_limit(stock.big_commodity_name) <= stock.actual_weight <= (
                ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT):
            huge_list.append(stock)
        else:
            tail_list.append(stock)
    return huge_list, tail_list
