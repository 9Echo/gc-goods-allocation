# -*- coding: utf-8 -*-
# Description: 跨区县拼货：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)、两装两卸(非同区仓库、跨区县)
# Created: jjunf 2021/01/06
from model_config import ModelConfig
from app.main.steel_factory.rule.pick_compose import compose_method
from param_config import ParamConfig


def across_district_compose_in_one_factory(stock_list, load_task_list):
    """
    跨区县拼货：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)
    :param load_task_list:
    :param stock_list:
    :return:组合后剩余的库存
    """

    """####跨区县配置"""  # 替换下面24-28行
    # # 没有需要跨区县拼货的区县、或者没有可拼的货物
    # if not ParamConfig.ACROSS_DISTRICT_DICT or not stock_list:
    #     return stock_list
    # # 按优先级跨区县拼货
    # for district_value_list in ParamConfig.ACROSS_DISTRICT_DICT.values():

    # 没有需要跨区县拼货的区县、或者没有可拼的货物
    if not ModelConfig.ACROSS_DISTRICT_DICT or not stock_list:
        return stock_list
    # 按优先级跨区县拼货
    for district_value_list in ModelConfig.ACROSS_DISTRICT_DICT.values():
        # 区县1、区县2
        district_one, district_other = district_value_list
        # 区县1的货物
        one_stock_list = [stock for stock in stock_list if stock.dlv_spot_name_end == district_one]
        # 区县2的货物
        other_stock_list = [stock for stock in stock_list if stock.dlv_spot_name_end == district_other]
        # 区县1、区县2之外的其他区县的货物
        surplus_stock_list = [stock for stock in stock_list
                              if stock.dlv_spot_name_end != district_one and stock.dlv_spot_name_end != district_other]
        # 让区县1和区县2跨区县拼货：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)
        temp_load_task_list, one_stock_list, other_stock_list = compose_method(one_stock_list, other_stock_list, flag=2)
        load_task_list.extend(temp_load_task_list)
        # 更新stock_list
        stock_list = one_stock_list + other_stock_list + surplus_stock_list
        if not stock_list:
            return stock_list
    return stock_list


def across_district_compose_in_two_factory(one_stock_list, other_stock_list, load_task_list):
    """
    跨区县拼货：两装两卸(非同区仓库、跨区县)
    :param other_stock_list:
    :param one_stock_list:
    :param load_task_list:
    :return:
    """

    """####跨区县配置"""  # 替换下面64-68行
    # # 没有需要跨区县拼货的区县、或者没有可拼的货物
    # if not ParamConfig.ACROSS_DISTRICT_DICT or not one_stock_list or not other_stock_list:
    #     return one_stock_list, other_stock_list
    # # 按优先级跨区县拼货
    # for district_value_list in ParamConfig.ACROSS_DISTRICT_DICT.values():

    # 没有需要跨区县拼货的区县、或者没有可拼的货物
    if not ModelConfig.ACROSS_DISTRICT_DICT or not one_stock_list or not other_stock_list:
        return one_stock_list, other_stock_list
    # 按优先级跨区县拼货
    for district_value_list in ModelConfig.ACROSS_DISTRICT_DICT.values():
        # 区县1、区县2
        district_one, district_other = district_value_list
        # 区县1的货物
        one_district_stock_list = [stock for stock in one_stock_list if stock.dlv_spot_name_end == district_one]
        # 区县1之外的其他区县的货物
        surplus_one_district_stock_list = [stock for stock in one_stock_list if stock.dlv_spot_name_end != district_one]
        # 区县2的货物
        other_district_stock_list = [stock for stock in other_stock_list if stock.dlv_spot_name_end == district_other]
        # 区县2之外的其他区县的货物
        surplus_other_district_stock_list = [stock for stock in other_stock_list
                                             if stock.dlv_spot_name_end != district_other]
        # 让区县1和区县2跨区县拼货：两装两卸(非同区仓库、跨区县)
        temp_load_task_list, one_district_stock_list, other_district_stock_list = compose_method(
            one_district_stock_list, other_district_stock_list, flag=3)
        load_task_list.extend(temp_load_task_list)
        # 更新one_stock_list、other_stock_list
        one_stock_list = one_district_stock_list + surplus_one_district_stock_list
        other_stock_list = other_district_stock_list + surplus_other_district_stock_list
        if not one_stock_list or not other_stock_list:
            return one_stock_list, other_stock_list
    return one_stock_list, other_stock_list
