# -*- coding: utf-8 -*-
# Description: 将同区县同仓库的尾货组合配载
# Created: jjunf 2020/10/09
from typing import List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.main.steel_factory.rule.pick_compose_public_method import get_optimal_group, merge_result, get_weight
from app.main.steel_factory.rule.pick_compose_public_method import delete_the_stock_be_composed
from app.main.steel_factory.rule.split_rule import split
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from model_config import ModelConfig


def surplus_compose(stock_dict, mark=None):
    """
    将同区县、同仓库，不同客户的尾货组合配载（一装两卸）
    '''1.先匹配同品种的货物'''
    '''2.再匹配不同品种的货物'''
    :param mark:
    :param stock_dict:
    :return:
    """
    # 组合成标载的货物列表
    load_task_list: List[LoadTask] = []
    # temp_stock_list中组合后剩余的尾货列表
    surplus_stock_list = []
    # 依次将每个客户的货物组合
    for key in stock_dict.keys():
        # 拿到当前区县当前客户的尾货列表
        temp_stock_list = stock_dict[key]
        '''2.再匹配不同品种的货物'''
        surplus_list = []  # temp_stock_list中剩余的
        # 按重量降序排序
        temp_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
        while temp_stock_list:
            temp_stock = temp_stock_list[0]
            filter_list = [stock for stock in temp_stock_list if stock is not temp_stock
                           and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
                temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
            min_weight, max_weight = get_weight(temp_stock, mark)
            target_compose_list = get_optimal_group(filter_list, temp_stock, max_weight - temp_stock.actual_weight,
                                                    min_weight - temp_stock.actual_weight, 'consumer')
            # 如果配载成功
            if target_compose_list:
                target_compose_list.append(temp_stock)
                # 组合配载
                load_task = create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_4.value)
                load_task_list.append(merge_result(load_task))
                # 删除库存中已经被组合的货物
                temp_stock_list = delete_the_stock_be_composed(temp_stock_list, target_compose_list)
            # 如果配载不成功
            else:
                # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
                if temp_stock.actual_number > 1:
                    temp_stock_split = split([temp_stock])
                    target_compose_list = get_optimal_group(temp_stock_split + filter_list, temp_stock,
                                                            max_weight, min_weight, 'consumer')
                    # 组合成功，满足重量下限要求
                    if target_compose_list:
                        load_task_list.append(
                            merge_result(
                                create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_4.value)))
                        temp_stock_list = delete_the_stock_be_composed(temp_stock_list, target_compose_list)
                    else:
                        surplus_list.extend([temp for temp in temp_stock_list
                                             if temp_stock.actual_weight == temp.actual_weight
                                             and temp_stock.actual_number == temp.actual_number])
                        temp_stock_list = [temp for temp in temp_stock_list
                                           if temp_stock.actual_weight != temp.actual_weight
                                           or temp_stock.actual_number != temp.actual_number]
                # 如果temp_stock整体配载不成功，拆分也配载不成功
                else:
                    surplus_list.extend([temp for temp in temp_stock_list
                                         if temp_stock.actual_weight == temp.actual_weight
                                         and temp_stock.actual_number == temp.actual_number])
                    temp_stock_list = [temp for temp in temp_stock_list
                                       if temp_stock.actual_weight != temp.actual_weight
                                       or temp_stock.actual_number != temp.actual_number]
        # 最后剩余的货物
        surplus_stock_list.extend(surplus_list)
    return load_task_list, surplus_stock_list
