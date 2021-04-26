# -*- coding: utf-8 -*-
# Description: 对于卷类的处理，黑白配等
# Created: jjunf 2020/10/13
from typing import List

from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.rule.pick_compose_in_same_district_and_consumer import goods_compose
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.main.steel_factory.rule.pick_compose_public_method import merge_result, delete_the_stock_be_composed
from app.main.steel_factory.rule.pick_compose_public_method import get_weight
from app.main.steel_factory.rule.pick_compose_public_method import get_optimal_group
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from model_config import ModelConfig


def compose_1_to_1(one_stock_list, other_stock_list, load_task_type=LoadTaskType.TYPE_1.value, across_district_flag=0):
    """
    拿one_stock_list组合other_stock_list (一装一卸)
    :param across_district_flag:
    :param load_task_type:
    :param one_stock_list:
    :param other_stock_list:
    :return:
    """
    # 如果没有可以组合的货物
    if not one_stock_list or not other_stock_list:
        return [], one_stock_list, other_stock_list
    # 组合的货物列表
    load_task_list: List[LoadTask] = []
    one_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    other_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    # one_stock_list中剩余的
    surplus_one_stock_list = []
    while one_stock_list:
        temp_stock = one_stock_list[0]
        # 找出待匹配的列表
        filter_list = [stock for stock in other_stock_list if stock.consumer == temp_stock.consumer
                       and stock.dlv_spot_name_end == temp_stock.dlv_spot_name_end
                       and stock.deliware_house == temp_stock.deliware_house
                       and (stock.big_commodity_name in
                            ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
                                                               [temp_stock.big_commodity_name]))]
        min_weight, max_weight = get_weight(temp_stock)
        # 得到被组合的货物列表
        load_task, target_compose_list = goods_compose(filter_list, temp_stock, max_weight - temp_stock.actual_weight,
                                                       min_weight, load_task_type)
        # 如果组合成功
        if target_compose_list:
            load_task_list.append(load_task)
            # 删除one_stock_list中的temp_stock
            one_stock_list = delete_the_stock_be_composed(one_stock_list, [temp_stock])
            # 删除other_stock_list中的temp_stock
            other_stock_list = delete_the_stock_be_composed(other_stock_list, target_compose_list)
        # 如果配载不成功
        else:
            surplus_one_stock_list.extend([temp for temp in one_stock_list
                                           if temp_stock.actual_weight == temp.actual_weight
                                           and temp_stock.actual_number == temp.actual_number])
            one_stock_list = [temp for temp in one_stock_list
                              if temp_stock.actual_weight != temp.actual_weight
                              or temp_stock.actual_number != temp.actual_number]
    return load_task_list, surplus_one_stock_list, other_stock_list


def compose_2_to_1(one_stock_list, other_stock_list, load_task_type=LoadTaskType.TYPE_3.value, across_district_flag=0):
    """
    拿one_stock_list组合other_stock_list（两装一卸：如果传入的one_stock_list和other_stock_list是同区的则是同区两装一卸，否则是非同区两装一卸）
    :param across_district_flag:
    :param load_task_type:
    :param other_stock_list:
    :param one_stock_list:
    :return:
    """
    # 如果没有可以组合的货物
    if not one_stock_list or not other_stock_list:
        return [], one_stock_list, other_stock_list
    # 组合的货物列表
    load_task_list: List[LoadTask] = []
    one_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    other_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    # one_stock_list中剩余的
    surplus_one_stock_list = []
    while one_stock_list:
        temp_stock = one_stock_list[0]
        # 找出西区中待匹配的列表
        filter_list = [stock for stock in other_stock_list if stock.consumer == temp_stock.consumer
                       and stock.dlv_spot_name_end == temp_stock.dlv_spot_name_end
                       and (stock.big_commodity_name in
                            ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
                                                               [temp_stock.big_commodity_name]))]
        min_weight, max_weight = get_weight(temp_stock)
        # 得到被组合的货物列表
        target_compose_list = get_optimal_group(filter_list, temp_stock, max_weight - temp_stock.actual_weight,
                                                min_weight - temp_stock.actual_weight, 'deliware_house')
        # 如果组合成功
        if target_compose_list:
            load_task_list.append(merge_result(create_load_task(target_compose_list + [temp_stock], GenerateId.get_id(),
                                                                load_task_type)))
            # 删除one_stock_list中的temp_stock
            one_stock_list = delete_the_stock_be_composed(one_stock_list, [temp_stock])
            # 删除other_stock_list中的temp_stock
            other_stock_list = delete_the_stock_be_composed(other_stock_list, target_compose_list)
        # 如果配载不成功
        else:
            surplus_one_stock_list.extend([temp for temp in one_stock_list
                                           if temp_stock.actual_weight == temp.actual_weight
                                           and temp_stock.actual_number == temp.actual_number])
            one_stock_list = [temp for temp in one_stock_list
                              if temp_stock.actual_weight != temp.actual_weight
                              or temp_stock.actual_number != temp.actual_number]
    return load_task_list, surplus_one_stock_list, other_stock_list


def compose_1_to_2(one_stock_list, other_stock_list, load_task_type=LoadTaskType.TYPE_4.value, across_district_flag=0):
    """
    拿one_stock_list去组合other_stock_list中的货物（一装两卸、一装两卸(跨区县)）
    :param across_district_flag: 默认不跨区县across_district_flag=0；跨区县across_district_flag=1
    :param load_task_type:
    :param other_stock_list:
    :param one_stock_list:
    :return:
    """
    # 如果没有可以组合的货物
    if not one_stock_list or not other_stock_list:
        return [], one_stock_list, other_stock_list
    # 组合的货物列表
    load_task_list: List[LoadTask] = []
    one_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    other_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    # one_stock_list中剩余的
    surplus_one_stock_list = []
    while one_stock_list:
        temp_stock = one_stock_list[0]
        # 找出other_stock_list中待匹配的列表
        if across_district_flag:
            # 如果跨区县卸货
            filter_list = [stock for stock in other_stock_list if stock.deliware_house == temp_stock.deliware_house
                           and (stock.big_commodity_name in
                                ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
                                                                   [temp_stock.big_commodity_name]))]
        else:
            filter_list = [stock for stock in other_stock_list if stock.deliware_house == temp_stock.deliware_house
                           and stock.dlv_spot_name_end == temp_stock.dlv_spot_name_end
                           and (stock.big_commodity_name in
                                ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
                                                                   [temp_stock.big_commodity_name]))]
        min_weight, max_weight = get_weight(temp_stock)
        # 得到被组合的货物列表
        target_compose_list = get_optimal_group(filter_list, temp_stock, max_weight - temp_stock.actual_weight,
                                                min_weight - temp_stock.actual_weight, 'consumer')
        # 如果组合成功
        if target_compose_list:
            load_task_list.append(merge_result(create_load_task(target_compose_list + [temp_stock], GenerateId.get_id(),
                                                                load_task_type)))
            # 删除one_stock_list中的temp_stock
            one_stock_list = delete_the_stock_be_composed(one_stock_list, [temp_stock])
            # 删除other_stock_list中的temp_stock
            other_stock_list = delete_the_stock_be_composed(other_stock_list, target_compose_list)
        # 如果配载不成功
        else:
            surplus_one_stock_list.extend([temp for temp in one_stock_list
                                           if temp_stock.actual_weight == temp.actual_weight
                                           and temp_stock.actual_number == temp.actual_number])
            one_stock_list = [temp for temp in one_stock_list
                              if temp_stock.actual_weight != temp.actual_weight
                              or temp_stock.actual_number != temp.actual_number]
    return load_task_list, surplus_one_stock_list, other_stock_list


def compose_2_to_2(one_stock_list, other_stock_list, load_task_type=LoadTaskType.TYPE_7.value, across_district_flag=0):
    """
    拿one_stock_list去组合other_stock_list中的货物（两装两卸：如果传入的one_stock_list和other_stock_list是同区的则是同区两装两卸，否则是非同区两装两卸；是否跨区县两卸取决于across_district_flag）
    :param across_district_flag: 默认不跨区县across_district_flag=0；跨区县across_district_flag=1
    :param load_task_type:
    :param other_stock_list:
    :param one_stock_list:
    :return:
    """
    # 如果没有可以组合的货物
    if not one_stock_list or not other_stock_list:
        return [], one_stock_list, other_stock_list
    one_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    other_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
    # 组合的货物列表
    load_task_list: List[LoadTask] = []
    # one_stock_list中剩余的
    surplus_one_stock_list = []
    while one_stock_list:
        # 找出西区中待匹配的列表
        temp_stock = one_stock_list[0]
        # 当前other_stock_list中的仓库集合
        deliware_set: set = set(i.deliware_house for i in other_stock_list)
        # 组合列表
        target_compose_list = []
        for temp_deliware in deliware_set:
            if temp_deliware != temp_stock.deliware_house:
                # 筛选出待匹配的库存列表
                if across_district_flag:
                    # 如果跨区县卸货
                    filter_list = [stock for stock in other_stock_list
                                   if (stock.deliware_house == temp_stock.deliware_house
                                       or stock.deliware_house == temp_deliware)
                                   and (stock.big_commodity_name in
                                        ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
                                                                           [temp_stock.big_commodity_name]))]
                else:
                    filter_list = [stock for stock in other_stock_list
                                   if stock.dlv_spot_name_end == temp_stock.dlv_spot_name_end
                                   and (stock.deliware_house == temp_stock.deliware_house
                                        or stock.deliware_house == temp_deliware)
                                   and (stock.big_commodity_name in
                                        ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
                                                                           [temp_stock.big_commodity_name]))]
                min_weight, max_weight = get_weight(temp_stock)
                target_compose_list = get_optimal_group(filter_list, temp_stock, max_weight - temp_stock.actual_weight,
                                                        min_weight - temp_stock.actual_weight, 'consumer')
                # 如果配载成功
                if target_compose_list:
                    target_compose_list.append(temp_stock)
                    # 组合配载
                    load_task_list.append(
                        merge_result(create_load_task(target_compose_list, GenerateId.get_id(), load_task_type)))
                    # 如果组合成功，则结束当前循环
                    break
        # for一遍后组合成功
        if target_compose_list:
            # 删除first_stock_list中已经被组合的货物
            one_stock_list.remove(temp_stock)
            # 删除other_stock_list中已经被组合的货物
            other_stock_list = delete_the_stock_be_composed(other_stock_list, target_compose_list)
        # 未组合成功
        else:
            # surplus_one_stock_list.append(temp_stock)
            # one_stock_list.remove(temp_stock)
            surplus_one_stock_list.extend([temp for temp in one_stock_list
                                           if temp_stock.actual_weight == temp.actual_weight
                                           and temp_stock.dlv_spot_name_end == temp.dlv_spot_name_end
                                           and temp_stock.actual_number == temp.actual_number])
            one_stock_list = [temp for temp in one_stock_list
                              if temp_stock.actual_weight != temp.actual_weight
                              or temp_stock.dlv_spot_name_end != temp.dlv_spot_name_end
                              or temp_stock.actual_number != temp.actual_number]
    return load_task_list, surplus_one_stock_list, other_stock_list


def inner_compose(stock_list):
    """
    将当前库存中件数为1重量为24-31t的库存先和自己内部的货物组合：一装一卸，两装一卸(同区)，一装两卸，两装两卸(同区)
    :param stock_list:
    :return:
    """
    # 如果为空
    if not stock_list:
        return [], stock_list
    # # 组合的货物列表
    # load_task_list: List[LoadTask] = []
    # 24-31t并且件数为1的
    one_stock_list = []
    # 其他的
    other_stock_list = []
    for stock in stock_list:
        if (ModelConfig.RG_SECOND_MIN_WEIGHT <= stock.actual_weight < ModelConfig.RG_MIN_WEIGHT
                and stock.actual_number == 1):
            one_stock_list.append(stock)
        else:
            other_stock_list.append(stock)
    # 一装一卸，两装一卸，一装两卸，两装两卸
    load_task_list, one_stock_list, other_stock_list = compose_method(one_stock_list, other_stock_list)
    return load_task_list, one_stock_list + other_stock_list


def compose_method(one_stock_list, other_stock_list, flag=None):
    """
    一装一卸，两装一卸，一装两卸，两装两卸  (其中两装一卸、两装两卸是否同区，取决于one_stock_list与other_stock_list是否是同区的)
    :param flag: 默认为同区 flag=None；非同区flag=1；同区装 跨区县flag=2；非同区装 跨区县flag=3
    :param one_stock_list:
    :param other_stock_list:
    :return:
    """
    # 组合的货物列表
    load_task_list: List[LoadTask] = []
    # 如果没有可以组合的货物
    if not one_stock_list or not other_stock_list:
        return load_task_list, one_stock_list, other_stock_list
    # 组合方法列表
    compose_method_list = []
    # 组合方法对应的装卸类型列表
    load_task_type_list = []
    # 同区：一装一卸，两装一卸，一装两卸，两装两卸
    if not flag:
        compose_method_list = [compose_1_to_1, compose_2_to_1, compose_1_to_2, compose_2_to_2]
        load_task_type_list = [LoadTaskType.TYPE_1.value, LoadTaskType.TYPE_2.value, LoadTaskType.TYPE_4.value,
                               LoadTaskType.TYPE_6.value]
    # 非同区：两装一卸，两装两卸
    elif flag == 1:
        compose_method_list = [compose_2_to_1, compose_2_to_2]
        load_task_type_list = [LoadTaskType.TYPE_3.value, LoadTaskType.TYPE_7.value]
    # 跨区县：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)
    elif flag == 2:
        compose_method_list = [compose_1_to_2, compose_2_to_2]
        load_task_type_list = [LoadTaskType.TYPE_8.value, LoadTaskType.TYPE_9.value]
    # 跨区县：两装两卸(非同区仓库、跨区县)
    elif flag == 3:
        compose_method_list = [compose_2_to_2]
        load_task_type_list = [LoadTaskType.TYPE_10.value]
    for i in range(len(compose_method_list)):
        # 跨区县的标记：1跨区县，0不跨区县
        across_district_flag = 1 if flag == 2 or flag == 3 else 0
        # 调用上面的方法进行组合配载
        temp_load_task_list, one_stock_list, other_stock_list = compose_method_list[i](one_stock_list, other_stock_list,
                                                                                       load_task_type_list[i],
                                                                                       across_district_flag)
        load_task_list.extend(temp_load_task_list)
        # 如果没有可以组合的货物
        if not one_stock_list or not other_stock_list:
            return load_task_list, one_stock_list, other_stock_list
    return load_task_list, one_stock_list, other_stock_list
