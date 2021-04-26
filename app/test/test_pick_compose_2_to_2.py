# -*- coding: utf-8 -*-
# Description: 两装两卸
# Created: luchengkai 2020/11/05
from typing import List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.main.steel_factory.rule.pick_compose_public_method import get_optimal_group, merge_result, get_weight
from app.main.steel_factory.rule.pick_compose_public_method import delete_the_stock_be_composed
from app.main.steel_factory.rule.goods_filter_rule import goods_filter
from app.main.steel_factory.rule.split_rule import split
from app.main.steel_factory.rule import pick_split_group_rule
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig


def same_deliware_compose(stock_list: List[Stock]):
    """
    两装(同区仓库)两卸
    :param stock_list:
    :return:
    """
    # 组合成标载的货物列表
    load_task_list: List[LoadTask] = []
    # 当前的仓库集合
    deliware_set: set = set(i.deliware_house for i in stock_list)
    # stock_list中组合后剩余的尾货列表
    surplus_stock_list = []
    while stock_list:
        temp_stock = stock_list[0]
        # 组合列表
        target_compose_list = []
        for temp_set in deliware_set:
            if temp_set != temp_stock.deliware_house:
                # 筛选出两装的待匹配的库存列表
                filter_list = [stock for stock in stock_list if stock is not temp_stock
                               and stock.dlv_spot_name_end == temp_stock.dlv_spot_name_end
                               and (stock.deliware_house == temp_stock.deliware_house or stock.deliware_house == temp_set)
                               and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
                    temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
                new_min_weight, surplus_weight = get_weight(temp_stock)
                target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight,
                                                        new_min_weight - temp_stock.actual_weight, 'consumer')
                # 如果配载成功
                if target_compose_list:
                    target_compose_list.append(temp_stock)
                    # 组合配载
                    load_task_list.append(
                        merge_result(
                            create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_6.value)))
                    # 如果组合成功，则结束当前循环
                    break
                # 如果配载不成功
                else:
                    # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
                    if temp_stock.actual_number > 1:
                        temp_stock_split = split([temp_stock])
                        target_compose_list = get_optimal_group(temp_stock_split + filter_list, temp_stock,
                                                                ModelConfig.RG_MAX_WEIGHT +
                                                                ModelConfig.RG_SINGLE_UP_WEIGHT,
                                                                new_min_weight, 'consumer')
                        # 组合成功，满足重量下限要求
                        if target_compose_list:
                            load_task_list.append(
                                merge_result(
                                    create_load_task(target_compose_list, GenerateId.get_id(),
                                                     LoadTaskType.TYPE_6.value)))
                            # 如果组合成功，则结束当前循环
                            break
        # for一遍后组合成功
        if target_compose_list:
            # 删除库存中已经被组合的货物
            stock_list = delete_the_stock_be_composed(stock_list, target_compose_list)
        # 未组合成功
        else:
            surplus_stock_list.append(temp_stock)
            stock_list.remove(temp_stock)
    return load_task_list, surplus_stock_list


def not_same_deliware_compose(west_stock_list: List[Stock], old_stock_list: List[Stock], lbg_stock_list: List[Stock]):
    """
    两装(非同区仓库)两卸
    :param west_stock_list:
    :param lbg_stock_list:
    :param old_stock_list:
    :return:
    """
    # 组合成标载的货物列表
    load_task_list: List[LoadTask] = []
    # stock_list中组合后剩余的尾货列表
    surplus_stock_old_list = []
    surplus_stock_west_list = []
    surplus_stock_lbg_list = []
    # 当前的仓库集合
    deliware_set: set = set(i.deliware_house for i in west_stock_list)
    while west_stock_list:
        temp_stock = west_stock_list[0]
        # 组合列表
        target_compose_list = []
        for temp_set in deliware_set:
            if temp_set != temp_stock.deliware_house:
                # 筛选出两装的待匹配的库存列表
                filter_list = [stock for stock in old_stock_list if stock is not temp_stock and (
                        stock.deliware_house == temp_stock.deliware_house or stock.deliware_house == temp_set)
                               and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
                    temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
                filter_list.extend(stock for stock in lbg_stock_list if stock is not temp_stock and (
                        stock.deliware_house == temp_stock.deliware_house or stock.deliware_house == temp_set)
                                   and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
                    temp_stock.big_commodity_name, [temp_stock.big_commodity_name]))
                new_min_weight, surplus_weight = get_weight(temp_stock)
                target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight,
                                                        new_min_weight - temp_stock.actual_weight, 'consumer')
                # 如果配载成功
                if target_compose_list:
                    target_compose_list.append(temp_stock)
                    # 组合配载
                    load_task_list.append(
                        merge_result(
                            create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_7.value)))
                    # 如果组合成功，则结束当前循环
                    break
                # 如果配载不成功
                else:
                    # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
                    if temp_stock.actual_number > 1:
                        temp_stock_split = split([temp_stock])
                        target_compose_list = get_optimal_group(temp_stock_split + filter_list, temp_stock,
                                                                ModelConfig.RG_MAX_WEIGHT +
                                                                ModelConfig.RG_SINGLE_UP_WEIGHT,
                                                                new_min_weight, 'consumer')
                        # 组合成功，满足重量下限要求
                        if target_compose_list:
                            load_task_list.append(
                                merge_result(
                                    create_load_task(target_compose_list, GenerateId.get_id(),
                                                     LoadTaskType.TYPE_7.value)))
                            # 如果组合成功，则结束当前循环
                            break
        # for一遍后组合成功
        if target_compose_list:
            west_stock_list = delete_the_stock_be_composed(west_stock_list, target_compose_list)
            old_stock_list = delete_the_stock_be_composed(old_stock_list, target_compose_list)
            lbg_stock_list = delete_the_stock_be_composed(lbg_stock_list, target_compose_list)
        # 未组合成功
        else:
            surplus_stock_west_list.append(temp_stock)
            west_stock_list.remove(temp_stock)
    # 当前的仓库集合
    deliware_set: set = set(i.deliware_house for i in old_stock_list)
    while old_stock_list:
        temp_stock = old_stock_list[0]
        # 组合列表
        target_compose_list = []
        for temp_set in deliware_set:
            if temp_set != temp_stock.deliware_house:
                # 筛选出两装的待匹配的库存列表
                filter_list = [stock for stock in lbg_stock_list if stock is not temp_stock and (
                        stock.deliware_house == temp_stock.deliware_house or stock.deliware_house == temp_set)
                               and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
                    temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
                new_min_weight, surplus_weight = get_weight(temp_stock)
                target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight,
                                                        new_min_weight - temp_stock.actual_weight, 'consumer')
                # 如果配载成功
                if target_compose_list:
                    target_compose_list.append(temp_stock)
                    # 组合配载
                    load_task_list.append(
                        merge_result(
                            create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_7.value)))
                    # 如果组合成功，则结束当前循环
                    break
                # 如果配载不成功
                else:
                    # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
                    if temp_stock.actual_number > 1:
                        temp_stock_split = split([temp_stock])
                        target_compose_list = get_optimal_group(temp_stock_split + filter_list, temp_stock,
                                                                ModelConfig.RG_MAX_WEIGHT +
                                                                ModelConfig.RG_SINGLE_UP_WEIGHT,
                                                                new_min_weight, 'consumer')
                        # 组合成功，满足重量下限要求
                        if target_compose_list:
                            load_task_list.append(
                                merge_result(
                                    create_load_task(target_compose_list, GenerateId.get_id(),
                                                     LoadTaskType.TYPE_7.value)))
                            # 如果组合成功，则结束当前循环
                            break
        # for一遍后组合成功
        if target_compose_list:
            old_stock_list = delete_the_stock_be_composed(old_stock_list, target_compose_list)
            lbg_stock_list = delete_the_stock_be_composed(lbg_stock_list, target_compose_list)
        # 未组合成功
        else:
            surplus_stock_old_list.append(temp_stock)
            old_stock_list.remove(temp_stock)
    if lbg_stock_list:
        surplus_stock_lbg_list.extend(lbg_stock_list)
    return load_task_list, surplus_stock_old_list, surplus_stock_west_list, surplus_stock_lbg_list


if __name__ == '__main__':
    print(0)

