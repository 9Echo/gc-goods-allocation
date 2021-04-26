# -*- coding: utf-8 -*-
# Description: 将同区县同客户的尾货组合配载
# Created: jjunf 2020/09/29
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
from model_config import ModelConfig


def tail_compose(stock_dict, mark=None):
    """
    将同区县同客户的尾货组合配载，一装一卸、两装一卸(同区)
    '''1.同仓库、同品种（一装一卸）'''
    '''2.同仓库、不同品种（一装一卸）'''
    '''3.不同仓库(同区)、同品种（两装一卸）'''
    '''4.不同仓库(同区)、不同品种（两装一卸）'''
    :param mark:
    :param stock_dict:
    :return:
    """
    # 组合成标载的货物列表
    load_task_list: List[LoadTask] = []
    # 组合后剩余的尾货列表
    surplus_stock_list = []
    # 依次将每个客户的货物组合
    for key in stock_dict.keys():
        # 拿到当前区县当前客户的尾货列表
        temp_stock_list = stock_dict[key]
        # 按重量降序排序，保证不让4*8t的卷先自己配走，而是优先考虑与31t以下的卷配载
        temp_stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
        # '''2.同仓库（一装一卸）'''  # （此处的consumer是为了方法的共用，必须传两个属性参数）
        if temp_stock_list:
            temp_stock_list = compose_1_to_1(temp_stock_list, load_task_list, mark)
        '''4.不同仓库(同区两装一卸）'''
        if temp_stock_list:
            temp_stock_list = compose_2_to_1(temp_stock_list, load_task_list, mark)
            # surplus_list = []
            # while temp_stock_list:
            #     temp_stock = temp_stock_list[0]
            #     filter_list = [stock for stock in temp_stock_list if stock is not temp_stock
            #                    and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
            #         temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
            #     min_weight, max_weight = get_weight(temp_stock, mark)
            #     target_compose_list = get_optimal_group(filter_list, temp_stock,
            #                                             max_weight - temp_stock.actual_weight,
            #                                             min_weight - temp_stock.actual_weight, 'deliware_house')
            #     # 配载成功
            #     if target_compose_list:
            #         target_compose_list.append(temp_stock)
            #         # 组合配载
            #         load_task_list.append(
            #             merge_result(
            #                 create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_2.value)))
            #         # 删除库存中已经被组合的货物
            #         temp_stock_list = delete_the_stock_be_composed(temp_stock_list, target_compose_list)
            #     # 如果配载不成功
            #     else:
            #         # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
            #         if temp_stock.actual_number > 1:
            #             temp_stock_split = split([temp_stock])
            #             target_compose_list = get_optimal_group(temp_stock_split + filter_list, temp_stock,
            #                                                     max_weight, min_weight, 'deliware_house')
            #             # 组合成功，满足重量下限要求
            #             if target_compose_list:
            #                 load_task_list.append(
            #                     merge_result(
            #                         create_load_task(target_compose_list, GenerateId.get_id(),
            #                                          LoadTaskType.TYPE_2.value)))
            #                 temp_stock_list = delete_the_stock_be_composed(temp_stock_list, target_compose_list)
            #             # 配载不成功
            #             else:
            #                 surplus_list.extend(([temp for temp in temp_stock_list
            #                                       if temp_stock.actual_weight == temp.actual_weight
            #                                       and temp_stock.actual_number == temp.actual_number]))
            #                 temp_stock_list = ([temp for temp in temp_stock_list
            #                                     if temp_stock.actual_weight != temp.actual_weight
            #                                     or temp_stock.actual_number != temp.actual_number])
            #         # 如果temp_stock整体配载不成功，拆分也配载不成功
            #         else:
            #             surplus_list.extend(([temp for temp in temp_stock_list
            #                                   if temp_stock.actual_weight == temp.actual_weight
            #                                   and temp_stock.actual_number == temp.actual_number]))
            #             temp_stock_list = ([temp for temp in temp_stock_list
            #                                 if temp_stock.actual_weight != temp.actual_weight
            #                                 or temp_stock.actual_number != temp.actual_number])
            # temp_stock_list = surplus_list
        # 该客户最后剩余的货物
        surplus_stock_list.extend(temp_stock_list)
    return load_task_list, surplus_stock_list


def compose_1_to_1(temp_stock_list, load_task_list, mark=None):
    """
    货物组合（一装一卸）
    :param mark:
    :param temp_stock_list:
    :param load_task_list:
    :return:
    """
    surplus_list = []  # temp_stock_list中剩余的
    # 将同区县、同客户的货物，按照仓库、品种分组
    stock_dict_of_key = pick_split_group_rule.split_group(temp_stock_list, 'deliware_house')
    # 将货物组合
    for k in stock_dict_of_key.keys():
        stock_list = stock_dict_of_key[k]
        # 按重量降序排序
        stock_list.sort(key=lambda x: x.actual_weight, reverse=True)
        temp_surplus_list = []  # stock_list中剩余的
        while stock_list:
            temp_stock = stock_list[0]
            filter_list = [stock for stock in stock_list if stock is not temp_stock
                           and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
                temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
            # # 如果卷重小于24或者大于29，则不拼线材
            # if temp_stock.big_commodity_name == '老区-卷板' and (
            #         temp_stock.actual_weight >= ModelConfig.RG_J_MIN_WEIGHT or
            #         temp_stock.actual_weight < ModelConfig.RG_SECOND_MIN_WEIGHT):
            #     filter_list = [stock_j for stock_j in filter_list if stock_j.big_commodity_name == '老区-卷板']
            min_weight, max_weight = get_weight(temp_stock, mark)
            load_task, target_compose_list = goods_compose(filter_list, temp_stock,
                                                           max_weight - temp_stock.actual_weight, min_weight,
                                                           LoadTaskType.TYPE_1.value)
            # 配载成功
            if load_task:
                load_task_list.append(load_task)
                # 删除库存中已经被组合的货物
                stock_list = delete_the_stock_be_composed(stock_list, target_compose_list)
            # 如果配载不成功
            else:
                # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
                if temp_stock.actual_number > 1:
                    temp_stock_split = split([temp_stock])
                    target_compose_list, temp_max_weight = goods_filter(temp_stock_split + filter_list,
                                                                        max_weight)
                    # 组合成功，满足重量下限要求
                    if temp_max_weight >= min_weight:
                        load_task_list.append(
                            merge_result(
                                create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_1.value)))
                        stock_list = delete_the_stock_be_composed(stock_list, target_compose_list)
                    else:
                        temp_surplus_list.extend(([temp for temp in stock_list
                                                   if temp_stock.actual_weight == temp.actual_weight
                                                   and temp_stock.actual_number == temp.actual_number]))
                        stock_list = ([temp for temp in stock_list
                                       if temp_stock.actual_weight != temp.actual_weight
                                       or temp_stock.actual_number != temp.actual_number])
                # 如果temp_stock整体配载不成功，拆分也配载不成功
                else:
                    temp_surplus_list.extend(([temp for temp in stock_list
                                               if temp_stock.actual_weight == temp.actual_weight
                                               and temp_stock.actual_number == temp.actual_number]))
                    stock_list = ([temp for temp in stock_list
                                   if temp_stock.actual_weight != temp.actual_weight
                                   or temp_stock.actual_number != temp.actual_number])
        surplus_list.extend(temp_surplus_list)
    return surplus_list


def goods_compose(filter_list, temp_stock, surplus_weight, new_min_weight, load_task_type):
    """
    拿temp_stock与filter_list中的货物进行组合，返回组合配载的车次、被组合的货物列表
    :param temp_stock:
    :param filter_list:
    :param load_task_type:
    :param surplus_weight:
    :param new_min_weight:
    :return:
    """
    if filter_list:
        filter_list.sort(key=lambda x: x.actual_weight, reverse=True)
        temp_filter_list = filter_list
        # 目标拼货组合重量
        temp_max_weight: int = 0
        # 目标拼货组合库存列表
        target_compose_list: List[Stock] = list()
        if temp_stock.big_commodity_name == '老区-型钢':  # 型钢最多只能拼两个规格（包括自身的规格）
            temp_set: set = set([item.specs for item in temp_filter_list])
            for j in temp_set:
                # 筛选出满足规格要求的货物
                temp_list = [v for v in temp_filter_list if v.specs == j or v.specs == temp_stock.specs]
                if temp_list:
                    result_list = split(temp_list)
                    # 选中的列表
                    compose_list, compose_weight = goods_filter(result_list, surplus_weight)
                    if compose_weight >= temp_max_weight:
                        temp_max_weight = compose_weight
                        target_compose_list = compose_list
        else:
            temp_list = split(temp_filter_list)
            # 选中的列表
            target_compose_list, temp_max_weight = goods_filter(temp_list, surplus_weight)
        if temp_max_weight >= new_min_weight - temp_stock.actual_weight:  # 满足重量下限要求
            target_compose_list.append(temp_stock)
            # 返回组合配载的车次、被组合的货物列表
            return merge_result(
                create_load_task(target_compose_list, GenerateId.get_id(), load_task_type)), target_compose_list
    # 一单在达标重量之上并且无货可拼的情况
    if temp_stock.actual_weight >= new_min_weight:
        return create_load_task([temp_stock], GenerateId.get_id(), load_task_type), [temp_stock]
    # 卷类1件，并且重量大于24t可发走
    # if (temp_stock.big_commodity_name in ModelConfig.RG_J_GROUP and temp_stock.actual_number == 1
    #         and temp_stock.actual_weight >= ModelConfig.RG_SECOND_MIN_WEIGHT):
    #     return create_load_task([temp_stock], TrainId.get_id(), load_task_type), [temp_stock]
    else:
        return None, []


def compose_2_to_1(temp_stock_list, load_task_list, mark=None):
    """
    货物组合（同区两装一卸）
    :param temp_stock_list:
    :param load_task_list:
    :param mark:
    :return:
    """
    surplus_list = []
    while temp_stock_list:
        temp_stock = temp_stock_list[0]
        filter_list = [stock for stock in temp_stock_list if stock is not temp_stock
                       and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
            temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
        min_weight, max_weight = get_weight(temp_stock, mark)
        target_compose_list = get_optimal_group(filter_list, temp_stock, max_weight - temp_stock.actual_weight,
                                                min_weight - temp_stock.actual_weight, 'deliware_house')
        # 配载成功
        if target_compose_list:
            target_compose_list.append(temp_stock)
            # 组合配载
            load_task_list.append(
                merge_result(
                    create_load_task(target_compose_list, GenerateId.get_id(), LoadTaskType.TYPE_2.value)))
            # 删除库存中已经被组合的货物
            temp_stock_list = delete_the_stock_be_composed(temp_stock_list, target_compose_list)
        # 如果配载不成功
        else:
            # 如果配载不成功，且temp_stock件数大于1，则将其拆分为单件去组合
            if temp_stock.actual_number > 1:
                temp_stock_split = split([temp_stock])
                target_compose_list = get_optimal_group(temp_stock_split + filter_list, temp_stock,
                                                        max_weight, min_weight, 'deliware_house')
                # 组合成功，满足重量下限要求
                if target_compose_list:
                    load_task_list.append(
                        merge_result(
                            create_load_task(target_compose_list, GenerateId.get_id(),
                                             LoadTaskType.TYPE_2.value)))
                    temp_stock_list = delete_the_stock_be_composed(temp_stock_list, target_compose_list)
                # 配载不成功
                else:
                    surplus_list.extend(([temp for temp in temp_stock_list
                                          if temp_stock.actual_weight == temp.actual_weight
                                          and temp_stock.actual_number == temp.actual_number]))
                    temp_stock_list = ([temp for temp in temp_stock_list
                                        if temp_stock.actual_weight != temp.actual_weight
                                        or temp_stock.actual_number != temp.actual_number])
            # 如果temp_stock整体配载不成功，拆分也配载不成功
            else:
                surplus_list.extend(([temp for temp in temp_stock_list
                                      if temp_stock.actual_weight == temp.actual_weight
                                      and temp_stock.actual_number == temp.actual_number]))
                temp_stock_list = ([temp for temp in temp_stock_list
                                    if temp_stock.actual_weight != temp.actual_weight
                                    or temp_stock.actual_number != temp.actual_number])
    return surplus_list
