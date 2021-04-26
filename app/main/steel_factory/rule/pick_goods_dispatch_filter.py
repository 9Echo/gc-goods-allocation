# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/9/29 9:13
from typing import List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.rule import pick_group_by_huge_and_tail_rule, pick_split_group_rule, pick_compose
from app.main.steel_factory.rule import last_j_handler_rule, pick_compose_2_to_2
from app.main.steel_factory.rule import pick_compose_in_same_district_and_deliware_house
from app.main.steel_factory.rule import pick_compose_in_same_district_and_consumer
from app.main.steel_factory.rule.pick_compose_across_district import across_district_compose_in_one_factory
from app.main.steel_factory.rule.pick_compose_across_district import across_district_compose_in_two_factory
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId


def dispatch_filter(west_stock_list, old_stock_list, lbg_stock_list, west_j_list, old_j_list, lbg_j_list,
                    other_warehouse_stock_list):
    """
    分货规则
    :param other_warehouse_stock_list:
    :param lbg_j_list:
    :param lbg_stock_list:
    :param old_j_list: 老区的"老区-卷板"
    :param west_j_list: 西区的'新产品-卷板', '新产品-白卷'
    :param west_stock_list: 西区库存列表
    :param old_stock_list: 老区库存列表
    :return:
    """
    """
    1.库存预处理
    2.将老区的黑卷、西区的白卷和黑卷单独拿出来，剩余的库存分为西区、老区两个部分，并且切分好
      先将老区的黑卷来配西区的白卷和黑卷，使得老区的黑卷尽量能够配成标载，再将剩下的放入西区和老区的库存中去配载
    3.将库存分为标载和尾货，标载直接生成车次
    4.将每个区的尾货库存，按区县、客户分组，同区县同客户的按出库仓库、品种再分类：
        ①.同仓库、同品种（一装一卸）
        ②.同仓库、不同品种（一装一卸）
        ③.不同仓库(同区)、同品种（两装一卸）
        ④.不同仓库(同区)、不同品种（两装一卸）
        按照上面的先后次序生成车次，并返回剩余的尾货列表
    5.将同区县、同仓库的剩余尾货组合配载，同区县不同客户之间拼货（一装两卸）
        ①.先匹配同品种的货物
        ②.再匹配不同品种的货物
        按照上面的先后次序生成车次，并返回最后剩余的尾货列表
    6.按流向、品种、件数、重量等要求合并单车货物生成摘单计划
    """
    # 返回配载好的货物列表
    load_task_list: List[LoadTask] = []
    '''1.老区的卷类先自己拼：一装一卸，两装一卸(同区)，一装两卸，两装两卸(同区)'''
    old_j_list = stock_compose(old_j_list, load_task_list, 1)
    '''2.西区、岚北港的卷类 件重24-31t的先内部组合：一装一卸，两装一卸(同区)，一装两卸，两装两卸(同区)'''
    # 西区
    temp_load_task_list, west_j_list = pick_compose.inner_compose(west_j_list)
    load_task_list.extend(temp_load_task_list)
    # 岚北港
    temp_load_task_list, lbg_j_list = pick_compose.inner_compose(lbg_j_list)
    load_task_list.extend(temp_load_task_list)
    '''3.老区配西区、岚北：两装一卸(非同区)、两装两卸(非同区)'''
    # 老区配西区
    temp_load_task_list, old_j_list, west_j_list = pick_compose.compose_method(old_j_list, west_j_list, 1)
    load_task_list.extend(temp_load_task_list)
    # 老区配岚北港
    temp_load_task_list, old_j_list, lbg_j_list = pick_compose.compose_method(old_j_list, lbg_j_list, 1)
    load_task_list.extend(temp_load_task_list)
    """4.西区、岚北港的卷类内部自己组合：一装一卸，两装一卸(同区)，一装两卸，两装两卸(同区)"""
    # 西区
    west_j_list = stock_compose(west_j_list, load_task_list)
    # 岚北港
    lbg_j_list = stock_compose(lbg_j_list, load_task_list)
    """5.将卷类与非卷类组合在一起走正常流程拼货：一装一卸，两装一卸(同区)，一装两卸，两装两卸(同区)"""
    # 西区
    west_stock_list = stock_compose(west_stock_list + west_j_list, load_task_list)
    # 老区
    old_stock_list = stock_compose(old_stock_list + old_j_list, load_task_list)
    # 岚北港
    lbg_stock_list = stock_compose(lbg_stock_list + lbg_j_list, load_task_list)
    '''6.跨区县拼货（卷类与非卷类组合在一起）：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)、两装两卸(非同区仓库、跨区县)'''
    # 西区：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)
    west_stock_list = across_district_compose_in_one_factory(west_stock_list, load_task_list)
    # 老区：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)
    old_stock_list = across_district_compose_in_one_factory(old_stock_list, load_task_list)
    # 岚北港：一装两卸(跨区县)、两装两卸(同区仓库、跨区县)
    lbg_stock_list = across_district_compose_in_one_factory(lbg_stock_list, load_task_list)
    # 老区配西区：两装两卸(非同区仓库、跨区县)
    old_stock_list, west_stock_list = across_district_compose_in_two_factory(old_stock_list, west_stock_list,
                                                                             load_task_list)
    # 西区配老区：两装两卸(非同区仓库、跨区县)
    west_stock_list, old_stock_list = across_district_compose_in_two_factory(west_stock_list, old_stock_list,
                                                                             load_task_list)
    '''7.不在当前仓库范围内的货物，则仓库内部自己组合'''
    # other_warehouse_stock_list组合后剩余的货物列表
    surplus_other_warehouse_stock_list = []
    if other_warehouse_stock_list:
        # 仓库列表
        warehouse_set = set([stock.deliware_house for stock in other_warehouse_stock_list])
        for warehouse in warehouse_set:
            # 得到当前仓库的货物列表
            stock_list = [stock for stock in other_warehouse_stock_list if stock.deliware_house == warehouse]
            # 当前仓库的货物自己拼：一装一卸，一装两卸
            stock_list = stock_compose(stock_list, load_task_list)
            surplus_other_warehouse_stock_list.extend(stock_list)
    '''8.处理24吨及以上的卷'''
    surplus_stock_list = (
        last_j_handler_rule.j_handler(
            west_stock_list + old_stock_list + lbg_stock_list + surplus_other_warehouse_stock_list, load_task_list))
    return load_task_list, surplus_stock_list


def stock_compose(stock_list, load_task_list, mark=None):
    """
    货物处理流程
    :param mark:
    :param stock_list:
    :param load_task_list:
    :return:
    """
    '''1.将库存分为标载huge_stock_list和尾货tail_stock_list'''
    huge_stock_list, tail_stock_list = pick_group_by_huge_and_tail_rule.stock_filter(stock_list)
    # 保存配载列表
    if huge_stock_list:
        for stock in huge_stock_list:
            load_task = create_load_task([stock], GenerateId.get_id(), LoadTaskType.TYPE_1.value)
            load_task_list.append(load_task)
    '''2.将尾货按照区县客户分组，结果为字典：{‘区县+客户’：[库存列表]}'''
    tail_stock_dict = pick_split_group_rule.split_group(tail_stock_list, 'dlv_spot_name_end', 'consumer')
    '''3.将同区县同客户的尾货组合配载，得到配载列表、无法配载的尾货列表'''  # 一装一卸、两装一卸(同区)
    temp_load_task_list, surplus_stock_list = pick_compose_in_same_district_and_consumer.tail_compose(
        tail_stock_dict, mark)
    load_task_list.extend(temp_load_task_list)
    """4.将剩余的尾货按照区县仓库分组，结果为字典：{‘区县+仓库’：[库存列表]}"""
    surplus_stock_dict = pick_split_group_rule.split_group(surplus_stock_list, 'dlv_spot_name_end', 'deliware_house')
    '''5.将同区县同仓库的尾货组合配载'''  # 一装两卸
    temp_load_task_list, last_surplus_stock_list = pick_compose_in_same_district_and_deliware_house.surplus_compose(
        surplus_stock_dict, mark)
    load_task_list.extend(temp_load_task_list)
    '''6.两装两卸(同区)'''  # 两装两卸(同区)
    temp_load_task_list, last_surplus_stock_list = pick_compose_2_to_2.same_deliware_compose(last_surplus_stock_list,
                                                                                             mark)
    load_task_list.extend(temp_load_task_list)
    # 返回最后剩余的尾货列表
    return last_surplus_stock_list
