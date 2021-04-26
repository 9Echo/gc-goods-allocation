# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/8/2 15:35
import copy
from typing import List

from flask import g

from app.main.pipe_factory.dao.match_dao import match_dao
from app.main.pipe_factory.entity.delivery_item import DeliveryItem
from app.main.pipe_factory.entity.delivery_sheet import DeliverySheet
from app.util import weight_calculator
from model_config import ModelConfig


def match_solve(order, delivery_item_list: List[DeliveryItem]):
    """

    :param order:
    :param delivery_item_list:
    :return:
    """
    # 提货单列表
    sheets = []
    # 大管列表
    max_delivery_items: List[DeliveryItem] = []
    # 小管列表
    min_delivery_items: List[DeliveryItem] = []
    # 大小管区分
    for delivery_item in delivery_item_list:
        if delivery_item.volume == ModelConfig.DEFAULT_VOLUME:
            min_delivery_items.append(delivery_item)
        else:
            max_delivery_items.append(delivery_item)
    # 如果没有大管，return
    if not max_delivery_items:
        return sheets
    # 查询大小管搭配数据
    match_data = match_dao.select_match_data(order, max_delivery_items, min_delivery_items)
    match_dict = dict()
    # 搭配结果按大管item_id分组
    for i in match_data:
        match_dict.setdefault(i.get('main_item_id'), []).append(i)

    # 遍历大管列表，寻找组合
    for max_item in copy.copy(max_delivery_items):
        # 历史此大管所搭配的组合数据
        temp_match_data = match_dict.get(max_item.item_id, [])
        # 按照次数多到少的顺序依次匹配
        for match_item in temp_match_data:
            # 找出被匹配的小管
            temp_min_item_list = [i for i in min_delivery_items if i.item_id == match_item.get('sub_item_id')]
            # 如果被匹配的小管还在
            if temp_min_item_list:
                min_item = temp_min_item_list[0]
                # 搭配件数
                max_item_quantity = match_item.get('main_quantity')
                min_item_quantity = match_item.get('sub_quantity')
                # 如果当前大小管件数满足搭配方案
                if max_item.quantity >= max_item_quantity and min_item.quantity >= min_item_quantity:
                    max_delivery_count = int(max_item.quantity / max_item_quantity)
                    min_delivery_count = int(min_item.quantity / min_item_quantity)
                    # 组合数
                    delivery_count = min(max_delivery_count, min_delivery_count)
                    if delivery_count > 0:
                        # 大小管扣减件数
                        max_item.quantity -= int(delivery_count * max_item_quantity)
                        min_item.quantity -= int(delivery_count * min_item_quantity)
                        # 如果大管扣除后件数为0并且散根也为0，则删除明细
                        if max_item.quantity == 0 and max_item.free_pcs == 0:
                            delivery_item_list.remove(max_item)
                            max_delivery_items.remove(max_item)
                        else:
                            # 大管重新计算参数
                            calculate(max_item)
                        # 如果小管扣除后件数为0并且散根也为0，则删除明细
                        if min_item.quantity == 0 and max_item.free_pcs == 0:
                            delivery_item_list.remove(min_item)
                            min_delivery_items.remove(min_item)
                        else:
                            # 小管重新计算参数
                            calculate(min_item)
                        # 批量生成提货单
                        for _ in range(delivery_count):
                            g.LOAD_TASK_ID += 1
                            main_sheet = DeliverySheet()
                            main_sheet.load_task_id = g.LOAD_TASK_ID
                            copy_max_item = copy.deepcopy(max_item)
                            calculate_for_copy_item(copy_max_item, max_item_quantity)
                            main_sheet.items = [copy_max_item]
                            sheets.append(main_sheet)
                            sub_sheet = DeliverySheet()
                            sub_sheet.load_task_id = g.LOAD_TASK_ID
                            copy_min_item = copy.deepcopy(min_item)
                            calculate_for_copy_item(copy_min_item, min_item_quantity)
                            sub_sheet.items = [copy_min_item]
                            sheets.append(sub_sheet)
                else:
                    continue
        # 大管单独生成提货单
        temp_min_item_list = [i for i in temp_match_data if not i.get('sub_item_id')]
        if temp_min_item_list:
            main_quantity = temp_min_item_list[0].get('main_quantity')
            if max_item.quantity >= main_quantity:
                max_delivery_count = int(max_item.quantity / main_quantity)
                if max_delivery_count > 0:
                    # 生成提货单，扣减件数
                    max_item.quantity -= int(max_delivery_count * main_quantity)
                    if max_item.quantity == 0 and max_item.free_pcs == 0:
                        delivery_item_list.remove(max_item)
                        max_delivery_items.remove(max_item)
                    else:
                        calculate(max_item)
                    for _ in range(max_delivery_count):
                        g.LOAD_TASK_ID += 1
                        main_sheet = DeliverySheet()
                        main_sheet.load_task_id = g.LOAD_TASK_ID
                        copy_max_item = copy.deepcopy(max_item)
                        calculate_for_copy_item(copy_max_item, main_quantity)
                        main_sheet.items = [copy_max_item]
                        sheets.append(main_sheet)

    return sheets


def calculate(item):
    """
    计算参数
    :param item:
    :return:
    """
    item.volume = item.quantity / item.max_quantity \
        if item.max_quantity else ModelConfig.DEFAULT_VOLUME
    item.weight = weight_calculator.calculate_weight(item.product_type, item.item_id,
                                                     item.quantity, item.free_pcs)
    item.total_pcs = weight_calculator.calculate_pcs(item.product_type, item.item_id,
                                                     item.quantity, item.free_pcs)


def calculate_for_copy_item(item, quantity):
    """
    生成的提货单参数计算
    :param item:
    :param quantity:
    :return:
    """
    item.quantity = quantity
    item.free_pcs = 0
    item.weight = weight_calculator.calculate_weight(item.product_type,
                                                     item.item_id,
                                                     item.quantity,
                                                     item.free_pcs)
    item.total_pcs = weight_calculator.calculate_pcs(item.product_type,
                                                     item.item_id,
                                                     item.quantity,
                                                     item.free_pcs)
