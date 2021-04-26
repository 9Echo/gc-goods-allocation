import copy

import pandas as pd
from flask import g
from app.main.pipe_factory.rule import weight_rule, package_solution
from app.main.pipe_factory.entity.delivery_sheet import DeliverySheet
from app.main.pipe_factory.service.dispatch_load_task_service import dispatch_load_task
from app.main.pipe_factory.service.replenish_property_service import replenish_property
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from app.util.uuid_util import UUIDUtil

from model_config import ModelConfig


def optimize_filter_max(delivery_items: list):
    """
    根据过滤规则将传入的发货子单划分到合适的发货单中
    """
    # 返回的提货单列表
    sheets = []
    # 提货单明细列表
    item_list = []
    # 剩余的发货子单
    left_items = delivery_items
    for i in copy.copy(delivery_items):
        if i.weight <= g.MAX_WEIGHT:
            item_list.append(i)
            left_items.remove(i)
    if left_items:
        left_items.sort(key=lambda x: x.weight, reverse=True)
    # 如果有超重的子单，进行compose
    while left_items:
        # 每次取第一个元素进行compose,  filtered_items是得到的一个饱和(饱和即已达到重量上限)的子单
        filtered_item, left_items = weight_rule.compose_split(left_items[0], left_items)
        if filtered_item.weight == 0:
            raise MyException('分单异常！', ResponseCode.Error)
        item_list.append(filtered_item)

    while item_list:
        # 是否满载标记
        is_full = False
        weight_cost = []
        for item in item_list:
            weight_cost.append((int(item.weight), float(item.volume), int(item.weight)))
        final_weight, result_list = package_solution.dynamic_programming(len(item_list), g.MAX_WEIGHT,
                                                                         ModelConfig.MAX_VOLUME, weight_cost)
        if final_weight == 0:
            break
        if (g.MAX_WEIGHT - ModelConfig.PACKAGE_LOWER_WEIGHT) <= final_weight <= g.MAX_WEIGHT:
            is_full = True
            g.LOAD_TASK_ID += 1
        # 临时明细存放
        temp_item_list = []
        for i in range(len(result_list)):
            if result_list[i] == 1:
                sheet = DeliverySheet()
                # 取出明细列表对应下标的明细
                sheet.items = [item_list[i]]
                # 设置提货单总体积占比
                sheet.volume = item_list[i].volume
                temp_item_list.append(item_list[i])
                if is_full:
                    sheet.load_task_id = g.LOAD_TASK_ID
                sheets.append(sheet)
        # 整体移除被开走的明细
        for i in temp_item_list:
            item_list.remove(i)

    return sheets


def optimize_filter_min(sheets, min_delivery_item, order, batch_no):
    """
    小管装填大管车次，将小管按照件数从小到大排序
    若小管不够，分完结束
    若小管装填完所有车次后有剩余，进行背包和遍历
    :param batch_no: 分货批次号
    :param order: 订单
    :param sheets:大管的提货单列表
    :param min_delivery_item:小管的子项列表
    :return: None
    """
    if not sheets:
        # 2、使用模型过滤器生成发货通知单
        min_sheets = optimize_filter_max(min_delivery_item)
        # 3、补充发货单的属性
        replenish_property(min_sheets, order, batch_no, '20')
        # 为发货单分配车次
        dispatch_load_task(min_sheets)
        sheets.extend(min_sheets)
    else:
        min_delivery_item.sort(key=lambda x: x.quantity)
        sheets_dict = [sheet.as_dict() for sheet in sheets]
        df = pd.DataFrame(sheets_dict)
        series = df.groupby(by=['load_task_id'])['weight'].sum().sort_index()
        for k, v in series.items():
            current_weight = v
            if v >= g.MAX_WEIGHT:
                continue
            for i in copy.copy(min_delivery_item):
                # 如果该子项可完整放入
                if i.weight <= g.MAX_WEIGHT - current_weight:
                    # 当前重量累加
                    current_weight += i.weight
                    # 生成新提货单，归到该车次下
                    new_sheet = DeliverySheet()
                    new_sheet.load_task_id = k
                    new_sheet.request_id = order.request_id
                    new_sheet.salesorg_id = order.salesorg_id
                    new_sheet.type = '20'
                    new_sheet.volume = i.volume
                    new_sheet.batch_no = batch_no
                    new_sheet.customer_id = order.customer_id
                    new_sheet.salesman_id = order.salesman_id
                    new_sheet.weight = i.weight
                    new_sheet.total_pcs = i.total_pcs
                    new_sheet.items.append(i)
                    sheets.append(new_sheet)
                    # 移除掉被分配的子项
                    min_delivery_item.remove(i)
                else:
                    i, new_item = weight_rule.split_item(i, i.weight - (g.MAX_WEIGHT - current_weight))
                    if new_item:
                        # 生成新提货单，归到该车次下
                        new_sheet = DeliverySheet()
                        new_sheet.load_task_id = k
                        new_sheet.request_id = order.request_id
                        new_sheet.salesorg_id = order.salesorg_id
                        new_sheet.type = '20'
                        new_sheet.volume = i.volume
                        new_sheet.batch_no = batch_no
                        new_sheet.customer_id = order.customer_id
                        new_sheet.salesman_id = order.salesman_id
                        new_sheet.weight = i.weight
                        new_sheet.total_pcs = i.total_pcs
                        new_sheet.items.append(i)
                        i.delivery_item_no = UUIDUtil.create_id('di')
                        # 移除掉被分配的子项
                        min_delivery_item.remove(i)
                        # 将剩余的子项放入子项列表
                        min_delivery_item.insert(0, new_item)
                        sheets.append(new_sheet)
                    break
        # 装填完如果小管还有剩余
        if min_delivery_item:
            # 2、使用模型过滤器生成发货通知单
            min_sheets = optimize_filter_max(min_delivery_item)
            # 3、补充发货单的属性
            replenish_property(min_sheets, order, batch_no, '20')
            # 为发货单分配车次
            dispatch_load_task(min_sheets)
            sheets.extend(min_sheets)
