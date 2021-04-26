import copy
from flask import g
from app.main.pipe_factory.rule import weight_rule, package_solution
from app.main.pipe_factory.entity.delivery_sheet import DeliverySheet
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from model_config import ModelConfig


def spec_filter(delivery_items: list):
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
    # 如果有超重的子单，进行compose
    while left_items:
        # 每次取第一个元素进行compose,  filtered_item是得到的一个饱和(饱和即已达到重量上限)的子单
        filtered_item, left_items = weight_rule.compose_split(left_items[0], left_items)
        if filtered_item.weight == 0:
            raise MyException('分单异常！', ResponseCode.Error)
        item_list.append(filtered_item)
    item_list = [i for i in item_list if i.weight > 0]
    while item_list:
        # 是否满载标记
        is_full = False
        weight_cost = []
        for item in item_list:
            weight_cost.append((int(item.weight), float(item.volume), int(item.weight)))
        # 将所有待选集进行背包
        final_weight, result_list = package_solution.dynamic_programming(len(item_list), g.MAX_WEIGHT,
                                                                         ModelConfig.MAX_VOLUME,
                                                                         weight_cost)
        if final_weight <= 0:
            raise MyException('package exception', ResponseCode.Error)
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
