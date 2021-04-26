from typing import List, Dict
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.entity.truck import Truck
from app.main.steel_factory.rule.create_load_task_rule import create_load_task
from app.main.steel_factory.rule.goods_filter_rule import goods_filter
from app.main.steel_factory.rule.split_rule import split
from app.util.enum_util import DispatchType, LoadTaskType
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig


def layer_filter(stock_list: List, stock_dict: Dict, truck: Truck):
    """
    按层次分货
    第一层：一装一卸
    第二层：同库两装一卸
    第三层：异库两装一卸
    第四层：一装两卸
    """
    max_weight = truck.load_weight
    tail_list = stock_dict['tail']
    huge_list = stock_dict['huge']
    function_list = [first_deal_general_stock, second_deal_general_stock, fourth_deal_general_stock]
    # 先跟tail_list执行拼货
    for i in stock_list:
        if truck.big_commodity_name and i.big_commodity_name != truck.big_commodity_name:
            continue
        if i.actual_weight > (max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT):
            continue
        for function in function_list:
            load_task = function(tail_list, i, DispatchType.SECOND, max_weight)
            if load_task:
                return merge_result(load_task)
        for function in function_list:
            load_task = function(huge_list, i, DispatchType.SECOND, max_weight)
            if load_task:
                return merge_result(load_task)
    return None


def first_deal_general_stock(stock_list, i, dispatch_type, max_weight):
    """
    一装一卸筛选器
    :param i:
    :param max_weight:
    :param stock_list:
    :param dispatch_type:
    :return:
    """
    # 取第i个元素作为目标库存
    temp_stock = i
    # 拆散的情况下，最大重量等于车辆最大载重，下浮1000
    if dispatch_type is DispatchType.THIRD:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT
        new_min_weight = surplus_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT
    # 如果拉标载
    elif get_lower_limit(temp_stock.big_commodity_name) <= max_weight <= ModelConfig.RG_MAX_WEIGHT:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
        new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
    # 不拉标载，最小重量等于车辆最大载重扣除目标货物的重量，下浮2000
    else:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
        new_min_weight = max_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT - temp_stock.actual_weight
    # 得到待匹配列表
    filter_list = [stock for stock in stock_list if stock is not temp_stock
                   and stock.deliware_house == temp_stock.deliware_house
                   and stock.standard_address == temp_stock.standard_address
                   and stock.piece_weight <= surplus_weight
                   and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
        temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
    # 如果卷重小于24或者大于29，则不拼线材
    if temp_stock.big_commodity_name == '老区-卷板' and (
            temp_stock.actual_weight >= ModelConfig.RG_J_MIN_WEIGHT or
            temp_stock.actual_weight < ModelConfig.RG_SECOND_MIN_WEIGHT):
        filter_list = [stock_j for stock_j in filter_list if stock_j.big_commodity_name == '老区-卷板']
    if filter_list:
        for i in range(len(filter_list)):
            temp_filter_list = filter_list[:i + 1]
            if temp_stock.big_commodity_name == '老区-型钢':
                temp_max_weight: int = 0
                # 目标拼货组合
                target_compose_list: List[Stock] = list()
                temp_set: set = set([i.specs for i in temp_filter_list])
                for i in temp_set:
                    temp_list = [v for v in temp_filter_list if v.specs == i or v.specs == temp_stock.specs]
                    result_list = split(temp_list)
                    # 选中的列表
                    compose_list, value = goods_filter(result_list, surplus_weight)
                    if value >= new_min_weight:
                        if temp_max_weight < value:
                            temp_max_weight = value
                            target_compose_list = compose_list
                if temp_max_weight:
                    temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
                    if temp_stock:
                        target_compose_list.append(temp_stock)
                    return create_load_task(target_compose_list, None, LoadTaskType.TYPE_1.value)
            else:
                temp_list = split(temp_filter_list)
                # 选中的列表
                compose_list, value = goods_filter(temp_list, surplus_weight)
                if value >= new_min_weight:
                    temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
                    if temp_stock:
                        compose_list.append(temp_stock)
                    return create_load_task(compose_list, None, LoadTaskType.TYPE_1.value)
    # 一单在达标重量之上并且无货可拼的情况生成车次
    if new_min_weight <= 0:
        return create_load_task([temp_stock], None, LoadTaskType.TYPE_1.value)
    if (temp_stock.big_commodity_name
            in ModelConfig.RG_J_GROUP
            and temp_stock.actual_number == 1
            and temp_stock.actual_weight >= ModelConfig.RG_J_PIECE_MIN_WEIGHT):
        return create_load_task([temp_stock], None, LoadTaskType.TYPE_1.value)
    else:
        return None


def second_deal_general_stock(stock_list, i, dispatch_type, max_weight):
    """
    两装一卸（同区仓库）筛选器
    :param max_weight:
    :param i:
    :param stock_list:
    :param dispatch_type:
    :return:
    """
    # 取第i个元素作为目标库存
    temp_stock = i
    # 拆散的情况下，最大重量等于车辆最大载重，下浮1000
    if dispatch_type is DispatchType.THIRD:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT
        new_min_weight = surplus_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT
    # 如果拉标载
    elif get_lower_limit(temp_stock.big_commodity_name) <= max_weight <= ModelConfig.RG_MAX_WEIGHT:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
        new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
    # 不拉标载，最大重量等于车辆最大载重扣除目标货物的重量，下浮2000
    else:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
        new_min_weight = max_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT - temp_stock.actual_weight
    # 获取可拼货同区仓库
    warehouse_out_group = get_warehouse_out_group(temp_stock)
    # 条件筛选
    filter_list = [stock for stock in stock_list if stock is not temp_stock
                   and stock.standard_address == temp_stock.standard_address
                   and stock.deliware_house in warehouse_out_group
                   and stock.piece_weight <= surplus_weight
                   and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
        temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
    optimal_weight, target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight, new_min_weight,
                                                            'deliware_house')
    if optimal_weight:
        temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
        if temp_stock:
            target_compose_list.append(temp_stock)
        return create_load_task(target_compose_list, None, LoadTaskType.TYPE_2.value)
    else:
        return None


def fourth_deal_general_stock(stock_list, i, dispatch_type, max_weight):
    """
    一装两卸筛选器
    :param max_weight:
    :param i:
    :param stock_list:
    :param dispatch_type:
    :return:
    """
    # 取第i个元素作为目标库存
    temp_stock = i
    # 拆散的情况下，最大重量等于车辆最大载重，下浮1000
    if dispatch_type is DispatchType.THIRD:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT
        new_min_weight = surplus_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT
    # 如果拉标载
    elif get_lower_limit(temp_stock.big_commodity_name) <= max_weight <= ModelConfig.RG_MAX_WEIGHT:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
        new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
    # 不拉标载，最大重量等于车辆最大载重扣除目标货物的重量，下浮2000
    else:
        surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
        new_min_weight = max_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT - temp_stock.actual_weight
    filter_list = [stock for stock in stock_list if stock is not temp_stock
                   and stock.deliware_house == temp_stock.deliware_house
                   and stock.actual_end_point == temp_stock.actual_end_point
                   and stock.piece_weight <= surplus_weight
                   and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
        temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
    optimal_weight, target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight, new_min_weight,
                                                            'standard_address')
    if optimal_weight:
        if temp_stock:
            target_compose_list.append(temp_stock)
        return create_load_task(target_compose_list, None, LoadTaskType.TYPE_4.value)
    else:
        return None


def get_optimal_group(filter_list, temp_stock, surplus_weight, new_min_weight, attr_name):
    """
    获取最优组别
    :param attr_name:
    :param filter_list:
    :param temp_stock:
    :param surplus_weight:
    :param new_min_weight:
    :return:
    """
    # 如果卷重小于24或者大于29，则不拼线材
    if temp_stock.big_commodity_name == '老区-卷板' and (
            temp_stock.actual_weight >= ModelConfig.RG_J_MIN_WEIGHT or
            temp_stock.actual_weight <= ModelConfig.RG_SECOND_MIN_WEIGHT):
        filter_list = [stock_j for stock_j in filter_list if stock_j.big_commodity_name == '老区-卷板']
    if not filter_list:
        return 0, []
    for item in range(len(filter_list)):
        temp_filter_list = filter_list[:item + 1]
        temp_max_weight: int = 0
        # 目标拼货组合
        target_compose_list: List[Stock] = list()
        temp_set: set = set([getattr(i, attr_name) for i in temp_filter_list])
        # 如果目标货物品类为型钢
        if temp_stock.big_commodity_name == '老区-型钢':
            for i in temp_set:
                if i != getattr(temp_stock, attr_name):
                    temp_list = [v for v in temp_filter_list if
                                 getattr(v, attr_name) == i or getattr(v, attr_name) == getattr(temp_stock, attr_name)]
                    # 获取规格信息
                    spec_set = set([j.specs for j in temp_list])
                    for spec in spec_set:
                        xg_list = [v for v in temp_list if v.specs == temp_stock.specs or v.specs == spec]
                        result_list = split(xg_list)
                        # 选中的列表
                        compose_list, value = goods_filter(result_list, surplus_weight)
                        if value >= new_min_weight:
                            if temp_max_weight < value:
                                temp_max_weight = value
                                target_compose_list = compose_list
        else:
            for i in temp_set:
                if i != getattr(temp_stock, attr_name):
                    temp_list = [v for v in temp_filter_list if
                                 getattr(v, attr_name) == i or getattr(v, attr_name) == getattr(temp_stock, attr_name)]
                    result_list = split(temp_list)
                    # 选中的列表
                    compose_list, value = goods_filter(result_list, surplus_weight)
                    if value >= new_min_weight:
                        if temp_max_weight < value:
                            temp_max_weight = value
                            target_compose_list = compose_list
        if temp_max_weight:
            return temp_max_weight, target_compose_list
    return 0, []


def get_warehouse_out_group(temp_stock: Stock) -> List[str]:
    for group in ModelConfig.RG_WAREHOUSE_GROUP:
        if temp_stock.deliware_house in group:
            return group


def merge_result(load_task):
    if load_task:
        result_dict = dict()
        for item in load_task.items:
            result_dict.setdefault(item.parent_load_task_id, []).append(item)
        # 暂时清空items
        load_task.items = []
        for res_list in result_dict.values():
            sum_list = [(i.weight, i.count) for i in res_list]
            sum_weight = sum(i[0] for i in sum_list)
            sum_count = sum(i[1] for i in sum_list)
            res_list[0].weight = round(sum_weight, 3)
            res_list[0].count = sum_count
            load_task.items.append(res_list[0])
        return load_task
    else:
        return None

#
# """
# 对于上面拼货尾货的优化，使当前货物先和重量小的货物拼凑，但是可能存在拼货失败的情况
# """
# import copy
# import pandas as pd
# from app.main.steel_factory.entity.load_task import LoadTask
# from app.main.steel_factory.entity.load_task_item import LoadTaskItem
#
#
# def layer_filter1(stock_list: List, stock_dict: Dict, truck: Truck):
#     max_weight = truck.load_weight
#     load_task_list: List[LoadTask] = []
#     tail_list = stock_dict['tail']
#     tail_list.sort(key=lambda x: x.actual_weight, reverse=False)  #
#     function_list = [first_deal_general_stock1, second_deal_general_stock, fourth_deal_general_stock]
#     for i in stock_list:
#         if truck.big_commodity_name and i.big_commodity_name != truck.big_commodity_name:
#             continue
#         if i.actual_weight > (max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT):
#             continue
#         for function in function_list:
#             load_task = function(tail_list, i, DispatchType.SECOND, max_weight)
#             if load_task:
#                 load_task_list.append(merge_result(load_task))
#     # 将load_task_list导出为excel
#     df = pd.DataFrame()
#     if load_task_list:
#         tid = 0
#         truck_id = []
#         total_weight = []
#         load_task_type = []
#         priority_grade = []
#         big_commodity = []
#         commodity = []
#         city = []
#         consumer = []
#         weight = []
#         count = []
#         priority = []
#         latest_order_time = []
#         out_stock_code = []
#         for load_task in load_task_list:
#             tid = tid + 1
#             load_task_item: LoadTaskItem
#             for load_task_item in load_task.items:
#                 truck_id.append(tid)
#                 total_weight.append(load_task.total_weight)
#                 load_task_type.append(load_task.load_task_type)
#                 priority_grade.append(load_task.priority_grade)
#                 big_commodity.append(load_task_item.big_commodity)
#                 commodity.append(load_task_item.commodity)
#                 city.append(load_task_item.city)
#                 consumer.append(load_task_item.consumer)
#                 weight.append(load_task_item.weight)
#                 count.append(load_task_item.count)
#                 priority.append(load_task_item.priority)
#                 latest_order_time.append(load_task_item.latest_order_time)
#                 out_stock_code.append(load_task_item.outstock_code)
#         df.insert(0, 'commodity', commodity)
#         df.insert(0, 'big_commodity', big_commodity)
#         df.insert(0, 'out_stock_code', out_stock_code)
#         df.insert(0, 'consumer', consumer)
#         df.insert(0, 'city', city)
#         df.insert(0, 'priority', priority)
#         df.insert(0, 'load_task_type', load_task_type)
#         df.insert(0, 'priority_grade', priority_grade)
#         df.insert(0, 'latest_order_time', latest_order_time)
#         df.insert(0, 'count', count)
#         df.insert(0, 'weight', weight)
#         df.insert(0, 'total_weight', total_weight)
#         df.insert(0, 'truck_id', truck_id)
#     df.to_excel('result.xls')
#     if load_task_list:
#         return load_task_list[0]
#     else:
#         return None
#
#
# def first_deal_general_stock1(stock_list, temp_stock, dispatch_type, max_weight):
#     # 拆散的情况下，最大重量等于车辆最大载重，下浮1000
#     if dispatch_type is DispatchType.THIRD:
#         surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT
#         new_min_weight = surplus_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT
#     # 如果拉标载
#     elif get_lower_limit(temp_stock.big_commodity_name) <= max_weight <= ModelConfig.RG_MAX_WEIGHT:
#         surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
#         new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
#     # 不拉标载，最小重量等于车辆最大载重扣除目标货物的重量，下浮2000
#     else:
#         surplus_weight = max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT - temp_stock.actual_weight
#         new_min_weight = max_weight - ModelConfig.RG_SINGLE_LOWER_WEIGHT - temp_stock.actual_weight
#     # 得到待匹配列表
#     filter_list = [stock for stock in stock_list if stock is not temp_stock
#                    and stock.deliware_house == temp_stock.deliware_house
#                    and stock.standard_address == temp_stock.standard_address
#                    and stock.piece_weight <= surplus_weight
#                    and stock.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(temp_stock.big_commodity_name,
#                                                                                       [temp_stock.big_commodity_name])]
#     # 如果卷重小于24或者大于29，则不拼线材
#     if temp_stock.big_commodity_name == '老区-卷板' and (
#             temp_stock.actual_weight >= ModelConfig.RG_J_MIN_WEIGHT or
#             temp_stock.actual_weight < ModelConfig.RG_SECOND_MIN_WEIGHT):
#         filter_list = [stock_j for stock_j in filter_list if stock_j.big_commodity_name == '老区-卷板']
#     if filter_list:
#         temp_max_weight: int = 0  # 目标拼货组合重量
#         target_compose_list: List[Stock] = list()  # 目标拼货组合库存列表
#         if temp_stock.big_commodity_name == '老区-型钢':  # 型钢最多只能拼两个规格（包括自身的规格）
#             temp_set: set = set([i.specs for i in filter_list])
#             for i in temp_set:
#                 # 筛选出满足规格要求的货物
#                 temp_list = [v for v in filter_list if v.specs == i or v.specs == temp_stock.specs]
#                 compose_list, compose_weight = get_optimal_compose(temp_list, surplus_weight)
#                 if compose_weight > temp_max_weight:
#                     temp_max_weight = compose_weight
#                     target_compose_list = compose_list
#         else:  # 非型钢
#             target_compose_list, temp_max_weight = get_optimal_compose(filter_list, surplus_weight)
#         if temp_max_weight >= new_min_weight:  # 满足重量下限要求
#             temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
#             if temp_stock:
#                 target_compose_list.append(temp_stock)
#             return create_load_task(target_compose_list, None, LoadTaskType.TYPE_1.value)
#     # 一单在达标重量之上并且无货可拼的情况生成车次
#     elif new_min_weight <= 0:
#         return create_load_task([temp_stock], None, LoadTaskType.TYPE_1.value)
#     elif (temp_stock.big_commodity_name in ModelConfig.RG_J_GROUP
#           and temp_stock.actual_number == 1
#           and temp_stock.actual_weight >= ModelConfig.RG_J_PIECE_MIN_WEIGHT):
#         return create_load_task([temp_stock], None, LoadTaskType.TYPE_1.value)
#     else:
#         return None
#
#
# def get_optimal_compose(filter_list: List[Stock], surplus_weight):
#     compose_weight: int = 0  # 目标拼货组合重量
#     compose_list: List[Stock] = list()  # 目标拼货组合库存列表
#     for stock in filter_list:
#         if compose_weight + stock.actual_weight <= surplus_weight:  # 直接添加整条库存
#             compose_weight += stock.actual_weight
#             compose_list.append(stock)
#         elif compose_weight + stock.piece_weight <= surplus_weight:  # 添加库存中的某几件
#             count = 1
#             for count in range(1, stock.actual_number + 1):  # 找出当前货物中有几件可以添加进目标组合
#                 if compose_weight + count * stock.piece_weight > surplus_weight:
#                     break
#             copy_stock = copy.deepcopy(stock)
#             copy_stock.actual_number = count - 1
#             copy_stock.actual_weight = copy_stock.actual_number * stock.piece_weight
#             compose_weight += copy_stock.actual_weight
#             compose_list.append(copy_stock)
#     return compose_list, compose_weight
