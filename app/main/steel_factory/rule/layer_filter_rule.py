from typing import Dict, List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule.calculate_rule import calculate
from app.main.steel_factory.rule.goods_filter_rule import goods_filter
from app.main.steel_factory.rule.split_rule import split
from app.util.enum_util import DispatchType, LoadTaskType
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig


def layer_filter(general_stock_dict, load_task_list, dispatch_type):
    """

    :param general_stock_dict:
    :param load_task_list:
    :param dispatch_type:
    :return:
    """
    first_result_dict = first_deal_general_stock(general_stock_dict, load_task_list, dispatch_type)
    second_result_dict = second_deal_general_stock(first_result_dict, load_task_list, dispatch_type)
    # 不同区两装一卸取消
    # third_stock_dict = third_deal_general_stock(second_result_dict, load_task_list, dispatch_type)
    surplus_stock_dict = fourth_deal_general_stock(second_result_dict, load_task_list, dispatch_type)
    return surplus_stock_dict


def first_deal_general_stock(general_stock_dict: Dict[int, Stock], load_task_list: List[LoadTask], dispatch_type):
    """
    一装一卸筛选器
    :param dispatch_type:
    :param general_stock_dict:
    :param load_task_list:
    :return:
    """
    result_dict = dict()
    while general_stock_dict:
        # 取第一个
        stock_id = list(general_stock_dict.keys())[0]
        temp_stock = general_stock_dict.get(stock_id)
        if dispatch_type is DispatchType.FIRST and temp_stock.sort != 1:
            result_dict = {**result_dict, **general_stock_dict}
            break
        if dispatch_type is DispatchType.THIRD:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name)
        else:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT - temp_stock.actual_weight
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
            general_stock_dict.pop(stock_id)
        # 得到待匹配列表
        filter_list = [v for v in general_stock_dict.values() if v.deliware_house == temp_stock.deliware_house
                       and v.standard_address == temp_stock.standard_address
                       and v.piece_weight <= surplus_weight
                       and v.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
            temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
        if filter_list:
            if temp_stock.big_commodity_name == '老区-型钢':
                temp_max_weight: int = 0
                # 目标拼货组合
                target_compose_list: List[Stock] = list()
                temp_set: set = set([i.specs for i in filter_list])
                for i in temp_set:
                    temp_list = [v for v in filter_list if v.specs == i or v.specs == temp_stock.specs]
                    result_list = split(temp_list)
                    # 选中的列表
                    compose_list, value = goods_filter(result_list, surplus_weight)
                    if value >= new_min_weight:
                        if temp_max_weight < value:
                            temp_max_weight = value
                            target_compose_list = compose_list
                if temp_max_weight:
                    temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
                    calculate(target_compose_list, general_stock_dict, load_task_list, temp_stock,
                              LoadTaskType.TYPE_1.value)
                    continue
            else:
                temp_list = split(filter_list)
                # 选中的列表
                compose_list, value = goods_filter(temp_list, surplus_weight)
                if value >= new_min_weight:
                    temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
                    calculate(compose_list, general_stock_dict, load_task_list, temp_stock, LoadTaskType.TYPE_1.value)
                    continue
        elif temp_stock.actual_weight >= get_lower_limit(temp_stock.big_commodity_name):
            calculate([], general_stock_dict, load_task_list, temp_stock, LoadTaskType.TYPE_1.value)
            continue
        general_stock_dict.pop(stock_id, 404)
        result_dict[stock_id] = temp_stock
    return result_dict


def second_deal_general_stock(general_stock_dict: Dict[int, Stock], load_task_list: List[LoadTask], dispatch_type):
    """
    两装一卸（同区仓库）筛选器
    :param dispatch_type:
    :param general_stock_dict:
    :param load_task_list:
    :return:
    """
    result_dict = dict()
    while general_stock_dict:
        # 取第一个
        stock_id = list(general_stock_dict.keys())[0]
        temp_stock = general_stock_dict.get(stock_id)
        # 优先考虑的货物分完后终止
        if dispatch_type is DispatchType.FIRST and temp_stock.sort != 1:
            result_dict = {**result_dict, **general_stock_dict}
            break
        if dispatch_type is DispatchType.THIRD:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name)
        else:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT - temp_stock.actual_weight
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
            general_stock_dict.pop(stock_id)
        # 获取可拼货同区仓库
        warehouse_out_group = get_warehouse_out_group(temp_stock)
        # 条件筛选
        filter_list = [v for v in general_stock_dict.values() if v.standard_address == temp_stock.standard_address
                       and v.deliware_house in warehouse_out_group
                       and v.piece_weight <= surplus_weight
                       and v.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
            temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
        optimal_weight, target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight, new_min_weight,
                                                                'deliware_house')
        if optimal_weight:
            temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
            calculate(target_compose_list, general_stock_dict, load_task_list, temp_stock, LoadTaskType.TYPE_2.value)
        else:
            general_stock_dict.pop(stock_id, 404)
            result_dict[stock_id] = temp_stock
    return result_dict


def third_deal_general_stock(general_stock_dict: Dict[int, Stock], load_task_list: List[LoadTask], dispatch_type):
    """
    两装一卸（非同区仓库）筛选器
    :param dispatch_type:
    :param general_stock_dict:
    :param load_task_list:
    :return:
    """
    result_dict = dict()
    while general_stock_dict:
        # 取第一个
        stock_id = list(general_stock_dict.keys())[0]
        temp_stock = general_stock_dict.get(stock_id)
        if dispatch_type is DispatchType.FIRST and temp_stock.sort != 1:
            result_dict = {**result_dict, **general_stock_dict}
            break
        if dispatch_type is DispatchType.THIRD:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name)
        else:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT - temp_stock.actual_weight
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
            general_stock_dict.pop(stock_id)
        filter_list = [v for v in general_stock_dict.values() if v.standard_address == temp_stock.standard_address
                       and v.piece_weight <= surplus_weight
                       and v.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
            temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
        optimal_weight, target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight, new_min_weight,
                                                                'deliware_house')
        if optimal_weight:
            temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
            calculate(target_compose_list, general_stock_dict, load_task_list, temp_stock, LoadTaskType.TYPE_3.value)
        else:
            general_stock_dict.pop(stock_id, 404)
            result_dict[stock_id] = temp_stock
    return result_dict


def fourth_deal_general_stock(general_stock_dict: Dict[int, Stock], load_task_list: List[LoadTask], dispatch_type):
    """
    一装两卸筛选器
    :param min_weight:
    :param dispatch_type:
    :param general_stock_dict:
    :param load_task_list:
    :return:
    """
    result_dict = dict()
    while general_stock_dict:
        # 取第一个
        stock_id = list(general_stock_dict.keys())[0]
        temp_stock = general_stock_dict.get(stock_id)
        if dispatch_type is DispatchType.FIRST and temp_stock.sort != 1:
            result_dict = {**result_dict, **general_stock_dict}
            break
        if dispatch_type is DispatchType.THIRD:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name)
        else:
            surplus_weight = ModelConfig.RG_MAX_WEIGHT - temp_stock.actual_weight
            new_min_weight = get_lower_limit(temp_stock.big_commodity_name) - temp_stock.actual_weight
            general_stock_dict.pop(stock_id)
        filter_list = [v for v in general_stock_dict.values() if v.deliware_house == temp_stock.deliware_house
                       and v.actual_end_point == temp_stock.actual_end_point
                       and v.piece_weight <= surplus_weight
                       and v.big_commodity_name in ModelConfig.RG_COMMODITY_GROUP.get(
            temp_stock.big_commodity_name, [temp_stock.big_commodity_name])]
        optimal_weight, target_compose_list = get_optimal_group(filter_list, temp_stock, surplus_weight, new_min_weight,
                                                                'standard_address')
        if optimal_weight:
            temp_stock = temp_stock if dispatch_type is not DispatchType.THIRD else None
            calculate(target_compose_list, general_stock_dict, load_task_list, temp_stock, LoadTaskType.TYPE_4.value)
        else:
            general_stock_dict.pop(stock_id, 404)
            result_dict[stock_id] = temp_stock
    return result_dict


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
    if not filter_list:
        return 0, []
    temp_max_weight: int = 0
    # 目标拼货组合
    target_compose_list: List[Stock] = list()
    temp_set: set = set([getattr(i, attr_name) for i in filter_list])
    # 如果目标货物品类为型钢
    if temp_stock.big_commodity_name == '老区-型钢':
        for i in temp_set:
            if i != getattr(temp_stock, attr_name):
                temp_list = [v for v in filter_list if
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
                temp_list = [v for v in filter_list if
                             getattr(v, attr_name) == i or getattr(v, attr_name) == getattr(temp_stock, attr_name)]

                result_list = split(temp_list)
                # 选中的列表
                compose_list, value = goods_filter(result_list, surplus_weight)
                if value >= new_min_weight:
                    if temp_max_weight < value:
                        temp_max_weight = value
                        target_compose_list = compose_list
    return temp_max_weight, target_compose_list


def get_warehouse_out_group(temp_stock: Stock) -> List[str]:
    target_group = []
    for group in ModelConfig.RG_WAREHOUSE_GROUP:
        if temp_stock.deliware_house in group:
            target_group = group
            break
    return target_group
