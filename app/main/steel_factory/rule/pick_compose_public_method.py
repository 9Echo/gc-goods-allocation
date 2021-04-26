# -*- coding: utf-8 -*-
# Description: 共用方法
# Created: jjunf 2020/10/09
import copy
from typing import List
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule.goods_filter_rule import goods_filter
from app.main.steel_factory.rule.split_rule import split
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig
from param_config import ParamConfig


def get_weight_with_city(city_name, commodity_name=None):
    """

    :param city_name:
    :param commodity_name:
    :return:
    """
    if city_name == "滨州市":
        min_weight = ModelConfig.RG_MIN_WEIGHT
        max_weight = ModelConfig.RG_TO_BZ_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT
    else:
        # TODO: 添加品种限制（卷类：29-35.4；非卷类：31-35.4）
        min_weight = round(ModelConfig.RG_MIN_WEIGHT)
        max_weight = round((ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT))

    return min_weight, max_weight


def can_generate_load_task(pre_stock_each_truck: List[Stock]) -> bool:
    """
    判断当前pre_stock_each_truck是否符合业务规则：同区县客户、品种可拼、仓库数量<=2、客户数量<=2、非卷类不能跨厂区、型钢最多只可拼两个规格、青岛客户数量=1
    :param pre_stock_each_truck:
    :return:
    """
    # 将stock与pre_stock_each_truck放入一个列表中
    # copy_pre_stock_each_truck = copy.deepcopy(pre_stock_each_truck)
    # copy_pre_stock_each_truck.append(stock)
    # 区县集合
    district_set = set()
    # 品种集合
    commodity_set = set()
    # 仓库集合
    deliware_house_set = set()
    # 客户集合
    consumer_set = set()
    # 规格集合
    specs_set = set()
    # 厂区集合
    factory_set = set()
    # 总重量
    total_weight = 0
    # 卷类与非卷类的标记：False卷类，True非卷类
    flag = False
    stock = Stock()
    for stock in pre_stock_each_truck:
        district_set.add(stock.dlv_spot_name_end)
        commodity_set.add(stock.big_commodity_name)
        deliware_house_set.add(stock.deliware_house)
        consumer_set.add(stock.consumer)
        specs_set.add(stock.specs)
        total_weight += stock.actual_weight
        # 如果是非卷类
        if stock.big_commodity_name not in ModelConfig.RG_J_GROUP:
            flag = True
        if stock.deliware_house in ModelConfig.RG_WAREHOUSE_GROUP[0]:
            factory_set.add('宝华')
        elif stock.deliware_house in ModelConfig.RG_WAREHOUSE_GROUP[1]:
            factory_set.add('厂内')
        elif stock.deliware_house in ModelConfig.RG_WAREHOUSE_GROUP[2]:
            factory_set.add('岚北港')
        # 如果不满足(非卷类不能跨厂区)条件
        if flag and len(factory_set) > 1:
            return False
        # 如果不满足(同区县客户、仓库数量<=2、客户数量<=2)的条件
        if len(district_set) > 1 or len(deliware_house_set) > 2 or len(consumer_set) > 2:
            return False
        # 青岛客户数量=1
        if stock.city == '青岛市' and len(consumer_set) > 1:
            return False
    for commodity in commodity_set:
        # 如果不满足(品种可拼)条件
        if commodity not in ModelConfig.RG_COMMODITY_GROUP.get(list(commodity_set)[0], [list(commodity_set)[0]]):
            return False
        # 如果不满足(型钢最多只可拼两个规格)条件
        if commodity == '老区-型钢' and len(specs_set) > 2:
            return False
    # 获取重量限制
    min_weight, max_weight = get_weight(stock)
    # 如果不满足重量限制
    if total_weight < min_weight or total_weight > max_weight:
        return False
    return True


def get_weight(stock, mark=None):
    """
    根据stock的属性获取载重上下限
    :param mark: 老区卷类配货标志
    :param stock:
    :return:
    """

    """####重量配置"""
    # # 获取维护好的城市、品种的载重
    # min_weight, max_weight = ParamConfig.RG_COMMODITY_WEIGHT.get(stock.city + ',' + stock.big_commodity_name, [0, 0])
    # if not min_weight:
    #     # 获取该城市的默认载重
    #     min_weight, max_weight = ParamConfig.RG_COMMODITY_WEIGHT.get(stock.city + ',' + '全部', [0, 0])
    # if not min_weight:
    #     # 未维护载重的情况
    #     raise MyException("请先维护【" + stock.city + "】的载重信息！", ResponseCode.Error)
    # return min_weight, max_weight + ModelConfig.RG_SINGLE_UP_WEIGHT

    # 滨州大于等于29吨小于31吨的卷，优先配货，最大载重调到39（只限滨州卷配货）
    if mark and stock.city == '滨州市':
        min_weight = ModelConfig.RG_MIN_WEIGHT
        max_weight = ModelConfig.RG_TO_BZ_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT
    elif (stock.city == '滨州市' and stock.big_commodity_name in ModelConfig.RG_J_GROUP and get_lower_limit(
            stock.big_commodity_name) <= stock.actual_weight < ModelConfig.RG_MIN_WEIGHT):
        min_weight = ModelConfig.RG_MIN_WEIGHT
        max_weight = ModelConfig.RG_TO_BZ_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT
    elif stock.city == '滨州市' and stock.big_commodity_name in ModelConfig.RG_J_GROUP:
        min_weight = ModelConfig.RG_MIN_WEIGHT
        max_weight = ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT
    else:
        min_weight = get_lower_limit(stock.big_commodity_name)
        max_weight = ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT
    return min_weight, max_weight


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
    # # 如果卷重小于24或者大于29，则不拼线材
    # if temp_stock.big_commodity_name in ModelConfig.RG_J_GROUP and (
    #         temp_stock.actual_weight >= ModelConfig.RG_J_MIN_WEIGHT or
    #         temp_stock.actual_weight <= ModelConfig.RG_SECOND_MIN_WEIGHT):
    #     filter_list = [stock_j for stock_j in filter_list if stock_j.big_commodity_name in ModelConfig.RG_J_GROUP]
    if not filter_list:
        return []
    temp_filter_list = filter_list
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
                    if xg_list:
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
                if temp_list:
                    result_list = split(temp_list)
                    if result_list:
                        # 选中的列表
                        compose_list, value = goods_filter(result_list, surplus_weight)
                        if value >= new_min_weight:
                            if temp_max_weight < value:
                                temp_max_weight = value
                                target_compose_list = compose_list
    if temp_max_weight:
        return target_compose_list
    return []


def delete_the_stock_be_composed(stock_list, compose_list):  # 有没有更好的删除方法？？？
    """
    删除库存中已经被组合的货物
    :param stock_list: 库存
    :param compose_list: 已经被组合的货物
    :return:
    """
    stock_dict = {i.stock_id: i for i in stock_list}
    for del_stock in compose_list:
        stock = stock_dict.get(del_stock.stock_id, 'null')
        if stock == 'null':
            continue
        # 扣除数量
        stock.actual_number -= del_stock.actual_number
        # 扣除重量
        stock.actual_weight -= stock.piece_weight * del_stock.actual_number
        # 若actual_number=0，则删除
        if stock.actual_number == 0:
            stock_list.remove(stock)
    return stock_list


def merge_result(load_task):
    """
    将load_task中相同订单的子项合并
    :param load_task:
    :return:
    """
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


def merge_split_stock(stock_list):
    """
    将stock_list中parent_stock_id相同的订单合并为一条记录
    :param stock_list:
    :return:
    """
    if stock_list:
        result_dict = dict()
        for item in stock_list:
            result_dict.setdefault(item.parent_stock_id, []).append(item)
        # 暂时清空stock_list
        stock_list = []
        for res_list in result_dict.values():
            sum_list = [(i.actual_weight, i.actual_number) for i in res_list]
            sum_weight = sum(i[0] for i in sum_list)
            sum_count = sum(i[1] for i in sum_list)
            res_list[0].actual_weight = round(sum_weight, 3)
            res_list[0].actual_number = sum_count
            stock_list.append(res_list[0])
        return stock_list
    else:
        return None
