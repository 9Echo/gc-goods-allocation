# -*- coding: utf-8 -*-
# @Time    : 2019/12/12
# @Author  : shaoluyu
from app.main.pipe_factory.rule import package_solution
from model_config import ModelConfig
from flask import g


def filter(delivery_dict_list: list, weight, volume):
    """重量过滤规则：
    总重量不超过35吨
    """
    filtered_items = []
    weight_cost = []
    # 根据item的重量生成weight_cost的列表
    for item in delivery_dict_list:
        weight_cost.append((float(item.get('weight')), float(item.get('volume')), float(item.get('weight'))))
    final_weight, result_list = package_solution.dynamic_programming(len(delivery_dict_list), g.MAX_WEIGHT - weight,
                                                                     ModelConfig.MAX_VOLUME - volume, weight_cost)
    print(result_list)
    for i in range(0, len(result_list)):
        if result_list[i] == 1:
            filtered_items.append(delivery_dict_list[i])
    return filtered_items if filtered_items else None
