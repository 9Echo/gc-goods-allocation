import copy
from typing import List
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule import pulp_solve


def goods_filter(general_stock_list: List[Stock], surplus_weight: int) -> (List[Stock], int):
    """
    背包过滤方法
    :param surplus_weight:
    :param general_stock_list:
    :return:
    """
    compose_list = list()
    general_stock_list = sorted(general_stock_list, key=lambda x: x.actual_weight, reverse=False)
    weight_list = ([item.actual_weight for item in general_stock_list])
    value_list = copy.deepcopy(weight_list)
    result_index_list, value = pulp_solve.pulp_pack(weight_list, None, value_list, surplus_weight)
    for index in sorted(result_index_list, reverse=True):
        compose_list.append(general_stock_list[index])
        general_stock_list.pop(index)
    if not value:
        return compose_list, 0
    # 数据返回
    return compose_list, value
