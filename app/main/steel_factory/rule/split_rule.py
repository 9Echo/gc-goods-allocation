import copy
from typing import List
from app.main.steel_factory.entity.stock import Stock


def split(filter_list: List[Stock]):
    """
    拆分到单件
    :param filter_list:
    :return:
    """
    # 拆分成件的stock列表
    result_list: List[Stock] = list()
    for i in filter_list:
        for j in range(i.actual_number):
            copy_stock = copy.deepcopy(i)
            copy_stock.actual_number = 1
            copy_stock.actual_weight = i.piece_weight
            result_list.append(copy_stock)
    return result_list
