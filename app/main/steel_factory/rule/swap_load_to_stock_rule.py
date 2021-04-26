from typing import List

from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.entity.load_task import LoadTask


def collect_to_list(load_task: LoadTask):
    """
    将load_task中的item放入old_west_list中
    :param load_task:
    :return:
    """
    old_west_list: List[LoadTaskItem] = []
    for item in load_task.items:
        old_west_list.append(item)
    return old_west_list


def swap_load_to_stock(tail_list: List[Stock], old_west_list: List[LoadTaskItem]):
    """
    将old_west_list中的stock合并到tail_list中
    :param tail_list:
    :param old_west_list:
    :return:
    """
    for item in old_west_list:
        flag = False
        for tail in tail_list:
            if item.parent_load_task_id == tail.parent_stock_id:
                flag = True
                tail.actual_weight += item.weight * 1000
                tail.actual_number += item.count
                break
        if flag is False:
            old_west_stock = Stock()
            old_west_stock.actual_weight = item.weight * 1000
            old_west_stock.actual_number = item.count
            # 必须参数
            old_west_stock.parent_stock_id = item.parent_load_task_id
            old_west_stock.notice_num = item.notice_num
            old_west_stock.oritem_num = item.oritem_num
            old_west_stock.deliware_house = item.outstock_code
            old_west_stock.source_number = item.source_number
            old_west_stock.can_split_number = item.can_split_number
            old_west_stock.can_split_weight = item.can_split_weight
            # 非必须参数
            old_west_stock.city = item.city
            old_west_stock.dlv_spot_name_end=item.end_point
            old_west_stock.big_commodity_name = item.big_commodity
            old_west_stock.commodity_name = item.commodity
            old_west_stock.consumer = item.consumer
            # append
            tail_list.append(old_west_stock)
    return tail_list

