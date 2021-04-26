# -*- coding: utf-8 -*-
# Description:
# Created: liujiaye  2020/07/10
from typing import List
from app.main.steel_factory.entity.stock import Stock
from app.util.get_weight_limit import get_lower_limit


def tail_grouping_filter(stock_list: List[Stock]):
    stock_dic = {
        "tail": [],
        "lock": [],
        "huge": []
    }
    user_dic = get_user_grouping(stock_list)
    for s in stock_list:
        # 获取重量下限
        min_weight = get_lower_limit(s.big_commodity_name)
        if s.limit_mark == 0:
            stock_dic["tail"].append(s)
            continue
        if user_dic[s.consumer + "," + s.big_commodity_name] < 150000 and s.actual_weight >= min_weight:
            stock_dic["lock"].append(s)
            continue
        if s.actual_weight >= min_weight:
            stock_dic["huge"].append(s)
        # else:
        #     stock_dic["tail"].append(s)
    return stock_dic


def get_user_grouping(stock_list):
    user_dic = {}
    record_list = []
    for s in stock_list:
        if s.parent_stock_id not in record_list:
            record_list.append(s.parent_stock_id)
            key = s.consumer + "," + s.big_commodity_name
            if key in user_dic:
                user_dic[key] += s.waint_fordel_weight
            else:
                user_dic[key] = s.waint_fordel_weight
    return user_dic
