# -*- coding: utf-8 -*-
# @Time    : 2019/11/25
# @Author  : biyushuang
from app.main.pipe_factory.dao.weight_calculator_dao import weight_calculator_dao
from flask import g


def get_item_a_dict_list(item_list):
    '''
    :param item_id_list: 物资代码列表
    :return: 得到根重的delivery_item列表
    '''
    return weight_calculator_dao.get_data_list_from_table(item_list)


def calculate_pcs(cname, itemid, pack_num=0, free_num=0):
    """
    :return: t_calculator_item中有此品种规格的记录，则返回:总根数，反之返回:0
    """
    data = g.ITEM_A_DICT.get(itemid)
    total_pcs = 0
    if data:
        # 根重
        if data["GS_PER"] and float(data["GS_PER"]) > 0:
            pcs = int(data["GS_PER"])
            total_pcs = int(pack_num) * pcs + int(free_num)
    return total_pcs


def get_quantity_pcs(cname, itemid):
    """
    根据品类和规格查询一件有多少根
    """
    return calculate_pcs(cname, itemid, 1, 0)


def calculate_weight(cname, itemid, pack_num=0, free_num=0):
    """
    # 外径、壁厚、长度、系数、根 / 件数
    # i["JM_D"], i["JM_P"], i["VER_L"], i["GS_XS"], i["GS_PER"]
    输入数据：品名:cname、规格:itemid、件数:pack_num、散根数:free_num
    :return: t_calculator_item中有此品种规格的记录，则返回:理重weight，反之返回:0
    """
    data = g.ITEM_A_DICT.get(itemid)
    weight = 0
    if data:
        # 根重
        if data["GBGZL"] and float(data["GBGZL"]) > 0:
            weight_one = float(data["GBGZL"])
            # else:
            #     weight_one = get_weight_of_each_root(i)
            # if pack_num == 0:
            #     weight = round(weight_one) * int(free_num)
            GS_PER = data["GS_PER"] or 0
            weight = round(weight_one * (int(pack_num) * GS_PER + int(free_num)))
    return weight

