# -*- coding: utf-8 -*-
# Description:
# Created: luchengkai 2021/01/06 10:08
from app.main.steel_factory.dao.pick_propelling_filter_dao import pick_propelling_filter_dao
from app.main.steel_factory.rule import pick_propelling_label_rule, pick_data_format_rule, pick_save_result
from app.main.steel_factory.rule import pick_propelling_recall_screen_rule
from app.util.result import Result
from model_config import ModelConfig


def get_driver(json_data):
    """
    司机集获取入口
    :param json_data:
    :return:
    """
    """
    1.摘单计划筛选
    2.司机集获取(标签提取)
    3.司机集筛选(召回筛选)
    """
    """1.摘单计划筛选"""
    res1 = None
    wait_driver_list = []  # 待推送消息的司机列表
    wait_propelling_list = pick_data_format_rule.data_format_district(json_data)  # 待匹配摘单列表
    # 根据前端传入的摘单信息去数据库查找对应摘单计划
    temp_wait_propelling_list = pick_propelling_filter_dao.select_pick_truck_count(wait_propelling_list[0].pickup_no)
    # 如果查不到，取前端传入的摘单信息
    wait_propelling_list = temp_wait_propelling_list if temp_wait_propelling_list else wait_propelling_list

    if wait_propelling_list:
        # 品名变更
        tmp_prod = wait_propelling_list[0].prod_name.split(",")
        wait_propelling_list[0].prod_name = ModelConfig.PICK_REMARK.get(tmp_prod[0], '未知品种')
        """2.司机集获取(标签提取)"""
        wait_driver_list.extend(pick_propelling_label_rule.pick_label_extract(wait_propelling_list))
        """3.司机集筛选(召回筛选)"""
        propelling_driver_list, total_count, current_count = pick_propelling_recall_screen_rule.pick_driver_recall_screen(
            wait_driver_list, wait_propelling_list)
        # 结果保存到数据库
        pick_save_result.save_propelling_log(propelling_driver_list)
        # 格式转换
        res1 = pick_data_format_rule.data_format_driver(propelling_driver_list, total_count, current_count)
    return Result.success(data=res1)


def deal_propelling(wait_propelling_list, truck_count_dict):
    for propelling in wait_propelling_list:
        if truck_count_dict[propelling.pickup_no]:
            tmp_propelling = truck_count_dict[propelling.pickup_no][0]
            propelling.total_truck_num = tmp_propelling.total_truck_num
            propelling.pickup_start_time = tmp_propelling.pickup_start_time
            propelling.remain_truck_num = tmp_propelling.reamin_truck_num
            propelling.end_point = tmp_propelling.end_point
            propelling.start_point = tmp_propelling.start_point
            propelling.total_weight = tmp_propelling.total_weight
            propelling.remain_total_weight = tmp_propelling.remain_total_weight

    return wait_propelling_list
