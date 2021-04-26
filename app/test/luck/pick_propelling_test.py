# -*- coding: utf-8 -*-
# Description:
# Created: luchengkai 2020/12/09 9:12
from typing import List

from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.main.steel_factory.rule import pick_propelling_rule, pick_propelling_label_rule, pick_save_result
from app.main.steel_factory.rule import pick_propelling_recall_screen_rule
from app.main.steel_factory.rule.pick_propelling_label_rule import pick_transport_poll, pick_cold_start, pick_normal_flow, pick_normal_product
from app.main.steel_factory.rule.pick_propelling_recall_screen_rule import pick_driver_distinct, \
    pick_driver_condition_deal, pick_cd_deal, pick_black_list, pick_distance_deal, pick_type_classify, \
    pick_distance_prior
from model_config import ModelConfig


def pick_label_extract(wait_propelling_list: List[PickPropelling]):
    """
    标签提取
    标签提取总入口
    :param wait_propelling_list:
    :return: wait_driver_list
    """

    wait_driver_list = []
    wait_driver_list.extend(pick_transport_poll(wait_propelling_list))  # 运力池
    wait_driver_list.extend(pick_cold_start(wait_propelling_list))  # 冷启动
    wait_driver_list.extend(pick_normal_flow(wait_propelling_list))  # 常运流向
    wait_driver_list.extend(pick_normal_product(wait_propelling_list))  # 常运品种
    #
    wait_propelling_dict = pick_propelling_rule.split_group(wait_propelling_list, ["pickup_no"])
    for driver in wait_driver_list:
        driver.pickup_prod_name = wait_propelling_dict[driver.pickup_no][0].prod_name
        driver.driver_type = wait_propelling_dict[driver.pickup_no][0].driver_type
        driver.pickup_start_time = wait_propelling_dict[driver.pickup_no][0].pickup_start_time

    return wait_driver_list


def pick_recall_screen(driver_list1: List[PickPropellingDriver]):
    """
    召回筛选
    召回筛选总入口
    :return: driver_list
    """
    driver_list1 = pick_driver_distinct(driver_list1)  # 去重
    # driver_list1 = pick_driver_condition_deal(driver_list1)  # 根据司机状态筛选
    # driver_list1 = pick_cd_deal(driver_list1)  # 冷却期处理
    # driver_list1 = pick_black_list(driver_list1)  # 黑名单处理

    # 日钢优先处理
    driver_list1 = pick_distance_deal(driver_list1)  # 司机当前距日钢距离
    driver_list12, driver_list22 = pick_type_classify(driver_list1)
    driver_list1 = pick_distance_prior(driver_list12, driver_list22)

    return driver_list1


def propelling(wait_propelling_list=None, wait_driver_list=None):
    """
    摘单计划推送入口
    :return:
    """
    """
    1.摘单计划筛选
    2.司机集获取(标签提取)
    3.司机集筛选(召回筛选)
    4.摘单计划与司机集合并
    """
    """1.摘单计划筛选"""
    res1 = None
    res2 = None
    wait_driver_list = []  # 待推送消息的司机列表
    wait_propelling_list = pick_propelling_rule.pick_list_filter()  # 待推送摘单计划列表
    exist_driver_list = pick_propelling_rule.pick_driver_list()  #
    # 待推送摘单计划中已存在的司机列表
    if wait_propelling_list:
        # 品名变更
        for wait_propelling in wait_propelling_list:
            tmp_prod = wait_propelling.prod_name.split(",")
            wait_propelling.prod_name = ModelConfig.PICK_REMARK.get(tmp_prod[0], '未知品种')
        """2.司机集获取(标签提取)"""
        wait_driver_list.extend(pick_label_extract(wait_propelling_list))
        """3.司机集筛选(召回筛选)"""
        wait_driver_list = pick_recall_screen(wait_driver_list)
        """4.摘单计划与司机集合并"""
        propelling_driver_list = pick_propelling_rule.add_driver_to_propelling(
            wait_propelling_list, wait_driver_list)
        """5.后台交互"""
        res1, res2 = pick_propelling_rule.interaction_with_java(propelling_driver_list, exist_driver_list)
        # 结果保存到数据库
        pick_save_result.save_propelling_log(propelling_driver_list)
    return res1, res2


if __name__ == '__main__':
    propelling_list = []
    for i in range(2):
        item = PickPropelling()
        item.pickup_no = "pickup_no_" + str(i)
        item.prod_name = "黑卷"
        item.city_name = "济南市"
        item.district_name = "历城区"
        propelling_list.append(item)
    #
    # driver_list = []
    #
    # item = PickPropellingDriver()
    # item.pickup_no = "pickup_no_0"
    # item.pickup_prod_name = "黑卷"
    # item.district_name = "历城区"
    # item.driver_id = "U000048839"
    # driver_list.append(item)
    #
    # for i in range(9):
    #     item = PickPropellingDriver()
    #     item.pickup_no = "pickup_no_" + str(i)
    #     item.pickup_prod_name = "黑卷"
    #     item.district_name = "历城区"
    #     item.driver_id = "driver_id_" + str(i)
    #     driver_list.append(item)

    propelling()
