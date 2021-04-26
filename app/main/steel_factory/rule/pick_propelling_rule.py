# -*- coding: utf-8 -*-
# Description: 筛选待二次推送的摘单计划
# Created: luchengkai 2020/11/16
import copy
from typing import List
import config
from app.main.steel_factory.dao.pick_propelling_dao import pick_propelling_dao
from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.main.steel_factory.rule.pick_data_format_rule import data_format_insert, data_format_msg
from app.util.rest_template import RestTemplate


def pick_list_filter():
    """
    从t_pick_order表中获取待摘单列表
    :return: wait_propelling_list
    """
    wait_propelling_list = pick_propelling_dao.select_wait_pick_list()
    return wait_propelling_list


def pick_driver_list():
    """
    从t_pick_order表中获取待摘单列表
    :return: wait_driver_list
    """
    wait_driver_list = pick_propelling_dao.select_wait_driver_list()
    return wait_driver_list


def interaction_with_java(propelling_driver_list: List[PickPropelling], exist_driver_list: List[PickPropellingDriver]):
    """
    和后台交互
    1、新增司机列表
    2、调用短信接口
    :param propelling_driver_list:
    :param exist_driver_list:
    :return:
    """
    res1 = None
    res2 = None
    """1、新增司机列表"""
    insert_driver_list = copy.deepcopy(propelling_driver_list)
    for propelling in insert_driver_list:
        # 根据摘单号提取司机id  list
        exist_driver_id_list = [item.driver_id for item in exist_driver_list if item.pickup_no == propelling.pickup_no]
        # 不在已经存在的列表的司机
        propelling.drivers = [item for item in propelling.drivers if item.driver_id not in exist_driver_id_list]
    result_insert_list = data_format_insert(insert_driver_list)
    if result_insert_list:
        for result_insert in result_insert_list:
            url = config.get_active_config().TENDER_SERVICE_URL + "/pickUpList/insertPickDriverList"
            res1 = RestTemplate.do_post(url, result_insert)

    """2、调用短信接口"""
    result_msg_list = data_format_msg(propelling_driver_list)
    if result_msg_list:
        url = config.get_active_config().TENDER_SERVICE_URL + "/pickUpMessage/incompletePickUp"
        res2 = RestTemplate.do_post(url, result_msg_list)
    return res1, res2
