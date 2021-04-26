# -*- coding: utf-8 -*-
# Description: 标签提取规则
# Created: luchengkai 2020/11/24
import copy
from typing import List

import config
from app.main.steel_factory.dao.pick_propelling_label_dao import pick_label_dao
from app.main.steel_factory.rule import pick_normal_rule
from app.main.steel_factory.rule.pick_data_format_rule import data_format_to_java, data_format_from_java
from app.main.steel_factory.rule.pick_propelling_recall_screen_rule import get_dist_type
from app.util.rest_template import RestTemplate
from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from model_config import ModelConfig


def pick_label_extract(wait_propelling_list: List[PickPropelling]):
    """
    标签提取
    标签提取总入口
    :param wait_propelling_list:
    :return: wait_driver_list
    """
    # 推荐类型区分：接口调用、定时范围
    for wait_propelling in wait_propelling_list:
        wait_propelling.dist_type = get_dist_type(wait_propelling)
    wait_driver_list = []
    wait_driver_list.extend(pick_transport_poll(wait_propelling_list))  # 运力池
    wait_driver_list.extend(pick_cold_start(wait_propelling_list))  # 冷启动
    wait_driver_list.extend(pick_normal_flow(wait_propelling_list))  # 常运流向
    wait_driver_list.extend(pick_normal_product(wait_propelling_list))  # 常运品种

    wait_propelling_dict = pick_normal_rule.split_group(wait_propelling_list, ["pickup_no"])
    for driver in wait_driver_list:
        driver.pickup_prod_name = wait_propelling_dict.get(driver.pickup_no, [PickPropelling()])[0].prod_name
        driver.pickup_start_time = wait_propelling_dict.get(driver.pickup_no, [PickPropelling()])[0].pickup_start_time

    return wait_driver_list


def pick_transport_poll(wait_propelling_list: List[PickPropelling]):
    """
    运力池
    调用数仓接口，获取6个月内3个区县的司机集
    :return: wait_driver_list
    """
    result_list = []
    url = config.get_active_config().DATA_WAREHOUSE_URL + "/DelegatedDispatching/getListDistrictDriver"
    for propelling in wait_propelling_list:
        post_dic = data_format_to_java(propelling, 12)
        res = RestTemplate.do_post(url, post_dic)
        result_list.extend(data_format_from_java(propelling, res.get('data')))

    return result_list


def pick_cold_start(wait_propelling_list: List[PickPropelling]):
    """
    冷启动
    根据摘单列表中的流向信息获取适合推送的新司机
    :param wait_propelling_list: {city:[PickPropelling_entity]}
    :return: wait_driver_list
    """
    wait_driver_list = []

    # 获取新司机表格
    new_driver_data = pick_label_dao.select_new_driver()

    # 判断是否有符合摘单要求的司机
    # 按城市匹配
    for index, row in new_driver_data.iterrows():
        moblie = row['mobile']
        driver_id = row['driver_id']
        name = row['name']
        city = row['city']
        for pickprop in wait_propelling_list:
            new_entity = PickPropellingDriver()
            new_entity.district_name = pickprop.district_name
            new_entity.pickup_no = pickprop.pickup_no
            new_entity.city_name = city
            new_entity.driver_id = driver_id
            new_entity.driver_name = name + '(新)'
            new_entity.driver_phone = moblie
            new_entity.label_type = ModelConfig.PICK_LABEL_TYPE['L2']
            wait_driver_list.append(new_entity)

    return wait_driver_list


def pick_normal_flow(wait_propelling_list: List[PickPropelling]):
    """
    常运流向
    根据摘单列表中的流向信息获取常运流向司机列表
    :return: wait_driver_list
    """
    driver_list = []
    # 查询司机常运流向
    tmp_driver_list = pick_label_dao.select_user_common_flow()
    if not tmp_driver_list:
        return []
    # 根据城市分组
    driver_dict = pick_normal_rule.split_group(tmp_driver_list, ['city_name'])
    # 循环摘单列表
    for propelling in wait_propelling_list:
        if propelling.dist_type == ModelConfig.PICK_RESULT_TYPE.get('DEFAULT'):
            # 根据摘单城市获取司机
            temp_driver_list = driver_dict.get(propelling.city_name, [])
            for driver in temp_driver_list:
                driver.pickup_no = propelling.pickup_no
                driver.driver_name = driver.driver_name + '(扩)'
                driver.label_type = ModelConfig.PICK_LABEL_TYPE['L3']
            driver_list.extend(temp_driver_list)
    return driver_list


def pick_normal_product(wait_propelling_list: List[PickPropelling]):
    """
    常运品种
    根据摘单列表中的品种信息获取常运品种司机列表
    :return: wait_driver_list
    """
    driver_list = []
    # 根据品名分组
    propelling_dict = pick_normal_rule.split_group(wait_propelling_list, ['prod_name'])
    values = propelling_dict.keys()
    # 查询司机常运品种,并根据品种分组
    tmp_driver_list = pick_label_dao.select_user_common_kind(values)
    if not tmp_driver_list:
        return []
    driver_dict = pick_normal_rule.split_group(tmp_driver_list, ['prod_name'])
    # 循环摘单列表
    for propelling in wait_propelling_list:
        if propelling.dist_type == ModelConfig.PICK_RESULT_TYPE.get('DEFAULT'):
            # 根据摘单城市获取司机
            temp_driver_list = driver_dict.get(propelling.prod_name, [])
            for driver in temp_driver_list:
                driver.pickup_no = propelling.pickup_no
                driver.driver_name = driver.driver_name + '(扩)'
                driver.label_type = ModelConfig.PICK_LABEL_TYPE['L4']
            driver_list.extend(temp_driver_list)
    return driver_list


if __name__ == '__main__':
    dicts = {
        "a": 1,
        "b": 2,
        "c": 3
    }
    a = dicts.keys()
    print(a)
