# -*- coding: utf-8 -*-
# Description: 召回筛选规则
# Created: luchengkai 2020/11/24
import time
from typing import List, Dict
import pandas as pd
import datetime

from geopy import distance
from app.main.steel_factory.dao.pick_propelling_dao import pick_propelling_dao
from app.main.steel_factory.dao.pick_propelling_filter_dao import pick_propelling_filter_dao
from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.main.steel_factory.rule import pick_data_format_rule, pick_normal_rule
from app.main.steel_factory.service.pick_propelling_redis_service import get_pick_propelling_driver_list
from app.main.steel_factory.service.pick_propelling_redis_service import set_pick_propelling_driver_list
from app.util.date_util import get_now_date
from app.util.rest_template import RestTemplate
from model_config import ModelConfig


def pick_recall_screen(driver_list: List[PickPropellingDriver], propelling_list: List[PickPropelling]):
    """
    召回筛选
    召回筛选总入口
    ①去重
    ②冷却期处理
    ③黑名单处理
    ④合并摘单计划和司机列表

    ⑤根据司机状态筛选
    ⑥司机当前距日钢距离
    ⑦活跃司机列表
    :return: driver_list
    """

    driver_list = pick_driver_distinct(driver_list)  # 去重
    driver_list = pick_cd_deal(driver_list)  # 冷却期处理
    driver_list = pick_black_list(driver_list)  # 黑名单处理

    propelling_driver_list = add_driver_to_propelling(propelling_list, driver_list)  # 合并

    propelling_driver_list = pick_driver_condition_deal(propelling_driver_list)  # 根据司机状态筛选
    # 日钢优先处理
    propelling_driver_list = pick_distance_deal(propelling_driver_list)  # 司机当前距日钢距离
    for propelling in propelling_driver_list:
        propelling.drivers = [driver for driver in propelling.drivers if driver.is_in_distance == 1]

    propelling_driver_list = pick_active_deal(propelling_driver_list)  # 活跃司机添加

    return propelling_driver_list


def pick_driver_recall_screen(driver_list: List[PickPropellingDriver], propelling_list: List[PickPropelling]):
    """
    新增摘单计划召回筛选
    :return: driver_list
    """
    # 去重
    driver_list = pick_driver_distinct(driver_list)

    # 司机总数
    current_count = 0
    total_count = len(driver_list)
    propelling_driver_list = add_driver_to_propelling(propelling_list, driver_list)  # 合并
    propelling_driver_list = pick_distance_deal(propelling_driver_list)  # 司机当前距日钢距离
    for propelling in propelling_driver_list:
        propelling.drivers = [driver for driver in propelling.drivers if driver.is_in_distance == 1]
        current_count += len(propelling.drivers)

    return propelling_driver_list, total_count, current_count


def add_driver_to_propelling(wait_propelling_list: List[PickPropelling], wait_driver_list: List[PickPropellingDriver]):
    """
    将司机列表写入对应propelling的对象
    :param wait_propelling_list:
    :param wait_driver_list:
    :return:
    """

    tmp_driver_dic = pick_normal_rule.split_group(wait_driver_list, ['pickup_no'])
    # 将司机列表并入propelling
    for wait_propelling in wait_propelling_list:
        # 将司机拼接到driver_list后
        driver_list = tmp_driver_dic.get(wait_propelling.pickup_no, [])
        wait_propelling.drivers.extend(driver_list)
    return wait_propelling_list


def pick_active_deal(wait_propelling_list):
    """
    将活跃司机列表并入propelling
    :param wait_propelling_list:
    :return:
    """
    # 活跃司机集
    active_list = pick_propelling_filter_dao.select_flows_relation()
    for driver in active_list:
        driver.driver_name += "(扩)"
        driver.label_type = ModelConfig.PICK_LABEL_TYPE['L5']

    # 将活跃司机列表并入propelling
    for wait_propelling in wait_propelling_list:
        # 计算待推送的司机上限
        remain_truck_num = wait_propelling.remain_truck_num
        wait_driver_count = remain_truck_num * 3

        # 将活跃司机拼接到driver_list后
        driver_list = joint_active_driver(wait_propelling.drivers, active_list, wait_driver_count)
        wait_propelling.drivers = driver_list
    return wait_propelling_list


def joint_active_driver(driver_list, active_list, wait_driver_count) -> Dict[str, List]:
    """
    将data_list按照attr_list属性list分组
    :param driver_list: 当前司机集
    :param active_list: 活跃司机集
    :param wait_driver_count: 待推送的司机上限
    :return:
    """
    # 根据driver_id去重
    # 取driver_list中的driver_id列表
    driver_id_list = [driver.driver_id for driver in driver_list]
    active_filter_list = [driver for driver in active_list if driver.driver_id not in driver_id_list]
    # 取前15%的活跃司机
    active_num = int(len(active_filter_list) * 0.15)
    active_filter_list = active_filter_list[0: active_num]

    tmp_driver_list = driver_list
    tmp_driver_list.extend(active_filter_list)
    # 若len(tmp_driver_list)的长度小于wait_driver_count，不截取
    result_list = tmp_driver_list[0: int(min(wait_driver_count, len(tmp_driver_list)))]
    return result_list


def pick_driver_distinct(driver_list: List[PickPropellingDriver]):
    """
    去重
    根据摘单号筛除重复司机
    :return: result_driver_list
    """
    # 筛除重复司机后的司机列表
    result_driver_list = []
    driver_dict = pick_normal_rule.split_group(driver_list, ["pickup_no", "driver_id"])
    # 记录存入redis的时间
    redis_date_time = get_now_date()
    #
    for key in driver_dict.keys():
        driver = driver_dict[key][0]
        driver.redis_date_time = redis_date_time
        result_driver_list.append(driver)
    return result_driver_list


def pick_driver_condition_deal(propelling_driver_list: List[PickPropelling]):
    """
    根据司机状态筛选司机集
    ①筛除重量超限的司机
    ②筛除有任务且在运动的司机
    :return: result_driver_list
    """
    # 筛除重量超限的司机
    # 获取司机调度单的重量信息
    driver_weight_list = pick_propelling_dao.select_driver_weight()
    driver_weight_dict = pick_normal_rule.split_group(driver_weight_list, ['driver_id'])
    for propelling in propelling_driver_list:
        # 单车重量
        single_weight = 0
        try:
            single_weight = propelling.total_weight / propelling.total_truck_num
        except Exception as e:
            pass
        # 重量未超限的司机
        propelling.drivers = [item for item in propelling.drivers if
                              item.driver_id not in driver_weight_dict.keys()
                              or (driver_weight_dict[item.driver_id][0].plan_weight + single_weight <=
                                  ModelConfig.PICK_TOTAL_WEIGHT)]

    # 筛除有任务且在运动的司机
    # 查询到的运动的司机id列表
    move_driver_id_list = []
    # 查询司机实时位置信息列表(按创建时间降序排序)
    driver_gps_list = pick_propelling_dao.select_driver_gps_list()
    driver_gps_dict = pick_normal_rule.split_group(driver_gps_list, ["driver_id"])
    # 根据司机最近一条位置记录和最远一条位置记录，计算距离，判断司机是否运动
    for dict_list in driver_gps_dict.values():
        if len(dict_list) < 2:
            continue
        else:
            # 取司机最近一条位置记录
            driver_first: PickPropellingDriver = dict_list[0]
            # 取司机最远一条位置记录
            driver_final: PickPropellingDriver = dict_list[-1]
            # 最近一条：纬度、经度
            first_tuple = (driver_first.latitude, driver_first.longitude)
            # 最远一条：纬度、经度
            final_tuple = (driver_final.latitude, driver_final.longitude)
            # 计算距离：米
            dist = distance.great_circle(first_tuple, final_tuple).m
            # 如果在运动
            if dist > ModelConfig.PICK_PROPELLING_DISTANCE_LIMIT:
                move_driver_id_list.append(driver_first.driver_id)
    # 从driver_list中筛除掉运动的司机
    for propelling in propelling_driver_list:
        propelling.drivers = [driver for driver in propelling.drivers if driver.driver_id not in move_driver_id_list]
    return propelling_driver_list


def pick_cd_deal(driver_list: List[PickPropellingDriver]):
    """
    冷却期处理
    根据冷却期筛选司机集
    :return: driver_list
    """
    # 从redis中获取司机列表信息
    redis_driver_dict = get_pick_propelling_driver_list()
    redis_driver_list: List[PickPropellingDriver] = [PickPropellingDriver(driver) for driver in redis_driver_dict]
    # 如果redis为空
    if not redis_driver_list:
        # 放回redis
        driver_dict = [driver.as_dict() for driver in driver_list]
        set_pick_propelling_driver_list(driver_dict)
        return driver_list
    # 找出还在冷却期内的司机列表
    redis_driver_id_list = [driver.driver_id for driver in redis_driver_list
                            if driver.redis_date_time >= str((get_now_date() -
                                                              datetime.timedelta(
                                                                  hours=ModelConfig.PICK_PROPELLING_COLD_HOUR)))]
    # 如果没有冷却期内的司机
    if not redis_driver_id_list:
        # 放回redis
        driver_dict = [driver.as_dict() for driver in driver_list]
        set_pick_propelling_driver_list(driver_dict)
        return driver_list
    # 从driver_list中筛除掉还在冷却期内的司机
    driver_list = [driver for driver in driver_list if driver.driver_id not in redis_driver_id_list]
    driver_id_list = [driver.driver_id for driver in driver_list]
    # 找出在冷却期内、但不在driver_list中的司机
    temp_driver_list = [driver for driver in redis_driver_list if driver.driver_id not in driver_id_list]
    # 放回redis
    driver_dict = [driver.as_dict() for driver in driver_list + temp_driver_list]
    set_pick_propelling_driver_list(driver_dict)
    return driver_list


def pick_black_list(driver_list: List[PickPropellingDriver]):
    """
    黑名单处理
    根据黑名单筛选司机集
    :return: driver_list
    """
    # 从blacklist中获取在黑名单的司机列表
    black_driver_list = pick_propelling_dao.select_black_list()

    # 过滤司机集
    if not black_driver_list:
        return driver_list

    for black_driver in black_driver_list:
        driver_list = [item for item in driver_list if black_driver.driver_id != item.driver_id or
                       black_driver.prod_name != item.pickup_prod_name or
                       black_driver.district_name != item.district_name]

    return driver_list


def pick_distance_deal(propelling_driver_list: List[PickPropelling]):
    """
    司机当前距日钢距离
    距日钢越近，越容易获取摘单消息
    ①通过app查询司机位置
    ②通过中交兴路轨迹数据查询车辆位置
    :param propelling_driver_list:
    """
    if not propelling_driver_list:
        return []
    tmp_driver_id_list = []
    for propelling in propelling_driver_list:
        tmp_driver_id_list.extend([driver.driver_id for driver in propelling.drivers])
    """通过app查询司机位置"""
    driver_location_list = pick_propelling_filter_dao.select_driver_location(tmp_driver_id_list)
    driver_location_dict = pick_normal_rule.split_group(driver_location_list, ['driver_id'])
    # 遍历摘单
    for propelling in propelling_driver_list:
        is_break = False
        # 符合条件司机条数
        temp_count = 0
        # 遍历司机列表
        for driver in propelling.drivers:
            # 如果有app位置
            dgs = driver_location_dict.get(driver.driver_id, [])
            if dgs:
                # 司机位置：纬度、经度
                driver_tuple = (dgs[0].latitude, dgs[0].longitude)
                # 日钢位置：纬度、经度
                rg_tuple = (ModelConfig.PICK_RG_LAT.get("日钢纬度"), ModelConfig.PICK_RG_LON.get("日钢经度"))
                # 计算距离：千米
                dist = distance.great_circle(driver_tuple, rg_tuple).km
                driver.latitude = dgs[0].latitude
                driver.longitude = dgs[0].longitude
                driver.vehicle_no = dgs[0].vehicle_no
                driver.location_flag = 'APP'
                driver.dist = dist
                # 如果距离符合条件
                if dist <= propelling.dist_type:
                    driver.is_in_distance = 1
                    temp_count += 1
                    if temp_count >= propelling.remain_truck_num * 3:
                        is_break = True
                        break
        if not is_break:
            # 查询车辆实时位置信息列表(按创建时间降序排序)
            driver_truck_list = pick_propelling_dao.select_driver_truck(
                [inner_driver.driver_id for inner_driver in propelling.drivers])
            for i in range(0, len(driver_truck_list), 100):
                # 取前一百个
                tmp_list = driver_truck_list[i: i + 100]
                str_driver_truck = ','.join([tmp.vehicle_no for tmp in tmp_list])
                # 格式转换
                result_str = pick_data_format_rule.data_format_truck(str_driver_truck)
                url = "https://jcos.jczh56.com/purchase-api/vehicleMultiLocationFromJC"
                result_location = RestTemplate.do_post_for_jcos(url, result_str)
                # 格式转换
                zjxl_list = pick_data_format_rule.data_format_truck_result(result_location)
                for driver in propelling.drivers:
                    # 如果该司机已经纳入推荐列表，则不再计算车辆位置和距离
                    if driver.is_in_distance == 1:
                        continue
                    driver.vehicle_no_list = [driver_truck.vehicle_no for driver_truck in driver_truck_list if
                                              driver_truck.driver_id == driver.driver_id]
                    target_latitude = 0
                    target_longitude = 0
                    target_vehicle = None
                    target_dist = 1000000
                    for no in driver.vehicle_no_list:
                        dgs = [dg for dg in zjxl_list if dg.vehicle_no == no]
                        if dgs:
                            # 司机位置：纬度、经度
                            driver_tuple = (float(dgs[0].latitude) / 600000, float(dgs[0].longitude) / 600000)
                            # 日钢位置：纬度、经度
                            rg_tuple = (ModelConfig.PICK_RG_LAT.get("日钢纬度"), ModelConfig.PICK_RG_LON.get("日钢经度"))
                            # 计算距离：米
                            dist = distance.great_circle(driver_tuple, rg_tuple).km
                            if dist < target_dist:
                                target_dist = dist
                                target_latitude = dgs[0].latitude
                                target_longitude = dgs[0].longitude
                                target_vehicle = dgs[0].vehicle_no
                    driver.latitude = float(target_latitude) / 600000
                    driver.longitude = float(target_longitude) / 600000
                    driver.vehicle_no = target_vehicle
                    driver.location_flag = 'TRUCK'
                    driver.dist = target_dist
                    # 如果距离符合条件
                    if target_dist <= propelling.dist_type:
                        driver.is_in_distance = 1
                        temp_count += 1
                        if temp_count >= propelling.remain_truck_num * 3:
                            is_break = True
                            break
                if is_break:
                    break
                time.sleep(0.1)

    return propelling_driver_list


def get_dist_type(wait_propelling):
    # 如果摘单开始时间为空，说明是初始状态单子，走十公里推荐;如果是策略2，走十公里推荐
    if (not wait_propelling.pickup_start_time) or (wait_propelling.driver_type == 'SJLY20'):
        return ModelConfig.PICK_RESULT_TYPE.get("DIST10")
    now_date = datetime.datetime.now()
    minutes = int((now_date - wait_propelling.pickup_start_time).total_seconds()) / 60
    # 摘单开始了10分钟以内
    if minutes <= ModelConfig.PICK_CONTINUE_TIME.get("MINUTE10"):
        result_type = ModelConfig.PICK_RESULT_TYPE.get("DIST10")
    # 摘单开始了15分钟以内
    elif minutes <= ModelConfig.PICK_CONTINUE_TIME.get("MINUTE15"):
        result_type = ModelConfig.PICK_RESULT_TYPE.get("DIST20")
    # 摘单开始了20分钟以内
    elif minutes <= ModelConfig.PICK_CONTINUE_TIME.get("MINUTE20"):
        result_type = ModelConfig.PICK_RESULT_TYPE.get("DIST30")
    # 摘单开始了25分钟以内
    elif minutes <= ModelConfig.PICK_CONTINUE_TIME.get("MINUTE25"):
        result_type = ModelConfig.PICK_RESULT_TYPE.get("DIST40")
    # 摘单开始了30分钟以内
    elif minutes <= ModelConfig.PICK_CONTINUE_TIME.get("MINUTE30"):
        result_type = ModelConfig.PICK_RESULT_TYPE.get("DIST50")
    # 摘单开始30分钟之后
    else:
        result_type = ModelConfig.PICK_RESULT_TYPE.get("DEFAULT")
    return result_type


if __name__ == '__main__':
    a = get_now_date()
    b = a - datetime.timedelta(hours=6)
    print(a)
    print(b)

    data = pd.read_excel('driver.xls')
    df = pd.DataFrame(data)
    dic = df.to_dict(orient="record")
    driver_list22 = [PickPropellingDriver(obj) for obj in dic]
    # driver_list22 = pick_recall_screen(driver_list22)
