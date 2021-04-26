# -*- coding: utf-8 -*-
# Description: 筛选待二次推送的摘单计划
# Created: luchengkai 2021/01/06
from typing import List
from app.main.steel_factory.dao.pick_propelling_label_dao import pick_label_dao
from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.main.steel_factory.rule import pick_normal_rule
from model_config import ModelConfig


def data_format_to_java(propelling: PickPropelling, num):
    """
    获取运力池司机集:调用数仓接口
    :param propelling:
    :param num:
    :return:
    """
    flow_relation = []
    # 如果距离类型是50公里外，则增加运力池拓展操作
    if propelling.dist_type == ModelConfig.PICK_RESULT_TYPE.get("DEFAULT"):
        # 查询流向关联表
        flow_relation = pick_label_dao.select_flows_relation(propelling)
        # 对结果list排序并截取前3位
        flow_relation.sort()
        flow_relation = flow_relation[0:3]
    # 推送记录的格式转换
    data = {'taskTime': num}
    tmp_list = []
    tmp_dic = {"province": "山东省", "city": propelling.city_name, "district": propelling.district_name}
    tmp_list.append(tmp_dic)
    tmp_list.extend([{"province": "山东省", "city": item.related_city, "district": item.related_district}
                     for item in flow_relation])
    data['pointList'] = tmp_list
    return data


def data_format_from_java(propelling: PickPropelling, res_list):
    """
    获取运力池司机集信息：接收数仓返回司机集
    :param propelling:
    :param res_list:
    :return:
    """
    result_list = []
    if res_list:
        # 单个推送对象的格式转换
        for data in res_list:
            driver = PickPropellingDriver()
            driver.district_name = data['district']
            driver.driver_id = data['driverId']
            driver.driver_name = data['driverName'] + '(扩)'
            driver.driver_phone = data['driverTel']
            driver.label_type = ModelConfig.PICK_LABEL_TYPE['L1']
            driver.pickup_no = propelling.pickup_no
            result_list.append(driver)
    return result_list


def data_format_insert(wait_propelling_list: List[PickPropelling]):
    """
    插入司机：调用后台插入司机接口
    :param wait_propelling_list:
    :return:
    """
    # result_dic = {'requestCompanyId': "C000062070", 'requestCompanyName': "会好运单车",
    #               'requestUserId': "U000050305", 'requestCompanyType': "GSLX30", 'requestUserSegmentId': None}
    # result_dic = {}
    # 推送记录的格式转换
    data = []
    if wait_propelling_list:
        # 单个推送对象的格式转换
        for wait_propelling in wait_propelling_list:
            tmp_dic = {"pickupNo": wait_propelling.pickup_no}

            # 转换司机列表格式
            driver_list = []
            for driver in wait_propelling.drivers:
                driver_dic = {
                    "driverId": driver.driver_id,
                    "driverTel": driver.driver_phone,
                    "driverName": driver.driver_name
                }
                driver_list.append(driver_dic)

            # 如果司机列表为空，不插入
            if len(driver_list) == 0:
                continue

            # 将司机列表加入对应的推送对象
            tmp_dic["driverList"] = driver_list
            data.append(tmp_dic)
    return data


def data_format_msg(wait_propelling_list: List[PickPropelling]):
    """
    格式转换：消息推送，调用后台短信推送接口
    :param wait_propelling_list:
    :return:
    """
    # result_dic = {'requestCompanyId': "C000062070", 'requestCompanyName': "会好运单车",
    #               'requestUserId': "U000050305", 'requestCompanyType': "GSLX30", 'requestUserSegmentId': None}
    # result_dic = {}
    # 推送记录的格式转换
    data = []
    if wait_propelling_list:
        # 单个推送对象的格式转换
        for wait_propelling in wait_propelling_list:
            tmp_dic = {
                "pickup_no": wait_propelling.pickup_no,
                "remainTruckNum": wait_propelling.remain_truck_num
            }

            # 转换司机列表格式
            driver_list = []
            for driver in wait_propelling.drivers:
                driver_dic = {
                    "userId": driver.driver_id,
                    "phoneNumber": driver.driver_phone
                }
                driver_list.append(driver_dic)
            # 如果司机列表为空，不插入
            if len(driver_list) == 0:
                continue
            # 将司机列表加入对应的推送对象
            tmp_dic["driver_list"] = driver_list
            data.append(tmp_dic)
    return data


def data_format_district(json_data):
    """
    模型生成十公里内司机集：数仓传入摘单计划信息，转换成PickPropelling格式
    :param json_data:
    :return:
    """
    # json_data格式转换
    if json_data:
        # json_data格式转换
        # for wait_propelling in json_data:
        pick_propelling_list = []
        pick_propelling = PickPropelling()
        pick_propelling.city_name = json_data["city"]
        pick_propelling.district_name = json_data["district"]
        pick_propelling.pickup_no = json_data["pickupNo"]
        pick_propelling.prod_name = json_data["prodName"]
        pick_propelling_list.append(pick_propelling)
        return pick_propelling_list

    return []


def data_format_driver(propelling_driver_list, total_count, current_count):
    """
    模型生成十公里内司机集：转换司机集格式，传给数仓
    :param propelling_driver_list:
    :param total_count:
    :param current_count:
    :return:
    """
    # result_dic = {'requestCompanyId': "C000062070", 'requestCompanyName': "会好运单车",
    #               'requestUserId': "U000050305", 'requestCompanyType': "GSLX30", 'requestUserSegmentId': None}
    # result_dic = {}
    # json_data格式转换
    # 推送记录的格式转换
    tmp_dic = {
        "currentCount": current_count,
        "totalCount": total_count
    }

    # 转换司机列表格式
    driver_list = []
    if propelling_driver_list:
        for propelling in propelling_driver_list:
            for driver in propelling.drivers:
                driver_dic = {
                    "driverId": driver.driver_id,
                    "driverName": driver.driver_name,
                    "driverTel": driver.driver_phone,
                    "district": driver.district_name
                }
                driver_list.append(driver_dic)
        # 将司机列表加入对应的推送对象
    tmp_dic["driverList"] = driver_list
    return tmp_dic


def data_format_truck(str_truck):
    """
    获取车辆位置信息：转换车辆集格式，调用中交兴路数据获取接口
    :param str_truck:
    :return:
    """
    # result_dic = {'requestCompanyId': "C000062070", 'requestCompanyName': "会好运单车",
    #               'requestUserId': "U000050305", 'requestCompanyType': "GSLX30", 'requestUserSegmentId': None}
    # result_dic = {}
    # json_data格式转换
    # 推送记录的格式转换
    tmp_dic = {"vclNs": str_truck}
    return tmp_dic


def data_format_truck_result(json_data):
    """
    获取车辆位置信息：中交兴路车辆位置信息，转换成PickPropellingDriver格式
    :param json_data:
    :return:
    """
    # result_dic = {'requestCompanyId': "C000062070", 'requestCompanyName': "会好运单车",
    #               'requestUserId': "U000050305", 'requestCompanyType': "GSLX30", 'requestUserSegmentId': None}
    # result_dic = {}
    # json_data格式转换
    if json_data and json_data['data']:
        # json_data格式转换
        # for wait_propelling in json_data:
        pick_propelling_list = []
        for data in json_data['data']:
            if not data["lon"] or not data["lat"]:
                continue
            pick_propelling = PickPropellingDriver()
            pick_propelling.vehicle_no = data["vno"]
            pick_propelling.longitude = data["lon"]
            pick_propelling.latitude = data["lat"]
            pick_propelling_list.append(pick_propelling)
        return pick_propelling_list

    return []
