import time
from typing import List

from geopy import distance

from app.main.steel_factory.dao.pick_propelling_dao import pick_propelling_dao
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.main.steel_factory.rule import pick_propelling_rule, pick_data_format_rule
from app.util.rest_template import RestTemplate
from model_config import ModelConfig


def pick_distance_deal(driver_list: List[PickPropellingDriver]):
    """
    司机当前距日钢距离
    距日钢越近，越容易获取摘单消息
    :return: driver_list
    """
    if not driver_list:
        return []
    # 查询司机实时位置信息列表(按创建时间降序排序)
    tmp_driver_id_list = []
    driver_gps_list = []
    for driver in driver_list:
        tmp_driver_id_list.append(driver.driver_id)
    driver_truck_list = pick_propelling_dao.select_driver_truck(tmp_driver_id_list)
    # 调用中交兴路车辆位置接口
    for i in range(0, len(driver_truck_list), 100):
        # 取前一百个
        tmp_list = driver_truck_list[i: i + 100]
        str_driver_truck = ','.join([tmp.vehicle_no for tmp in tmp_list])
        # 格式转换
        result_str = pick_data_format_rule.data_format_truck(str_driver_truck)
        url = "https://jcos.jczh56.com/purchase-api/vehicleMultiLocationFromJC"
        result_location = RestTemplate.do_post_for_jcos(url, result_str)
        # 格式转换
        driver_gps_list.extend(pick_data_format_rule.data_format_truck_result(result_location))
        time.sleep(1)
    # 循环司机对象列表，进行车牌经纬度赋值
    for driver in driver_list:
        driver.vehicle_no_list = [driver_truck.vehicle_no for driver_truck in driver_truck_list if
                                  driver_truck.driver_id == driver.driver_id]
        target_latitude = 0
        target_longitude = 0
        target_vehicle = None
        target_dist = 1000000
        for no in driver.vehicle_no_list:
            dgs = [dg for dg in driver_gps_list if dg.vehicle_no == no]
            if dgs:
                # 司机位置：纬度、经度
                driver_tuple = (float(dgs[0].latitude) / 600000, float(dgs[0].longitude) / 600000)
                # 日钢位置：纬度、经度
                rg_tuple = (
                    ModelConfig.PICK_LON_AND_LAT.get("日钢经纬度")[0], ModelConfig.PICK_LON_AND_LAT.get("日钢经纬度")[1])
                # 计算距离：米
                dist = distance.great_circle(driver_tuple, rg_tuple).m
                if dist < target_dist:
                    target_dist = dist
                    target_latitude = dgs[0].latitude
                    target_longitude = dgs[0].longitude
                    target_vehicle = dgs[0].vehicle_no
        driver.latitude = target_latitude
        driver.longitude = target_longitude
        driver.vehicle_no = target_vehicle
        driver.dist = target_dist
        if 0 < driver.dist <= 10000:
            driver.distance_to_rg = ModelConfig.PICK_DISTANCE_TO_RG.get("0-10")
        elif 10000 < driver.dist <= 20000:
            driver.distance_to_rg = ModelConfig.PICK_DISTANCE_TO_RG.get("10-20")
        elif 20000 < driver.dist <= 30000:
            driver.distance_to_rg = ModelConfig.PICK_DISTANCE_TO_RG.get("20-30")
        elif 30000 < driver.dist <= 40000:
            driver.distance_to_rg = ModelConfig.PICK_DISTANCE_TO_RG.get("30-40")
        elif 40000 < driver.dist <= 50000:
            driver.distance_to_rg = ModelConfig.PICK_DISTANCE_TO_RG.get("40-50")

    return driver_list


if __name__ == '__main__':
    test_driver_list = []

    driver = PickPropellingDriver()
    driver.driver_id = 'U000100063'  # 司机id
    test_driver_list.append(driver)

    driver = PickPropellingDriver()
    driver.driver_id = 'U000038291'  # 司机id
    test_driver_list.append(driver)

    driver = PickPropellingDriver()
    driver.driver_id = 'U000005376'  # 司机id
    test_driver_list.append(driver)

    driver = PickPropellingDriver()
    driver.driver_id = 'U000050902'  # 司机id
    test_driver_list.append(driver)

    pick_distance_deal(test_driver_list)

