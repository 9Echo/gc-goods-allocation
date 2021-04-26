# -*- coding: utf-8 -*-
# Description: 筛选待二次推送的摘单计划
# Created: luchengkai 2021/01/06
from typing import List
from datetime import datetime
from app.main.steel_factory.dao.pick_propelling_dao import pick_propelling_dao
from app.main.steel_factory.entity.pick_propelling import PickPropelling


def save_propelling_log(propelling_driver_list: List[PickPropelling]):
    """
    保存待推送摘单计划的司机列表
    :param propelling_driver_list:
    :return:
    """
    if not propelling_driver_list:
        return
    values = []
    create_date = datetime.now()
    for data in propelling_driver_list:
        for driver in data.drivers:
            item_tuple = (data.pickup_no,
                          data.prod_name,
                          data.start_point,
                          data.end_point,
                          data.city_name,
                          data.district_name,
                          data.total_truck_num,
                          data.remain_truck_num,

                          driver.driver_id,
                          driver.driver_phone,
                          driver.label_type,
                          create_date,

                          data.driver_type,
                          driver.latitude,
                          driver.longitude,
                          driver.dist,
                          driver.location_flag
                          )
            values.append(item_tuple)
    # 存在driver_list不为空，但是推荐司机为空的情况
    if values:
        pick_propelling_dao.save_pick_log(values)

# def save_pick_driver_log(wait_driver_list: List[PickPropellingDriver], total_count, current_count):
#     """
#     保存待推送摘单计划的司机列表
#     :param wait_driver_list:
#     :param total_count:
#     :param current_count:
#     :return:
#     """
#     if not wait_driver_list:
#         return
#     values = []
#     for driver in wait_driver_list:
#         create_date = datetime.now()
#         item_tuple = (driver.pickup_no,
#                       driver.driver_id,
#                       driver.driver_phone,
#                       driver.label_type,
#                       total_count,
#                       current_count,
#                       create_date
#                       )
#         values.append(item_tuple)
#     pick_propelling_dao.save_pick_driver_log(values)
