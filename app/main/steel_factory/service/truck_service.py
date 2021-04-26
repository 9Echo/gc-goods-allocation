# -*- coding: utf-8 -*-
# Description: 车辆实例化服务
# Created: shaoluyu 2020/06/16

from app.main.steel_factory.entity.truck import Truck
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from model_config import ModelConfig


def generate_truck(json_data):
    """
    车辆信息实例化
    :param json_data:
    :return:
    """
    truck = Truck()
    truck.schedule_no = json_data['schedule_no']
    truck.car_mark = json_data.get('car_mark', None)
    truck.driver_id = json_data.get('driver_id', None)
    truck.trans_group_name = json_data.get('trans_group_name', None)
    truck.province = json_data.get('province', None)
    truck.city = json_data.get('city', None)
    truck.dlv_spot_name_end = json_data.get('dlv_spot_name_end', None)
    truck.big_commodity_name = json_data.get('big_commodity_name', None)
    truck.load_weight = int(float(json_data['load_weight']) * 1000)
    truck.remark = json_data.get('remark', None)
    if truck.dlv_spot_name_end:
        truck.actual_end_point = ModelConfig.RG_LY_GROUP.get(truck.dlv_spot_name_end, [truck.dlv_spot_name_end])
    return truck
