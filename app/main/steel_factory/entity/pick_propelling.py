# -*- coding: utf-8 -*-
# @Time    : 2020/11/16
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity
from model_config import ModelConfig


class PickPropelling(BaseEntity):
    """待推送摘单计划表"""

    def __init__(self, item=None):
        self.pickup_no = None  # 摘单号
        self.prod_name = "未知品种"  # 摘单品种
        self.start_point = None  # 装货地
        self.end_point = None  # 卸货地
        self.city_name = None  # 卸货城市
        self.district_name = None  # 卸货区县
        self.total_truck_num = 0  # 总车次
        self.remain_truck_num = 1  # 剩余车次
        self.total_weight = 0  # 总重量
        self.remain_total_weight = 0  # 剩余总重量
        self.driver_type = None  # 司机传入方式
        self.pickup_start_time = None  # 摘单开始时间
        self.drivers = []  # 司机列表
        self.dist_type = ModelConfig.PICK_RESULT_TYPE.get("DEFAULT")
        if item:
            self.set_attr(item)
