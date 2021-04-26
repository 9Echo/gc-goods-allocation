# -*- coding: utf-8 -*-
# @Time    : 2020/06/15
# @Author  : shaoluyu
from app.util.base.base_entity import BaseEntity


class Truck(BaseEntity):
    """
    车辆信息类
    """

    def __init__(self, truck=None):
        self.schedule_no = None  # 报道号
        self.car_mark = None  # 车牌号
        self.driver_id = None  # 司机id
        self.trans_group_name = None  # 车队名
        self.province = None  # 省份
        self.city = None  # 城市
        self.dlv_spot_name_end = None  # 终点（区县）
        self.big_commodity_name = None  # 大品名
        self.load_weight = 0  # 载重
        self.remark = None  # 备注信息(配件信息)
        self.actual_end_point = None  # 实际区县
        if truck:
            self.set_attr(truck)
