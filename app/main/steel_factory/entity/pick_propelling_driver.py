# -*- coding: utf-8 -*-
# @Time    : 2020/11/17 11:01
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class PickPropellingDriver(BaseEntity):
    """待推送司机"""

    def __init__(self, item=None):
        self.pickup_no = None  # 摘单号
        self.pickup_prod_name = None  # 摘单品种
        self.prod_name = None  # 司机常运品种
        self.driver_id = None  # 司机id
        self.driver_name = None  # 司机姓名
        self.driver_phone = None  # 司机电话
        self.province_name = None  # 省份信息
        self.city_name = None  # 城市信息
        self.district_name = None  # 区县信息
        self.be_confirmed = 0  # 是否摘单
        self.pickup_start_time = None  # 摘单开始时间
        self.label_type = None  # 标签类型
        self.is_in_distance = 0  # 是否在推送范围内
        self.redis_date_time = None  # 存入redis的时间
        self.vehicle_no_list = None  # 车牌号列表
        self.vehicle_no = None  # 车牌号
        self.latitude = None  # 纬度
        self.longitude = None  # 经度
        self.location_flag = None
        self.dist = None  # 距离
        self.gps_create_date = None  # gps时间

        self.plan_weight = 0    # 司机当前已接调度单的总重量
        if item:
            self.set_attr(item)

    def __lt__(self, other):
        return self.dist < other.dist
