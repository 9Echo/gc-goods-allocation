# -*- coding: utf-8 -*-
# @Time    :
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class ActualTrans(BaseEntity):
    """实时运输车辆类"""

    def __init__(self):
        self.waybill_no = None  # 运单号
        self.car_mark = None  # 车牌号
        self.out_warehouse_time = None  # 出库时间
        self.return_waybill_time = None  # 回单时间
        self.longitude = None  # 经度
        self.latitude = None  # 纬度
        self.create_date = None  # 该条数据创建时间
        self.company_id = None  # 公司编号
        self.plan_no = None  # 调度单号
        self.adr = None  # 车辆地理位置名称
        self.province = None  # 省
        self.country = None  # 县（区）
        self.drc = None  # 方向
        self.spd = None  # 速度（单位km/h）
