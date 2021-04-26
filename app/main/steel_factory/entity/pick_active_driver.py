# -*- coding: utf-8 -*-
# @Time    : 2020/11/17 11:01
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class PickActiveDriver(BaseEntity):
    """三个月活跃司机"""

    def __init__(self, item=None):
        self.driver_id = None  # 司机id
        self.driver_name = None  # 司机姓名
        self.driver_phone = None    # 司机电话
        self.waybill_count = 0  # 司机三个月的运单数
        self.label_type = None  # 标签类型

        if item:
            self.set_attr(item)

