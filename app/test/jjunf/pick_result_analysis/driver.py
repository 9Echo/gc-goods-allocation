# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/12
from app.util.base.base_entity import BaseEntity


class Driver(BaseEntity):
    """
    库存类
    """

    def __init__(self, driver=None):
        # 司机id
        self.user_id = None
        # 司机姓名
        self.name = None
        # 车牌号
        self.vehicle_no = None
        # 城市
        self.city = None
        # 区县
        self.district = None

        if driver:
            self.set_attr(driver)
