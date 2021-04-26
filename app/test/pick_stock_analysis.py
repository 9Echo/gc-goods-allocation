# -*- coding: utf-8 -*-
# @Time    : 2020/09/28
# @Author  : jjunf
from app.util.base.base_entity import BaseEntity


class Stock(BaseEntity):
    """
    Stock
    """
    def __init__(self, stock=None):
        self.schedule_no = None
        self.plan_weight = 0  # 运营开单重量
        self.remark = None

        self.main_no = None

        self.commodity_name = None
        self.outstock_code = None
        self.instock_code = None
        self.outstock_name = None
        self.instock_name = None
        self.xmkd_weight = 0  # 西门开单重量

        self.detail_address = None
        self.real_weight = 0  # 实际出库重量

        if stock:
            self.set_attr(stock)

        # self.plan_weight = float(self.plan_weight)
        # self.xmkd_weight = float(self.xmkd_weight)
        # self.real_weight = float(self.real_weight)
