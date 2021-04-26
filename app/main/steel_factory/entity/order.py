# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 15:32
# @Author  : Zihao.Liu
# Modified: shaoluyu 2019/11/13

from app.util.base.base_entity import BaseEntity


class Order(BaseEntity):
    """管厂订单"""

    def __init__(self, order=None):
        self.rowid = None  # 主键id
        self.order_no = None  # 订单号
        self.items = []  # 订单子项
        self.company_id = None  # 公司id
        self.customer_id = None  # 客户id
        self.salesman_id = None  # 销售员id
        self.truck_weight = None  # 载重自定义
        self.create_time = None  # 创建时间
        self.update_time = None  # 更新时间
        if order:
            self.set_attr(order)
