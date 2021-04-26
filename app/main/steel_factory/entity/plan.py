# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/11/11 16:03
from app.util.base.base_entity import BaseEntity


class Plan(BaseEntity):
    """
    摘单未开单调度单类
    """

    def __init__(self, plan=None):
        self.prodname = None
        self.city = None
        self.district_name = None
        self.plan_quantity = None
        self.trains_no = None
        if plan:
            self.set_attr(plan)
