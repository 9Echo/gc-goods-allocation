# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/01

from app.main.steel_factory.entity.stock import Stock


class SaveStock(Stock):
    """
    库存类
    """

    def __init__(self, stock=None):
        super().__init__(stock)
        self.PORTNUM = None
        self.CANSENDNUMBER = None
        self.CANSENDWEIGHT = None
        self.PACK_NUMBER = None
        self.NEED_LADING_NUM = None
        self.NEED_LADING_WT = None
        self.OVER_FLOW_WT = None
        self.port_name_end = None

        if stock:
            self.set_attr(stock)
