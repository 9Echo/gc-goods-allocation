# -*- coding: utf-8 -*-
# @Time    :
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class FreeTime(BaseEntity):
    """空闲时间段类"""

    def __init__(self):
        self.waybill_no = None  # 运单号
        self.stop_date = None  # 停车时间
        self.start_date = None  # 发车时间
