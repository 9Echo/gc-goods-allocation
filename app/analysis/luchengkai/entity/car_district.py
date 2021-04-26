# -*- coding: utf-8 -*-
# @Time    :
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class CarDistrict(BaseEntity):
    """车辆区县类"""

    def __init__(self):
        self.car_no = None  # 车牌号
        self.district_name = None  # 区县名
        self.travel_count = 0  # 运输次数
        self.score = 0.0  # 车辆流向评分

    def __lt__(self, other):
        return self.score > other.score
