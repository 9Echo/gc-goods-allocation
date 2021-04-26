# -*- coding: utf-8 -*-
# @Time    : 2020/11/26 11:01
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class PickPropellingFlowRelation(BaseEntity):
    """流向关联类"""

    def __init__(self, item=None):
        self.main_province = None  # 主省份
        self.main_city = None  # 主城市
        self.main_district = None  # 主区县
        self.correlation = 0.0  # 关联值
        self.related_province = None  # 关联省份
        self.related_city = None  # 关联城市
        self.related_district = None  # 关联区县
        if item:
            self.set_attr(item)

    def __lt__(self, other):
        return self.correlation > other.correlation



