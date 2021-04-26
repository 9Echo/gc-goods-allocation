# -*- coding: utf-8 -*-
# @Time    : 2020/12/1 14:41
# @Author  : luchengkai
from app.util.base.base_entity import BaseEntity


class PickBlackListDao(BaseEntity):
    """黑名单类"""

    def __init__(self, item=None):
        self.driver_id = None  # 司机id
        self.district_name = None  # 区县信息
        self.prod_name = None  # 常运品种
        self.ignore_count = 0  # 连续未点击列表次数
        self.update_time = None  # 数据更新时间
        if item:
            self.set_attr(item)
