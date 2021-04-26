# -*- coding: utf-8 -*-
# Description: 摘单记录子类
# Created: jjunf 2020/10/12
from app.util.base.base_entity import BaseEntity


class PickTaskItem(BaseEntity):
    """
    摘单子类
    """

    def __init__(self):
        # 摘单记录号
        self.pick_id = None
        # 货源名称
        self.source_name = None
        # 重量
        self.total_weight = 0
        # 总车次数
        self.truck_num = 0
        # 每车的件数
        self.truck_count = None
        # 省
        self.province = None
        # 城市
        self.city = None
        # 区县
        self.end_point = None
        # 明细的装点库区
        self.deliware_district = None
        # 大品种(当有多个品种时：当货物都在一个厂区时，取重量最大的一个；当货物在多个厂区时，取重量最小的品种)
        self.big_commodity = None
        # 品种
        self.commodity = None
        # 注释
        self.remark = None
        # 车次记录子项
        self.items = []
