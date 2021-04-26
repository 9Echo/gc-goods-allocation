# -*- coding: utf-8 -*-
# @Time    :
# @Author  : shaoluyu
from app.util.base.base_entity import BaseEntity


class LoadTask(BaseEntity):
    """车次类"""

    def __init__(self):
        self.schedule_no = None  # 报道号
        self.car_mark = None  # 车牌号
        self.load_task_id = None  # 所属车次号
        self.load_task_type = None  # 装卸类型
        self.total_weight = 0  # 总重量
        self.count = 0  # 件数
        self.province = None  # 省
        self.city = None  # 城市
        self.end_point = None  # 区县
        self.price_per_ton = 0  # 车次吨单价
        self.total_price = 0  # 车次总价
        self.remark = None  # 注释
        self.priority_grade = None  # 优先级对应的ABCD等级
        self.items = []  # 子项
        self.latest_order_time = None  # 最新挂单时间
