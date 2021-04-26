# -*- coding: utf-8 -*-
# @Time    : 2020/03/19 15:32
# @Author  : zhouwentao

from app.util.base.base_entity import BaseEntity


class LoadingFloor(BaseEntity):
    """管厂订单"""

    def __init__(self, loading_floor=None):
        self.rowid = None  # 主键id
        self.load_task_id = None  # 所属车次号
        self.left_width_in=None        # 内层剩余宽度
        self.left_width_out=None        # 外层剩余宽度
        self.floor=None        # 所属层数
        self.height_in=None     # 每层内侧高度
        self.height_out=None    # 每层外侧高度
        self.goods_in = []      # 内侧装配货物
        self.goods_out = []     # 外侧装配货物
        self.goods_list = []     # 装配的货物
        self.create_time = None # 创建时间
        self.update_time = None # 更新时间
        if loading_floor:
            self.set_attr(loading_floor)
