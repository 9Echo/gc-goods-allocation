# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 16:25
# @Author  : Zihao.Liu
from app.util.base.base_entity import BaseEntity


class LoadingItem(BaseEntity):
    """装配载子项类"""

    def __init__(self, loading_item=None):
        self.rowid = None  # 主键id
        self.product_type = None  # 产品类型
        self.quantity_size = None  # 件数规格(高*宽)
        self.free_pcs_size = None  # 散根规格(高*宽)
        self.item_id = None  # 物资代码
        self.quantity = None  # 件数
        self.free_pcs = None  # 散根数
        self.od_id=None         #外径
        self.pipe_length = None  # 管长
        self.shape = None  # 形状
        self.is_entity = None  # 是否实体(True or False)
        self.create_time = None  # 创建时间
        self.update_time = None  # 更新时间
        if loading_item:
            self.set_attr(loading_item)


