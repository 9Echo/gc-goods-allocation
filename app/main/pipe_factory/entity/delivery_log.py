# -*- coding: utf-8 -*-
# @Time    : 2019/11/20 13:39
# @Author  : Zihao.Liu
from app.util.base.base_entity import BaseEntity


class DeliveryLog(BaseEntity):

    def __init__(self, delivery_log):
        self.rid = None                         # 主键id
        self.delivery_no = None                 # 发货单号
        self.delivery_item_no = None            # 子发货单号
        self.company_id = None  # 公司id
        self.op = None  # 操作标记 00:删除，01：插入，02：更新
        self.quantity_before = None             # 修改前数量
        self.quantity_after = None              # 修改后数量
        self.free_pcs_before = None             # 修改前散根数
        self.free_pcs_after = None              # 修改后散根数
        self.create_time = None
        self.update_time = None
        if delivery_log:
            self.set_attr(delivery_log)