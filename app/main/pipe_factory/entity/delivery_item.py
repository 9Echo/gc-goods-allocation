# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 16:25
# @Author  : Zihao.Liu
from app.util.base.base_entity import BaseEntity


class DeliveryItem(BaseEntity):
    """提货单子项类"""

    def __init__(self, delivery_item=None):
        # self.rowid = None  # 主键id
        self.delivery_no = None  # 发货通知单主单号
        self.delivery_item_no = None  # 子单号
        self.order_num = None  # 订单号
        self.product_type = None  # 产品类型
        self.company_id = None  # 公司id
        self.spec = None  # 产品规格
        self.item_id = None  # 物资代码
        self.f_whs = None  # 仓库
        self.max_quantity = None  # 体积限制的物资装载最大件数
        self.volume = 0  # 所占体积
        self.f_loc = None  # 垛号
        self.material = None  # 材质
        self.weight = None  # 产品重量
        self.quantity = None  # 件数
        self.free_pcs = None  # 散根数
        self.total_pcs = None  # 总根数
        self.one_quantity_weight = None  # 单件重量
        self.one_free_pcs_weight = None  # 单根重量
        self.create_time = None  # 创建时间
        self.update_time = None  # 更新时间
        self.ext_info_map = None  # 携带参数
        if delivery_item:
            self.set_attr(delivery_item)

    def as_dict(self):
        ignore_attr = ['one_quantity_weight', 'one_free_pcs_weight', 'order_num', 'end_point', 'address',
                       'can_send_time', 'volume', 'create_time', 'update_time', 'max_quantity']
        result_dict = super(DeliveryItem, self).as_dict()
        for i in ignore_attr:
            result_dict.pop(i, 404)
        return result_dict
