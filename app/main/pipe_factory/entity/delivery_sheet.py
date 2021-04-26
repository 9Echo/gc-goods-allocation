# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 16:23
# @Author  : Zihao.Liu

from app.util.base.base_entity import BaseEntity


class DeliverySheet(BaseEntity):
    """提货单类"""

    def __init__(self, delivery_sheet=None):
        # self.rowid = None  # 主键id
        self.load_task_id = None  # 所属车次号
        self.car_mark = None  # 车牌号
        self.delivery_no = None  # 发货通知单号
        # self.company_id = None  # 公司id
        self.batch_no = None  # 批次号
        self.customer_id = None  # 客户id
        self.request_id = None  # 请求id
        self.salesorg_id = None  # 部门id
        self.salesman_id = None  # 业务员id
        self.total_pcs = None  # 总根数
        self.weight = None  # 重量
        self.type = None  # 类型
        self.volume = 0  # 所占体积
        self.items = []  # 发货通知单子单
        self.create_time = None  # 创建时间
        self.update_time = None  # 更新时间
        if delivery_sheet:
            self.set_attr(delivery_sheet)

    def as_dict(self):
        ignore_attr = ['car_mark', 'city', 'batch_no', 'volume', 'create_time', 'update_time']
        result_dict = super(DeliverySheet, self).as_dict()
        for i in ignore_attr:
            result_dict.pop(i, 404)
        return result_dict
