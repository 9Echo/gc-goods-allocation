# -*- coding: utf-8 -*-
# @Time    :
# @Author  : shaoluyu
from app.util.base.base_entity import BaseEntity


class LoadTask(BaseEntity):
    """车次类"""

    def __init__(self):
        self.load_task_id = None  # 所属车次号
        # 优先级
        self.priority = 0
        # 装卸类型
        self.load_task_type = None
        # 总重量
        self.total_weight = 0
        # 发货通知单重量
        self.weight = 0
        # 发货通知单件数
        self.count = 0
        # 城市
        self.city = None
        # 区县
        self.end_point = None
        # 大品种
        self.big_commodity = None
        # 品种
        self.commodity = None
        # 发货通知单号
        self.notice_num = None
        # 订单项次号
        self.oritem_num = None
        # 规格
        self.standard = None
        # 材质
        self.sgsign = None
        # 出库仓库
        self.outstock_code = None
        # 入库仓库
        self.instock_code = None
        # 收货地址
        self.receive_address = None
        # 车次吨单价
        self.price_per_ton = 0
        # 车次总价
        self.total_price = 0
        # 注释
        self.remark = None
        # 父Id
        self.parent_load_task_id = None
        # 优先级对应的ABCD等级
        self.priority_grade = None
        # 最新挂单时间
        self.latest_order_time = None

