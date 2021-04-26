# -*- coding: utf-8 -*-
# @Time    :
# @Author  : shaoluyu
from app.util.base.base_entity import BaseEntity


class LoadTaskItem(BaseEntity):
    """车次类"""

    def __init__(self):
        self.load_task_id = None  # 所属车次号
        # 优先级
        self.priority = 0
        # 发货通知单重量
        self.weight = 0
        # 发货通知单件数
        self.count = 0
        # 省
        self.province = None
        # 城市
        self.city = None
        # 区县
        self.end_point = None
        # 包装形式
        self.pack = None
        # 货源名称
        self.source_name = None
        # 大品种
        self.big_commodity = None
        # 品种
        self.commodity = None
        # 发货通知单号
        self.notice_num = None
        # 订单项次号
        self.oritem_num = None
        # 收货用户
        self.consumer = None
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
        # 父Id
        self.parent_load_task_id = None
        # 最新挂单时间
        self.latest_order_time = None
        # 库存号
        self.notice_stockinfo_id = None
        # 合同号
        self.contract_no = None
        #
        self.can_split_weight = 0
        #
        self.can_split_number = 0
        #
        self.source_number = None
