# -*- coding: utf-8 -*-
# Description: 摘单记录类
# Created: jjunf 2020/10/15
from app.util.base.base_entity import BaseEntity


class PickTask(BaseEntity):
    """
    摘单类
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
        # 省
        self.province = None
        # 城市
        self.city = None
        # 区县
        self.end_point = None
        # 大品种
        self.big_commodity = None
        # 品种
        self.commodity = None
        # 注释
        self.remark = None
        # 跨厂区的标记：1 跨厂区，0 不跨厂区
        self.group_flag = False
        # 模板号：标识上一次摘单记录的标志(由城市、区县、备注编码组成)
        self.template_no = 0
        # 摘单子类
        self.items = []

        # 装点库区列表
        self.deliware_district = []
        # 各品种的件数:{品种：件数，...}
        self.commodity_count = {}
        # 各品种的重量:{品种：重量，...}
        self.commodity_weight = {}
        # 如果是滨州市的热镀锌钢卷，则值为：(包括热镀锌)
        self.bz_rdx = None
        # 如果是24h内的热卷，则值为：必须铁架子，
        self.hot_j = None
        # 卸点区县（主要是考虑到跨区县卸货时有多个区县）
        self.district_set = set()
