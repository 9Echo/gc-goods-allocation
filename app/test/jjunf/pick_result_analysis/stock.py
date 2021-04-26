# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/10
from app.util.base.base_entity import BaseEntity


class Stock(BaseEntity):
    """
    库存类
    """

    def __init__(self, stock=None):
        # 调度方式
        self.fs = None
        # 车次号
        self.trains_no = None
        # 重量
        self.plan_weight = None
        # 司机id
        self.driver_id = None
        # 车牌号
        self.vehicle_no = None
        # 备注
        self.remark = None
        # 城市
        self.city = None
        # 区县
        self.district = None
        # 品种
        self.commodity_name = None
        # 创建时间
        self.create_date = None
        # 摘单时间
        self.pickup_time = None
        # 摘单下发时间
        self.pick_create_date = None
        # 摘单时间段
        self.pick_time_interval = None
        # 货源名称
        self.source_name = None
        # 调度来源：DDLY40:手工调度，DDLY50:摘单调度（包括模型生成和人工添加）
        self.plan_source = None
        # 调度状态
        self.plan_status = None
        # 车辆类型：是否是推荐车辆
        self.vehicle_type = None
        # 是否跨厂区：是（跨厂区）、否（不跨厂区）
        self.two_factory = None
        # 模型生成备注
        self.model_remark = None
        # 模型生成时间
        self.model_create_data = None
        # 模型与实际的区别
        self.difference = None
        # 说明
        self.explain = None
        # 说明列表
        self.explain_list = []
        # 摘单号
        self.pickup_no = None
        # 是否相邻线路运力
        self.adjacent_transport = '否'
        # 是否消息推送后摘单
        self.message_propel = '否'
        # 摘单时的纬度
        self.latitude = None
        # 摘单时的经度
        self.longitude = None
        # 摘单时APP距离日钢的距离
        self.distance = None
        # 下发摘单时选择的司机来源
        self.driver_type = None


        if stock:
            self.set_attr(stock)


class HourStock(BaseEntity):
    """
    保存的小时库存类
    """

    def __init__(self, stock=None):
        # 仓库
        self.deliware_house = None
        # 省
        self.province = None
        # 城市
        self.city = None
        # 区县
        self.district = None
        # 品种
        self.big_commodity_name = None
        # 重量
        self.actual_weight = None
        # 最新挂单时间
        self.latest_order_time = None

        if stock:
            self.set_attr(stock)


class Gather(BaseEntity):
    """
    汇总结果类
    """

    def __init__(self, gather=None):
        # 调度来源
        self.str1 = None
        # 调度来源
        self.str2 = None
        # 城市
        self.city = None
        # 区县
        self.district = None
        # 品种
        self.commodity_name = None
        # 车次数
        self.number = None
        # 每个调度来源的车次数汇总
        self.total_number = None
        # 占比
        self.percent = 0

        if gather:
            self.set_attr(gather)


class Gather2(BaseEntity):
    """
    汇总结果类
    """

    def __init__(self, gather=None):
        # 城市
        self.city = None
        # 调度来源
        self.fs = None
        # 区县
        self.district = None
        # 品种
        self.commodity_name = None
        # 8点库存吨数
        self.stock_weight = 0
        # 库存所需车辆数
        self.need_truck_num = 0
        # 车次数
        self.truck_num = None
        # 车次数汇总
        self.total_number = None
        # 占比
        self.percent = None

        if gather:
            self.set_attr(gather)


class ModelPick(BaseEntity):
    """
    汇总结果类
    """

    def __init__(self, model_pick=None):
        # id
        self.pick_id = None
        # 总重量
        self.pick_total_weight = None
        # 总车次
        self.pick_truck_num = None
        # 备注
        self.remark = None
        # 城市
        self.city = None
        # 区县
        self.district = None
        # 品种
        self.commodity_name = None
        # 品种
        self.commodity_list = []
        # 创建时间
        self.create_date = None

        if model_pick:
            self.set_attr(model_pick)
