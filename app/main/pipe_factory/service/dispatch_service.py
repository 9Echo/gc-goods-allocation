# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 17:20
# @Author  : Zihao.Liu
# Modified: shaoluyu 2019/11/13
from flask import g
from app.main.pipe_factory.model.optimize_filter import optimize_filter_max, optimize_filter_min
from app.main.pipe_factory.model.spec_filter import spec_filter
from app.main.pipe_factory.model.weight_filter import weight_filter
from app.main.pipe_factory.rule.match_solution import match_solve
from app.main.pipe_factory.service.dispatch_load_task_service import dispatch_load_task
from app.util.aspect.method_before import get_item_a, param_init
from app.main.pipe_factory.service.combine_sheet_service import combine_sheets
from app.main.pipe_factory.service.create_delivery_item_service import CreateDeliveryItem
from app.main.pipe_factory.service.replenish_property_service import replenish_property
from app.util.uuid_util import UUIDUtil


@param_init
@get_item_a
def dispatch_spec(order):
    """根据订单执行分货
    """
    # 1、将订单项转为发货通知单子单的list
    delivery_item_list = CreateDeliveryItem(order)
    delivery_item_spec_list = delivery_item_list.spec()  # 调用spec()，即规格优先来处理
    # 2、使用模型过滤器生成发货通知单
    sheets = spec_filter(delivery_item_spec_list)
    # 3、补充发货单的属性
    batch_no = UUIDUtil.create_id("ba")
    replenish_property(sheets, order, batch_no, '00')
    # 4、为发货单分配车次
    dispatch_load_task(sheets)
    # 5、车次提货单按类合并
    combine_sheets(sheets)
    return sheets


def dispatch_weight(order):
    """根据订单执行分货
    """
    # 1、将订单项转为发货通知单子单的list
    g.LOAD_TASK_ID = 0
    delivery_item_list = CreateDeliveryItem(order)
    delivery_item_weight_list = delivery_item_list.weight()  # 调用weight()，即重量优先来处理
    # 排序，按重量倒序
    delivery_item_weight_list.sort(key=lambda x: x.weight, reverse=True)
    # 放入重量优先模型
    sheets = weight_filter(delivery_item_weight_list)
    # 3、补充发货单的属性
    batch_no = UUIDUtil.create_id("ba")
    replenish_property(sheets, order, batch_no, '10')
    # 归类合并
    combine_sheets(sheets, types='weight')
    return sheets


def dispatch_optimize(order):
    """根据订单执行分货
    """
    g.LOAD_TASK_ID = 0
    sheets = []
    batch_no = UUIDUtil.create_id("ba")
    # 1、将订单项转为发货通知单子单的list
    delivery_item_list = CreateDeliveryItem(order)
    # 根据历史数据匹配开单
    match_sheets = match_solve(order, delivery_item_list.delivery_item_list)
    replenish_property(match_sheets, order, batch_no, '20')
    # 调用optimize()，即将大小管分开
    max_delivery_items, min_delivery_items = delivery_item_list.optimize()
    if max_delivery_items:
        # 2、使用模型过滤器生成发货通知单
        sheets = optimize_filter_max(max_delivery_items)
        # 3、补充发货单的属性
        batch_no = UUIDUtil.create_id("ba")
        replenish_property(sheets, order, batch_no, '20')
        sheets.sort(key=lambda x: x.volume, reverse=True)
        # 4、为发货单分配车次
        dispatch_load_task(sheets, min_delivery_items)
    if min_delivery_items:
        optimize_filter_min(sheets, min_delivery_items, order, batch_no)
    # 车次提货单合并
    sheets = match_sheets + sheets
    combine_sheets(sheets)
    return sheets
