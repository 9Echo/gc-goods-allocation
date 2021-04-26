# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 16:14
# @Author  : Zihao.Liu
from threading import Thread
from app.main.pipe_factory.dao.order_dao import order_dao
from app.main.pipe_factory.entity.order import Order
from app.main.pipe_factory.entity.order_item import OrderItem
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from app.util.uuid_util import UUIDUtil


def generate_order(order_data):
    """根据json数据生成对应的订单"""
    order = Order(order_data)
    order.items = []
    order.order_no = UUIDUtil.create_id("order")
    order.truck_weight = order_data.get('truck_weight')
    for item in order_data['items']:
        oi = OrderItem(item)
        oi.order_no = order.order_no
        oi.quantity = 0 if int(oi.quantity) < 0 else int(oi.quantity)
        oi.free_pcs = 0 if int(oi.free_pcs) < 0 else int(oi.free_pcs)
        if oi.quantity >= 1000:
            raise MyException('物资代码' + oi.item_id + '输入件数过大！', ResponseCode.Error)
        order.items.append(oi)
    # 生成的订单入库
    Thread(target=order_dao.insert, args=(order,)).start()
    return order
