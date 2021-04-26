# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 17:10
# @Author  : Zihao.Liu
# Modified: shaoluyu 2019/11/13

from app.util.base.base_dao import BaseDao
from app.util.date_util import get_now_str


class OrderDao(BaseDao):

    def insert(self, order):
        # 保存订单
        sql = """insert into t_ga_order(
            request_id,
            order_no,
            company_id,
            salesorg_id,
            customer_id,
            salesman_id,
            create_date) value (%s,%s,%s,%s,%s,%s,%s)"""
        values = (
            order.request_id,
            order.order_no,
            order.company_id,
            order.salesorg_id,
            order.customer_id,
            order.salesman_id,
            get_now_str())
        self.execute(sql, values)
        # 保存订单项
        if order.items:
            sql = """ insert into t_ga_order_item(
                order_no,
                product_type,
                spec,
                quantity,
                free_pcs,
                item_id,
                material,
                f_whs,
                f_loc,
                create_time) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            values = [(
                item.order_no,
                item.product_type,
                item.spec,
                item.quantity,
                item.free_pcs,
                item.item_id,
                item.material,
                item.f_whs,
                item.f_loc,
                get_now_str()) for item in order.items]
            self.executemany(sql, values)


order_dao = OrderDao()
