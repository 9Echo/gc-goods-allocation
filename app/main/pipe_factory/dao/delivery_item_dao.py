# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 17:13
# @Author  : Zihao.Liu

from app.util.base.base_dao import BaseDao
from app.util.date_util import get_now_str


class DeliveryItemDao(BaseDao):

    def batch_insert(self, items):
        """批量插入子发货单"""
        sql = """insert into t_ga_delivery_item(
            delivery_no,
            delivery_item_no,
            product_type,
            item_id,
            spec,
            f_whs,
            f_loc,
            material,
            weight,
            quantity,
            free_pcs,
            create_date) value(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        if items:
            values = [(
                item.delivery_no,
                item.delivery_item_no,
                item.product_type,
                item.item_id,
                item.spec,
                item.f_whs,
                item.f_loc,
                item.material,
                item.weight,
                item.quantity,
                item.free_pcs,
                get_now_str()) for item in items]
            self.executemany(sql, values)
        return


delivery_item_dao = DeliveryItemDao()
