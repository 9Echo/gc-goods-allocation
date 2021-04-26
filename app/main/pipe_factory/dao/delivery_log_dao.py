# -*- coding: utf-8 -*-
# @Time    : 2019/11/19 14:12
# @Author  : shaoluyu

from app.util.base.base_dao import BaseDao
from app.util.date_util import get_now_str


class DeliveryLogDao(BaseDao):

    def insert(self, log_list):

        sql = """
            insert into db_trans_plan.t_ga_delivery_log(
            company_id,
            delivery_no,
            delivery_item_no,
            op,
            quantity_before,
            quantity_after,
            free_pcs_before,
            free_pcs_after,
            create_time) value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        if log_list:
            values = [tuple([
                item.company_id,
                item.delivery_no,
                item.delivery_item_no,
                item.op,
                item.quantity_before,
                item.quantity_after,
                item.free_pcs_before,
                item.free_pcs_after,
                get_now_str()])for item in log_list]
            self.executemany(sql, values)



delivery_log_dao=DeliveryLogDao()