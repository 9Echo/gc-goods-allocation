# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 17:12
# @Author  : Zihao.Liu

from app.util.base.base_dao import BaseDao
from app.util.date_util import get_now_str


class DeliverySheetDao(BaseDao):

    def batch_insert(self, sheets):
        """保存发货单"""
        sql = """insert into t_ga_delivery_sheet(
            request_id,
            load_task_id,
            delivery_no,
            salesorg_id,
            customer_id,
            salesman_id,
            weight,
            `type`,
            create_date) value(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        if sheets:
            values = [(
                sheet.request_id,
                sheet.load_task_id,
                sheet.delivery_no,
                sheet.salesorg_id,
                sheet.customer_id,
                sheet.salesman_id,
                sheet.weight,
                sheet.type,
                get_now_str()) for sheet in sheets]
            self.executemany(sql, values)
        return


delivery_sheet_dao = DeliverySheetDao()
