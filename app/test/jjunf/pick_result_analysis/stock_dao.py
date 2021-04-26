# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/03
from app.util.base.base_dao import BaseDao


class StockDao(BaseDao):
    """
    数据库访问层
    """

    def select_hour_stock(self,city,district,commodity,date):

        sql = """
            SELECT
                SUBSTRING_INDEX(DELIWAREHOUSE,'-',1) as deliware_house,
                BIG_COMMODITYNAME as big_commodity_name,
                PROVINCE as province, 
                CITY as city, 
                DLV_SPOT_NAME_END as district, 
                CANSENDNUMBER, 
                CANSENDWEIGHT, 
                NEED_LADING_NUM, 
                NEED_LADING_WT, 
                OVER_FLOW_WT, 
                LATEST_ORDER_TIME as latest_order_time
            FROM
                db_model.t_hour_stock_log
            WHERE 
                CITY = '{}'
                AND DLV_SPOT_NAME_END = '{}'
                AND BIG_COMMODITYNAME = '{}'
                AND save_date LIKE '{}%'
        """
        data = self.select_all(sql.format(city,district,commodity,date))
        return data


select_dao = StockDao()