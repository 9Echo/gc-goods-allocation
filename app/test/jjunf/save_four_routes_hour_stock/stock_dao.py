# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/01
from app.util.base.base_dao import BaseDao


class StockDao(BaseDao):
    """
    数据访问层
    """

    def select_stock(self):
        """
        查询青岛的库存
        :return:
        """
        sql = """
                SELECT
                    NOTICENUM AS notice_num,
                    PURCHUNIT AS consumer,
                    ORITEMNUM AS oritem_num,
                    DEVPERIOD AS devperiod,
                    SUBSTRING_INDEX( DELIWAREHOUSE, '-', 1 ) AS deliware_house,
                    DELIWAREHOUSE AS deliware_house_name,
                    COMMODITYNAME AS commodity_name,
                    BIG_COMMODITYNAME AS big_commodity_name,
                    PACK AS pack,
                    MATERIAL AS mark,
                    STANDARD AS specs,
                    DELIWARE AS deliware,
                    CONTRACT_NO AS contract_no,
                    PORTNUM,
                    DETAILADDRESS AS detail_address,
                    PROVINCE AS province,
                    CITY AS city,
                    WAINTFORDELNUMBER AS waint_fordel_number,
                    WAINTFORDELWEIGHT AS waint_fordel_weight,
                    CANSENDNUMBER,
                    CANSENDWEIGHT,
                    DLV_SPOT_NAME_END AS dlv_spot_name_end,
                    PACK_NUMBER,
                    NEED_LADING_NUM,
                    NEED_LADING_WT,
                    OVER_FLOW_WT,
                    LATEST_ORDER_TIME AS latest_order_time,
                    PORT_NAME_END AS port_name_end,
                    priority,
                    concat( longitude, '_', latitude ) AS standard_address
                FROM
                    db_ods.db_inter_bclp_can_be_send_amount 
                WHERE
                    CITY IN ( '菏泽市', '济南市', '淄博市', '滨州市' )
                    AND ( CANSENDNUMBER > 0 OR NEED_LADING_NUM > 0 )
        """
        #
        result = self.select_all(sql)
        return result

    def save_stock_log(self, values):
        """
        保存原始库存记录
        :param values:
        :return:
        """
        sql = """
                insert into 
                db_model.t_hour_stock_log(
                                    stock_id,
                                    NOTICENUM,
                                    PURCHUNIT,
                                    ORITEMNUM,
                                    DEVPERIOD,
                                    DELIWAREHOUSE,
                                    COMMODITYNAME,
                                    BIG_COMMODITYNAME,
                                    PACK,
                                    MATERIAL,
                                    STANDARD,
                                    DELIWARE,
                                    CONTRACT_NO,
                                    PORTNUM,
                                    DETAILADDRESS,
                                    PROVINCE,
                                    CITY,
                                    WAINTFORDELNUMBER,
                                    WAINTFORDELWEIGHT,
                                    CANSENDNUMBER,
                                    CANSENDWEIGHT,
                                    DLV_SPOT_NAME_END,
                                    PACK_NUMBER,
                                    NEED_LADING_NUM,
                                    NEED_LADING_WT,
                                    OVER_FLOW_WT,
                                    LATEST_ORDER_TIME,
                                    PORT_NAME_END,
                                    priority,
                                    longitude,
                                    latitude,
                                    save_date,
                                    create_date
                                  )
                value( %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s
                      )
                """
        self.executemany(sql, values)


save_stock_dao = StockDao()
