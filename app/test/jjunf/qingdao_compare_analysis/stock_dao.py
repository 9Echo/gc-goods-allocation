# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/24


from app.util.base.base_dao import BaseDao
import pandas as pd


class StockDao(BaseDao):
    """
    数据访问层               数仓正式环境
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
                    db_ads.kc_rg_product_can_be_send_amount 
                WHERE
                    CITY = '青岛市' 
                    AND ( CANSENDNUMBER > 0 OR NEED_LADING_NUM > 0 )
        """
        #
        result = self.select_all(sql)
        return result

    def select_result(self):
        """
        查询青岛每天的结果
        :return:
        """
        sql = """
                SELECT
                    a.trains_no,
                    b.plan_weight,
                    d.driver_id,
                    d.vehicle_no,
                    a.remark,
                    c.city_name AS city,
                    c.district_name AS district,
                    b.prodname AS commodity_name,
                    a.create_date,
                    a.plan_source,-- , a.plan_no, b.order_no, c.province_name AS province
                    a.plan_status 
                FROM
                    (
                    SELECT
                        plan_no,
                        trains_no,
                        plan_source,
                        plan_status,
                        remark,
                        carrier_company_name,
                        create_date 
                    FROM
                        db_trans_t_plan 
                    WHERE
                        create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                        AND NOW( ) 
                        AND business_moduleId = '020' 
                -- 		AND plan_source IN ( 'DDLY40', 'DDLY50' ) 
                        AND carrier_company_name = '会好运单车' 
                        AND plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT45' ) 
                    ) a,
                    (
                    SELECT
                        end_point,
                        order_no,
                        plan_weight,
                        plan_no,
                        prodname 
                    FROM
                        db_trans_t_plan_item 
                    WHERE
                        create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                        AND NOW( ) 
                    ) b,
                    (
                    SELECT
                        plan_no,
                        driver_id,
                        vehicle_no 
                    FROM
                        db_trans_t_plan_driver 
                    WHERE
                        create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                        AND NOW( ) 
                    ) d,
                    db_sys_t_point c 
                WHERE
                    a.plan_no = b.plan_no 
                    AND a.plan_no = d.plan_no 
                    AND b.end_point = c.location_id 
                    AND c.city_name = '青岛市'  --  LIMIT 1000
                    
                ORDER BY
                    a.plan_source,
                    c.city_name,
                    c.district_name,
                    b.prodname
        """
        #
        result = self.select_all(sql)
        return pd.DataFrame(result)


stock_dao = StockDao()
