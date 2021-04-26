# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/23
from app.util.base.base_dao import BaseDao
import pandas as pd


class SelectDao(BaseDao):
    """
    备注查询数据访问层
    """

    def select_remark(self):
        """
        查询最近一个月的备注信息
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
                a.create_date,-- 	e.source_name,
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
                        db_trans.`t_plan` 
                    WHERE
                        create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                        AND NOW( ) 
                        AND business_moduleId = '020' 
                        AND plan_source IN ( 'DDLY40', 'DDLY50' ) 
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
                        db_trans.t_plan_item 
                    WHERE
                        create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                        AND NOW( ) 
                ) b,-- 	LEFT JOIN (
                -- 	SELECT
                -- 		order_no,
                -- 		pickup_no,
                -- 		driver_name,
                -- 		driver_id 
                -- 	FROM
                -- 		db_tender.t_pickup_order_driver 
                -- 	WHERE
                -- 		create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                -- 		AND NOW( ) 
                -- 	) p ON b.order_no = p.order_no
                -- 	LEFT JOIN db_tender.t_pickup_order e ON e.pickup_no = p.pickup_no,  -- 这样join太慢了，并且用不到e.source_name
                (
                    SELECT
                        plan_no,
                        driver_id,
                        vehicle_no 
                    FROM
                        db_trans.t_plan_driver 
                    WHERE
                        create_date BETWEEN date_sub( now( ), INTERVAL 1 MONTH ) 
                        AND NOW( ) 
                ) d,
                db_sys.t_point c 
            WHERE
                a.plan_no = b.plan_no 
                AND a.plan_no = d.plan_no 
                AND b.end_point = c.location_id 
                AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' ) --  LIMIT 100
            ORDER BY
                a.plan_source,-- 	e.source_name,
                c.city_name,
                c.district_name,
                b.prodname
        """
        #
        result = self.select_all(sql)
        return pd.DataFrame(result)


select_dao = SelectDao()
