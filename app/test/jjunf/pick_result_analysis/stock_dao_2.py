# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/10
from app.test.jjunf.pick_result_analysis.get_date import get_date_before_830
from app.test.jjunf.pick_result_analysis.get_date_2 import get_one_day_before_now_ymd, get_one_day_before_now
from app.test.jjunf.pick_result_analysis.get_date_2 import get_the_day_now
from app.test.jjunf.pick_result_analysis.pick_analysis_config import PickAnalysisConfig
from app.test.jjunf.pick_result_analysis.stock import Stock
from app.util.base.base_dao_2 import BaseDao2
import pandas as pd
from datetime import datetime

"""
# # 在数据库中查询情况(修改4处的时间create_date)
# SELECT
# 	a.trains_no,
# 	b.plan_weight,
# 	d.driver_id,
# 	d.vehicle_no,
# 	a.remark,
# 	c.city_name AS city,
# 	c.district_name AS district,
# 	b.prodname AS commodity_name,
# 	a.create_date,
# 	p.pickup_time,
# 	e.create_date AS pick_create_date,
# 	CONCAT_WS( '__', e.pickup_start_time, e.pickup_end_time ) AS pick_time_interval,
# 	e.source_name,
#   e.driver_type,
# 	a.plan_source,-- , a.plan_no, b.order_no, c.province_name AS province
# 	a.plan_status,
# 	p.pickup_no 
# FROM
# 	(
# 	SELECT
# 		plan_no,
# 		trains_no,
# 		plan_source,
# 		plan_status,
# 		remark,
# 		carrier_company_name,
# 		create_date 
# 	FROM
# 		db_ods.`db_trans_t_plan` 
# 	WHERE
# 		create_date LIKE '2021-01-11%' 
# 		AND business_moduleId = '020' 
# 		AND plan_source IN ( 'DDLY40', 'DDLY50' ) 
# 		AND carrier_company_name = '会好运单车' 
# 		AND plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT42', 'DDZT45' ) 
# 	) a
# 	LEFT JOIN ( SELECT order_no, pickup_no, driver_name, pickup_time, driver_id FROM db_ods.db_tender_t_pickup_order_driver WHERE create_date LIKE '2021-01-11%' AND order_no is NOT NULL) p ON p.order_no RLIKE a.plan_no
# 	LEFT JOIN db_ods.db_tender_t_pickup_order e ON e.pickup_no = p.pickup_no,
# 	( SELECT end_point, order_no, plan_weight, plan_no, prodname FROM db_ods.db_trans_t_plan_item WHERE create_date LIKE '2021-01-11%' ) b,
# 	( SELECT plan_no, driver_id, vehicle_no FROM db_ods.db_trans_t_plan_driver WHERE create_date LIKE '2021-01-11%' ) d,--  db_trans.t_plan_driver d,
# 	db_ods.db_sys_t_point c 
# WHERE
# 	a.plan_no = b.plan_no 
# 	AND a.plan_no = d.plan_no 
# 	AND b.end_point = c.location_id 
# 	AND c.city_name IN ( '菏泽市', '济南市', '滨州市', '淄博市' ) --  LIMIT 100
# 
# ORDER BY
# 	a.plan_source,
# 	e.source_name,
# 	c.city_name,
# 	c.district_name,
# 	b.prodname --  a.trains_no, c.province_name,
"""


class SelectDao(BaseDao2):
    """
    第二个数据访问层
    """

    def select_pick_result(self):
        """
        查询模型生成、人工录入和人工调度的详细信息
        :return:
        """
        """
        注：1.2020年12月23日晚上新上线了摘单的项目，将db_ods.db_tender_t_pickup_order_driver
              表中的order_no字段取值为【调度单号】（可由db_ods.db_trans_t_plan表关联）
            2.在此之前（2020年12月9日及以前）order_no字段取值为【委托单号】（可由db_ods.db_trans_t_plan_item表关联）
            3.2020年12月10日——2020年12月23日 由于需要检查车辆的4证，不走摘单，db_ods.db_tender_t_pickup_order_driver表中无数据
        """
        '''此sql适用于2020年12月9日及以前数据的查询'''
        # sql = """
        #         SELECT
        #             a.trains_no,
        #             b.plan_weight,
        #             d.driver_id,
        #             d.vehicle_no,
        #             a.remark,
        #             c.city_name AS city,
        #             c.district_name AS district,
        #             b.prodname AS commodity_name,
        #             a.create_date,
        #             p.pickup_time,
        #             e.create_date as pick_create_date,
        #             CONCAT_WS('__',e.pickup_start_time,e.pickup_end_time) as pick_time_interval,
        #             e.source_name,
        #             a.plan_source,	-- , a.plan_no, b.order_no, c.province_name AS province
        #             a.plan_status,
        #             p.pickup_no
        #         FROM
        #             (
        #                 SELECT
        #                     plan_no,
        #                     trains_no,
        #                     plan_source,
        #                     plan_status,
        #                     remark,
        #                     carrier_company_name,
        #                     create_date
        #                 FROM
        #                     db_ods.`db_trans_t_plan`
        #                 WHERE
        #                     date_sub( '{}', INTERVAL {} DAY ) <= create_date
        #                     AND create_date < date_sub( '{}', INTERVAL 0 DAY )
        #                     AND business_moduleId = '020'
        #                     AND plan_source IN ('DDLY40','DDLY50' )
        #                     AND carrier_company_name = '会好运单车'
        #                     AND plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT42', 'DDZT45' )
        #             ) a,
        #             (
        #                 SELECT
        #                     end_point,
        #                     order_no,
        #                     plan_weight,
        #                     plan_no,
        #                     prodname
        #                 FROM
        #                     db_ods.db_trans_t_plan_item
        #                 WHERE
        #                     date_sub( '{}', INTERVAL {} DAY ) <= create_date
        #                     AND create_date < date_sub( '{}', INTERVAL 0 DAY )
        #             ) b LEFT JOIN (
        #                                 SELECT
        #                                     order_no,
        #                                     pickup_no ,
        #                                     driver_name,
        #                                     pickup_time,
        #                                     driver_id
        #                                 FROM
        #                                     db_ods.db_tender_t_pickup_order_driver
        #                                 WHERE
        #                                     date_sub( '{}', INTERVAL {} DAY ) <= create_date
        #                                     AND create_date < date_sub( '{}', INTERVAL 0 DAY )
        #                             ) p ON b.order_no = p.order_no
        #                 LEFT JOIN db_ods.db_tender_t_pickup_order e ON e.pickup_no = p.pickup_no ,
        #             (
        #                 SELECT
        #                     plan_no,
        #                     driver_id,
        #                     vehicle_no
        #                 FROM
        #                     db_ods.db_trans_t_plan_driver
        #                 WHERE
        #                     date_sub( '{}', INTERVAL {} DAY ) <= create_date
        #                     AND create_date < date_sub( '{}', INTERVAL 0 DAY )
        #             ) d,
        #             --  db_trans.t_plan_driver d,
        #             db_ods.db_sys_t_point c
        #         WHERE
        #             a.plan_no = b.plan_no
        #             AND a.plan_no = d.plan_no
        #             AND b.end_point = c.location_id
        #             AND c.city_name IN {}
        #             --  LIMIT 100
        #         ORDER BY
        #             a.plan_source,
        #             e.source_name,
        #             c.city_name,
        #             c.district_name,
        #             b.prodname	--  a.trains_no, c.province_name,
        # """
        '''此sql适用于2020年12月24日及以后数据的查询'''
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
                    p.pickup_time,
                    e.create_date as pick_create_date,
                    CONCAT_WS('__',e.pickup_start_time,e.pickup_end_time) as pick_time_interval,
                    e.source_name, 
                    e.driver_type,
                    a.plan_source,	-- , a.plan_no, b.order_no, c.province_name AS province
                    a.plan_status,
                    p.latitude,
                    p.longitude,
                    p.pickup_no
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
                            db_ods.`db_trans_t_plan` 
                        WHERE 
                            date_sub( '{}', INTERVAL {} DAY ) <= create_date 
                            AND create_date < date_sub( '{}', INTERVAL 0 DAY ) 
                            AND business_moduleId = '020' 
                            AND plan_source IN ('DDLY40','DDLY50' )
                            AND carrier_company_name = '会好运单车' 
                            AND plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT42', 'DDZT45' )
                    ) a LEFT JOIN (
                                        SELECT	
                                            order_no,	
                                            pickup_no ,	
                                            driver_name,	
                                            pickup_time,
                                            driver_id,
                                            latitude,
                                            longitude
                                        FROM	
                                            db_ods.db_tender_t_pickup_order_driver 
                                        WHERE	
                                            date_sub( '{}', INTERVAL {} DAY ) <= create_date 
                                            AND create_date < date_sub( '{}', INTERVAL 0 DAY ) 
                                            AND order_no is NOT NULL
                                    ) p ON p.order_no RLIKE a.plan_no
                        LEFT JOIN db_ods.db_tender_t_pickup_order e ON e.pickup_no = p.pickup_no ,
                    (
                        SELECT 
                            end_point, 
                            order_no, 
                            plan_weight, 
                            plan_no, 
                            prodname 
                        FROM 
                            db_ods.db_trans_t_plan_item 
                        WHERE 
                            date_sub( '{}', INTERVAL {} DAY ) <= create_date 
                            AND create_date < date_sub( '{}', INTERVAL 0 DAY ) 
                    ) b,
                    ( 
                        SELECT 
                            plan_no,
                            driver_id,
                            vehicle_no
                        FROM 
                            db_ods.db_trans_t_plan_driver 
                        WHERE	
                            date_sub( '{}', INTERVAL {} DAY ) <= create_date
                            AND create_date < date_sub( '{}', INTERVAL 0 DAY ) 
                    ) d,
                    --  db_trans.t_plan_driver d,
                    db_ods.db_sys_t_point c 
                WHERE
                    a.plan_no = b.plan_no 
                    AND a.plan_no = d.plan_no 
                    AND b.end_point = c.location_id 
                    AND c.city_name IN {}
                    --  LIMIT 100
                ORDER BY 
                    a.plan_source,
                    e.source_name, 
                    c.city_name, 
                    c.district_name, 
                    a.trains_no,
                    b.prodname	--  a.trains_no, c.province_name,
        """
        # 如果需要查询淄博的
        if PickAnalysisConfig.ZIBO_FLAG:
            city = ('菏泽市', '济南市', '滨州市', '淄博市')
        # 不查询淄博的
        else:
            city = ('菏泽市', '济南市', '滨州市')
        #
        result = self.select_all(sql.format(get_the_day_now(), PickAnalysisConfig.DAY_TOTAL, get_the_day_now(),
                                            get_the_day_now(), PickAnalysisConfig.DAY_TOTAL + 2, get_the_day_now(),
                                            get_the_day_now(), PickAnalysisConfig.DAY_TOTAL + 2, get_the_day_now(),
                                            get_the_day_now(), PickAnalysisConfig.DAY_TOTAL, get_the_day_now(),
                                            city
                                            )
                                 )
        return pd.DataFrame(result)

    def select_driver(self):
        """
        查询推荐司机信息
        :return:
        """
        sql = """
                SELECT DISTINCT
                    u.user_id,	
                    u.`name`, 
                    d.vehicle_no,	
                    p.city_name as city,	
                    p.district_name as district
                FROM
                    db_ods.db_sys_t_company_user cu
                    LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
                    LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
                    LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
                    LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point 
                WHERE
                    u.user_company_type = 'YHLX20' 
                    AND cu.company_id = 'C000062070' 
                    AND u.auth_status = 'RZZT90' 
                    AND u.STATUS = 'YHZT10' 
                    AND p.province_name RLIKE '山东' 
                    AND p.city_name in ( '菏泽市', '济南市', '淄博市', '滨州市' ) 
                    AND w.load_date BETWEEN date_sub( '{}', INTERVAL 6 MONTH ) AND '{}'
                ORDER BY 
                    p.city_name,
                    p.district_name DESC,
                    u.user_id
        """

        sql_time = get_one_day_before_now()
        #
        result = self.select_all(sql.format(sql_time, sql_time))
        return pd.DataFrame(result)

    def select_model_pick(self):
        """
        查询模型生成的摘单记录
        :return:
        """
        sql = """
                SELECT DISTINCT
                    pick_id,
                    pick_total_weight,
                    pick_truck_num,
                    remark,
                    city,
                    end_point as district,
                    big_commodity as commodity_name,
                    create_date
                FROM
                    db_model.`t_pick_log` 
                WHERE
                    load_task_id LIKE '{}%' 
                    AND pick_truck_num != - 1 
                ORDER BY
                    city,
                    end_point,
                    pick_truck_num,
                    pick_total_weight
        """
        sql_time = datetime.strftime(get_one_day_before_now_ymd(), "%Y%m%d")
        #
        result = self.select_all(sql.format(sql_time))
        return pd.DataFrame(result)

    def select_pick_concat(self):
        """
        查询模型摘单分货记录中的 城市，区县，品种，来判断库存中是否存在货物
        :return:
        """
        sql = """
                SELECT DISTINCT
                    CONCAT_WS( ',', city, end_point, big_commodity ) AS pick_concat 
                FROM
                    db_model.`t_pick_log` 
                WHERE
                    load_task_id LIKE '{}%' 
                ORDER BY
                    pick_concat
        """
        sql_time = datetime.strftime(get_one_day_before_now_ymd(), "%Y%m%d")
        #
        result = self.select_all(sql.format(sql_time))
        # 城市，区县，品种  拼接的列表
        pick_concat_list = []
        for item in result:
            pick_concat_list.extend(item.values())
        return pick_concat_list

    def select_pick_remark(self, city, end_point, big_commodity):
        """
        按照 城市，区县，品种 查询模型摘单分货记录中的 备注信息
        :param city:
        :param end_point:
        :param big_commodity:
        :return:
        """
        sql = """
                SELECT DISTINCT
                    remark
                FROM
                    db_model.`t_pick_log` 
                WHERE
                    city = '{}' 
                    AND end_point='{}'
                    AND big_commodity = '{}'
                    AND load_task_id LIKE '{}%' 
                    AND pick_truck_num != -1
                ORDER BY
                    remark
        """
        sql_time = datetime.strftime(get_one_day_before_now_ymd(), "%Y%m%d")
        #
        result = self.select_all(sql.format(city, end_point, big_commodity, sql_time))
        # 备注列表
        remark_list = []
        for item in result:
            remark_list.extend(item.values())
        return remark_list

    def select_model_create_data(self):
        """
        查询模型生成时间
        :return:
        """
        sql = """
                SELECT DISTINCT
                    create_date
                FROM
                    db_model.`t_pick_log` 
                WHERE
                    load_task_id LIKE '{}%' 
        """
        sql_time = datetime.strftime(get_one_day_before_now_ymd(), "%Y%m%d")
        #
        result = self.select_one(sql.format(sql_time))
        #
        re = get_date_before_830()
        for k, v in result.items():
            re = v
        re = datetime.strptime(re.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
        return re

    def select_stock_weight_8(self, city, district, commodity_name):
        """
        查询8点的库存吨数
        :return:
        """
        sql = """
                    SELECT
                        SUM( weight )  as weight
                    FROM
                        `t_pick_log` 
                    WHERE
                        create_date LIKE '{}%' 
                        AND city = '{}' 
                        AND end_point = '{}' 
                        AND big_commodity = '{}' 
        """
        # 时间
        sql_time = datetime.strftime(get_one_day_before_now_ymd(), "%Y-%m-%d 08")
        result = self.select_all(sql.format(sql_time, city, district, commodity_name))
        # 返回查询的重量
        if result[0]['weight']:
            return result[0]['weight']
        else:
            return 0

    def select_send(self):
        """
        查询人工调度派单的
        :return:
        """
        sql = """
                SELECT
                    a.plan_no,
                    a.plan_source,
                    a.plan_status,
                    b.order_no,
                    a.remark,
                    c.province_name as province,
                    c.city_name as city,
                    c.district_name as district,
                    b.prodname as commodity_name,
                    a.create_date
                    
                FROM
                    (
                        SELECT
                            plan_no,
                            plan_source,
                            plan_status,
                            remark,
                            carrier_company_name,
                            create_date 
                        FROM
                            db_trans.`t_plan` 
                        WHERE
                            date_sub(curdate(),interval 1 day) <= create_date
                            and create_date < date_sub(curdate(),interval 0 day)
                            AND business_moduleId = '020' 
                    ) a,
                    ( 
                        SELECT 
                            end_point, 
                            order_no, 
                            plan_no, 
                            prodname 
                        FROM 
                            db_trans.t_plan_item 
                        WHERE 
                            date_sub(curdate(),interval 1 day) <= create_date
                            and create_date < date_sub(curdate(),interval 0 day)
                    ) b,
                    db_sys.t_point c 
                    
                WHERE
                    a.plan_no = b.plan_no 
                    and a.plan_source = 'DDLY40'
                    AND a.carrier_company_name = '会好运单车' 
                    AND a.plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT42', 'DDZT45' ) 
                    AND b.end_point = c.location_id 
                    AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' ) 
                ORDER BY
                    c.province_name,
                    c.city_name,
                    c.district_name,
                    b.prodname
            """
        #
        result = self.select_all(sql)
        if result:
            send_list = [Stock(row) for row in result]
            return send_list
        return []

    def select_pick22(self):
        """
        查询通过摘单功能派单的
        :return:
        """
        sql = """
                SELECT
                    a.plan_no,
                    a.plan_source,
                    a.plan_status,
                    b.order_no,
                    a.remark,
                    c.province_name as province,
                    c.city_name as city,
                    c.district_name as district,
                    b.prodname as commodity_name,
                    e.source_name,
                    e.create_date

                FROM
                    (
                        SELECT
                            plan_no,
                            plan_source,
                            plan_status,
                            remark,
                            carrier_company_name,
                            create_date 
                        FROM
                            db_trans.`t_plan` 
                        WHERE
                            date_sub(curdate(),interval 1 day) <= create_date
                            and create_date < date_sub(curdate(),interval 0 day)
                            AND business_moduleId = '020' 
                    ) a,
                    ( 
                        SELECT 
                            end_point, 
                            order_no, 
                            plan_no, 
                            prodname 
                        FROM 
                            db_trans.t_plan_item 
                        WHERE 
                            date_sub(curdate(),interval 1 day) <= create_date
                            and create_date < date_sub(curdate(),interval 0 day)
                    ) b,
                    db_sys.t_point c ,
                    (
                        select 
                            order_no,
                            pickup_no
                        from 
                            db_tender.t_pickup_order_driver 
                        WHERE 
                            date_sub(curdate(),interval 1 day) <= create_date
                            and create_date < date_sub(curdate(),interval 0 day)
                    ) d,
                    db_tender.t_pickup_order e

                WHERE
                    a.plan_no = b.plan_no 
                    and a.plan_source = 'DDLY50'
                    AND a.carrier_company_name = '会好运单车' 
                    AND a.plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT42', 'DDZT45' ) 
                    AND b.end_point = c.location_id 
                    AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' ) 
                    and b.order_no = d.order_no
                    and e.pickup_no = d.pickup_no
                ORDER BY
                    e.source_name,
                    c.province_name,
                    c.city_name,
                    c.district_name,
                    b.prodname
            """
        #
        result = self.select_all(sql)
        if result:
            pick_list = [Stock(row) for row in result]
            return pick_list
        return []


select_dao_2 = SelectDao()

if __name__ == '__main__':
    print(select_dao_2.select_stock_weight_8('淄博市', '淄川区', '老区-卷板'))

# # 司机历史6个月运输区县统计（同城市的区县合并一起）
# sql = """
# SELECT
# 	user_id,
# 	`name`,
# 	city_name,
# 	GROUP_CONCAT( district_name )
# FROM
# 	(
# 	SELECT DISTINCT
# 		u.user_id,
# 		u.`name`,
# 		p.city_name,
# 		p.district_name
# 	FROM
# 		db_ods.db_sys_t_company_user cu
# 		LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
# 		LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
# 		LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
# 		LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point
# 	WHERE
# 		u.user_company_type = 'YHLX20'
# 		AND cu.company_id = 'C000062070'
# 		AND u.auth_status = 'RZZT90'
# 		AND u.STATUS = 'YHZT10'
# 		AND p.province_name RLIKE '山东'
# 		AND w.load_date BETWEEN date_sub( now( ), INTERVAL 6 MONTH )
# 		AND NOW( ) --                 AND w.load_date LIKE '2020-11-10%'
#
# 	)
# GROUP BY
# 	user_id,
# 	city_name
# """
# '''需要将里面的now( )改为相应的时间'''
# # 司机历史6个月运输区县统计
# sql2 = """
# SELECT DISTINCT
# 		u.user_id,
# 		u.`name`,
# 		d.vehicle_no,
# 		p.city_name,
# 		p.district_name
# 	FROM
# 		db_ods.db_sys_t_company_user cu
# 		LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
# 		LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
# 		LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
# 		LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point
# 	WHERE
# 		u.user_company_type = 'YHLX20'
# 		AND cu.company_id = 'C000062070'
# 		AND u.auth_status = 'RZZT90'
# 		AND u.STATUS = 'YHZT10'
# 		AND p.province_name RLIKE '山东'
# 		AND p.city_name in ( '菏泽市', '济南市', '淄博市', '滨州市' )
# 		AND w.load_date BETWEEN date_sub( now( ), INTERVAL 6 MONTH )
# 		AND NOW( )
# 		ORDER BY 4,3,1
# """
#
#
# # 摘单情况汇总统计
# sql3 = """
# SELECT
#  a.trains_no, b.plan_weight, d.driver_id, d.vehicle_no, a.remark, c.city_name AS city, c.district_name AS district, b.prodname AS commodity_name, a.create_date,e.source_name, a.plan_source	-- , a.plan_no, a.plan_status, b.order_no, c.province_name AS province
# FROM
#  (SELECT plan_no, trains_no, plan_source, plan_status, remark, carrier_company_name, create_date
# FROM db_trans.`t_plan`
# WHERE date_sub( curdate( ), INTERVAL 3 DAY ) <= create_date
#  AND create_date < date_sub( curdate( ), INTERVAL 2 DAY )
#  AND business_moduleId = '020'
#  ) a,
#  (SELECT end_point, order_no, plan_weight, plan_no, prodname
# FROM db_trans.t_plan_item
# WHERE date_sub( curdate( ), INTERVAL 3 DAY ) <= create_date
#  AND create_date < date_sub( curdate( ), INTERVAL 2 DAY )
#  ) b LEFT JOIN
#  (	SELECT	order_no,	pickup_no ,	driver_name,	driver_id
# 	FROM	db_tender.t_pickup_order_driver
# 	WHERE	date_sub( curdate( ), INTERVAL 3 DAY ) <= create_date
# 		AND create_date < date_sub( curdate( ), INTERVAL 2 DAY )
# 	) p ON b.order_no = p.order_no
#  LEFT JOIN 	db_tender.t_pickup_order e ON e.pickup_no = p.pickup_no ,
#  ( SELECT plan_no,driver_id,vehicle_no
# 	FROM db_trans.t_plan_driver
# 	WHERE	date_sub( curdate( ), INTERVAL 3 DAY ) <= create_date
# 		AND create_date < date_sub( curdate( ), INTERVAL 2 DAY )
#  ) d,
# --  db_trans.t_plan_driver d,
#  db_sys.t_point c
# WHERE
#  a.plan_no = b.plan_no
#  AND a.plan_source IN ('DDLY40','DDLY50' )
#  AND a.carrier_company_name = '会好运单车'
#  AND a.plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT45' )
#  and a.plan_no = d.plan_no
#  AND b.end_point = c.location_id
#  AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' )
# --  LIMIT 100
# ORDER BY a.plan_source,e.source_name, c.city_name, c.district_name, b.prodname	--  a.trains_no, c.province_name,
#     """
#
#
#
#
#
#
#
# # 司机查询
# sql_4 = """
#         SELECT
#             a.plan_no,
#             a.plan_source,
#             a.plan_status,
#             b.order_no,
#             a.remark,
#             d.driver_id,
#             d.driver_name,
#             c.province_name AS province,
#             c.city_name AS city,
#             c.district_name AS district,
#             b.prodname AS commodity_name,
#             e.create_date
#         FROM
#             (
#             SELECT
#                 plan_no,
#                 plan_source,
#                 plan_status,
#                 remark,
#                 carrier_company_name,
#                 create_date
#             FROM
#                 db_trans.`t_plan`
#             WHERE
#                 date_sub( curdate( ), INTERVAL 2 DAY ) <= create_date
#                 AND create_date < date_sub( curdate( ), INTERVAL 1 DAY )
#                 AND business_moduleId = '020'
#             ) a,
#             (
#             SELECT
#                 end_point,
#                 order_no,
#                 plan_no,
#                 prodname
#             FROM
#                 db_trans.t_plan_item
#             WHERE
#                 date_sub( curdate( ), INTERVAL 2 DAY ) <= create_date
#                 AND create_date < date_sub( curdate( ), INTERVAL 1 DAY )
#             ) b,
#             db_sys.t_point c,
#             (
#             SELECT
#                 order_no,
#                 pickup_no,
#                 driver_name,
#                 driver_id
#             FROM
#                 db_tender.t_pickup_order_driver
#             WHERE
#                 date_sub( curdate( ), INTERVAL 2 DAY ) <= create_date
#                 AND create_date < date_sub( curdate( ), INTERVAL 1 DAY )
#             ) d,
#             db_tender.t_pickup_order e
#         WHERE
#             a.plan_no = b.plan_no
#             AND a.plan_source = 'DDLY50'
#             AND a.carrier_company_name = '会好运单车'
#             AND a.plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT45' )
#             AND b.end_point = c.location_id
#             AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' )
#             AND b.order_no = d.order_no
#             AND e.pickup_no = d.pickup_no
#             AND e.source_name IS NULL
#         ORDER BY
#             e.source_name,
#             c.province_name,
#             c.city_name,
#             c.district_name,
#             b.prodname
#         """
#
# # 司机拉货是否跨厂区分析
# sql_5 = """
#             SELECT
#              a.plan_no,
#              a.trains_no,
#              a.plan_source,
#              a.plan_status,
#              b.plan_weight,
#              b.order_no,
#              d.driver_id,
#              a.remark,
#              c.province_name AS province,
#              c.city_name AS city,
#              c.district_name AS district,
#              b.prodname AS commodity_name,
#              a.create_date
#             FROM
#              (
#             SELECT
#              plan_no,
#              trains_no,
#              plan_source,
#              plan_status,
#              remark,
#              carrier_company_name,
#              create_date
#             FROM
#              db_trans.`t_plan`
#             WHERE
#              date_sub( curdate( ), INTERVAL 1 DAY ) <= create_date
#              AND create_date < date_sub( curdate( ), INTERVAL 0 DAY )
#              AND business_moduleId = '020'
#              ) a,
#              (
#             SELECT
#              end_point,
#              order_no,
#              plan_weight,
#              plan_no,
#              prodname
#             FROM
#              db_trans.t_plan_item
#             WHERE
#              date_sub( curdate( ), INTERVAL 1 DAY ) <= create_date
#              AND create_date < date_sub( curdate( ), INTERVAL 0 DAY )
#              ) b,
#              db_trans.t_plan_driver d,
#              db_sys.t_point c
#             WHERE
#              a.plan_no = b.plan_no
#              AND a.plan_source = 'DDLY50'
#              AND a.carrier_company_name = '会好运单车'
#              AND a.plan_status NOT IN ( 'DDZT35', 'DDZT40', 'DDZT45' )
#              and a.plan_no = d.plan_no
#                  and d.driver_id in ('U000047180','U000088789','U000068878','U000093981')
#              AND b.end_point = c.location_id
#              AND c.city_name IN ( '菏泽市', '济南市', '淄博市', '滨州市' )
#             ORDER BY
#             a.trains_no,
#              c.province_name,
#              c.city_name,
#              c.district_name,
#              b.prodname
#         """
#
#
# # 11月10日摘单中人工派单的司机历史6个月记录查询
# sql_6 = """
#             SELECT
#                 w.waybill_no,
#                 d.company_id,
#                 total_weight,
#                 w.load_date,
#                 u.user_id,
#                 u.NAME driver_name,
#                 u.mobile,
#                 p.city_name,
#                 w.carrier_company_id,
#                 cu.company_id hhy_carrier_company_id,
#                 d.vehicle_no
#             FROM
#                 db_ods.db_sys_t_company_user cu
#                 LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
#                 LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
#                 LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
#                 LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point
#             WHERE
#                 u.user_company_type = 'YHLX20'
#                 AND cu.company_id = 'C000062070'
#                 AND u.auth_status = 'RZZT90'
#                 AND u.STATUS = 'YHZT10'
#                 AND p.province_name RLIKE '山东'
#                 AND d.driver_id IN ( 'U000060779', 'U000045621', 'U000061787',
#                 'U000050321', 'U000043842', 'U000052075',
#                 'U000085166', 'U000104197', 'U000104189' )
#                 AND w.load_date BETWEEN date_sub( now( ), INTERVAL 6 MONTH )
#                 AND NOW( )
#                 AND w.load_date LIKE '2020-11-10%'
#                 LIMIT 1000
#         """
#
# # 11月9日摘单中人工派单的司机历史6个月记录查询
# """SELECT
# 	w.waybill_no,
# 	d.company_id,
# 	total_weight,
# 	w.load_date,
# 	u.user_id,
# 	u.NAME driver_name,
# 	u.mobile,
# 	p.city_name,
# 	w.carrier_company_id,
# 	cu.company_id hhy_carrier_company_id,
# 	d.vehicle_no
# FROM
# 	db_ods.db_sys_t_company_user cu
# 	LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
# 	LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
# 	LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
# 	LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point
# WHERE
# 	u.user_company_type = 'YHLX20'
# 	AND cu.company_id = 'C000062070'
# 	AND u.auth_status = 'RZZT90'
# 	AND u.STATUS = 'YHZT10'
# 	AND p.province_name RLIKE '山东'
# 	AND d.driver_id IN (
# 		'U000060779',
# 		'U000045621',
# 		'U000061787',
# 		'U000060017',
# 		'U000047195',
# 		'U000064755',
# 		'U000047180',
# 		'U000049329',
# 		'U000062171',
# 		'U000047185',
# 		'U000038187',
# 		'U000093981',
# 		'U000038148',
# 		'U000044995',
# 		'U000038361',
# 		'U000051259',
# 		'U000075496',
# 		'U000038494',
# 		'U000051263',
# 		'U000045726',
# 		'U000045561'
# 	)
# 	AND w.load_date BETWEEN date_sub( now( ), INTERVAL 6 MONTH )
# 	AND NOW( ) -- 	and w.load_date LIKE '2020-11-09%'
#
# 	LIMIT 2000
# ORDER BY
# 	u.user_id
# """
#
# # 11月9日摘单中人工派单的司机历史6个月最早的一次记录查询
# """SELECT
# 	w.waybill_no,
# 	d.company_id,
# 	total_weight,
# 	MIN( w.load_date ),
# 	u.user_id,
# 	u.NAME driver_name,
# 	u.mobile,
# 	p.city_name,
# 	w.carrier_company_id,
# 	cu.company_id hhy_carrier_company_id,
# 	d.vehicle_no
# FROM
# 	db_ods.db_sys_t_company_user cu
# 	LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
# 	LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
# 	LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
# 	LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point
# WHERE
# 	u.user_company_type = 'YHLX20'
# 	AND cu.company_id = 'C000062070'
# 	AND u.auth_status = 'RZZT90'
# 	AND u.STATUS = 'YHZT10'
# 	AND p.province_name RLIKE '山东'
# 	AND d.driver_id IN (
# 		'U000060779',
# 		'U000045621',
# 		'U000061787',
# 		'U000060017',
# 		'U000047195',
# 		'U000064755',
# 		'U000047180',
# 		'U000049329',
# 		'U000062171',
# 		'U000047185',
# 		'U000038187',
# 		'U000093981',
# 		'U000038148',
# 		'U000044995',
# 		'U000038361',
# 		'U000051259',
# 		'U000075496',
# 		'U000038494',
# 		'U000051263',
# 		'U000045726',
# 		'U000045561'
# 	)
# 	AND w.load_date BETWEEN date_sub( now( ), INTERVAL 6 MONTH )
# 	AND NOW( )
# 	AND w.load_date < '2020-11-09'
# 	AND p.city_name IN ( '滨州市', '菏泽市', '济南市', '淄博市' )
# GROUP BY
# 	u.user_id -- 	ORDER BY u.user_id
# """
#
# # 11月9、10日摘单中人工派单的司机货物明细查询
# """    SELECT
#    w.waybill_no,
#    d.company_id,
# 	 wp.out_stock,
# 	 wp.pre_pack_weight as weight,
# 	 wp.pre_pack_sheet as count,
#    total_weight,
#    w.load_date,
#    u.user_id ,
#    u. NAME driver_name,
#    u.mobile,
#    p.city_name ,
#    w.carrier_company_id,
#    cu.company_id hhy_carrier_company_id ,
#    d.vehicle_no
#   FROM
#    db_ods.db_sys_t_company_user cu
#   LEFT JOIN db_ods.db_sys_t_user u ON cu.user_id = u.user_id
#   LEFT JOIN db_ods.db_trans_t_waybill_driver d ON u.user_id = d.driver_id
#   LEFT JOIN db_ods.db_trans_t_waybill w ON w.waybill_no = d.waybill_no
#   LEFT JOIN db_ods.db_trans_t_waybill_pack wp ON w.waybill_no = wp.waybill_no
#   LEFT JOIN db_ods.db_sys_t_point p ON p.location_id = w.end_point
#   WHERE
#    u.user_company_type = 'YHLX20'
#   AND cu.company_id = 'C000062070'
#   and u.auth_status = 'RZZT90'
#   and u.status = 'YHZT10'
#    and p.province_name rlike '山东'
# 	 and d.driver_id in ('U000060779','U000045621','U000061787','U000060017','U000047195',
# 	 'U000064755','U000047180','U000049329','U000062171','U000047185','U000038187',
# 	 'U000093981','U000038148','U000044995','U000038361','U000051259','U000075496',
# 	 'U000038494','U000051263','U000045726','U000045561')
#    and w.load_date BETWEEN date_sub(now(),INTERVAL 6 month) and NOW()
# 	and w.load_date > '2020-11-09'
# 	and p.city_name in ('滨州市','菏泽市','济南市','淄博市')
# 	LIMIT 1000
# 	ORDER BY w.load_date
#
# """
