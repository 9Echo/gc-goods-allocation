# -*- coding: utf-8 -*-
# Description:
# Created: luchengkai 2021/01/06 14:43
from app.main.steel_factory.entity.pick_active_driver import PickActiveDriver
from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.util.base.base_dao import BaseDao


class PickPropellingFilterDao(BaseDao):

    def select_flows_relation(self):
        """
        查询司机三个月内地运单量
        :return:
        """
        sql = """
        SELECT
        *	
        FROM
        db_cdm.dim_active_driver
        ORDER BY
        waybill_count DESC
        """
        data = self.select_all(sql)
        return [PickActiveDriver(i) for i in data]

    def select_pick_truck_count(self, pickup_no):
        """
        根据pickup_no查询摘单计划
        :return:
        """
        sql = """
        SELECT
            tmp.pickup_no,
            tmp.start_point,
            tmp.city_name,
            tmp.district_name,
            tmp.end_point,
            SUM( tmp.total_truck_num ) total_truck_num,
            SUM( tmp.remain_truck_num ) remain_truck_num,
            tmp.total_weight,
            tmp.remain_total_weight,
            tmp.driver_type,
            tmp.pickup_start_time,
            tmp.pickup_status,
            tmp.prod_name
        FROM
            (
        SELECT
            DISTINCT pickup_no,
            start_point,
            end_point,
            city_name,
            district_name,
            total_truck_num,
            remain_truck_num,
            total_weight,
            remain_total_weight,
            driver_type,
            prod_name,
            pickup_start_time,
            pickup_status
        FROM
            db_ads.zd_pickup_order_driver 
            ) tmp
        WHERE tmp.pickup_status IN ('PUST00', 'PUST10') 
        AND pickup_no = '{}'
        GROUP BY
            tmp.pickup_no,
            tmp.start_point,
            tmp.end_point,
            tmp.prod_name
        """
        data = self.select_all(sql.format(pickup_no))
        return [PickPropelling(i) for i in data]

    def select_driver_location(self, values):
        """
        查询司机当前位置信息
        :return:
        """
        sql = """
        SELECT
        t1.* 
        FROM
        db_ads.`zd_hhy_driver_location` t1
        JOIN ( 
        SELECT 
        driver_id, 
        max( receive_date ) AS receive_date 
        FROM db_ads.zd_hhy_driver_location 
        GROUP BY driver_id 
        ) t2 ON t1.driver_id = t2.driver_id 
        AND t1.receive_date = t2.receive_date 
        """
        commodity_values = "'"
        commodity_values += "','".join(values)
        commodity_values += "'"
        data = self.select_all(sql.format(commodity_values))
        return [PickPropellingDriver(i) for i in data]


pick_propelling_filter_dao = PickPropellingFilterDao()
