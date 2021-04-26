# -*- coding: utf-8 -*-
# Description:
# Created: luchengkai 2020/11/16 14:43
from app.main.steel_factory.entity.pick_propelling import PickPropelling
from app.main.steel_factory.entity.pick_propelling_black_list import PickBlackListDao
from app.main.steel_factory.entity.pick_propelling_driver import PickPropellingDriver
from app.util.base.base_dao import BaseDao


class PickPropellingDao(BaseDao):

    def select_wait_pick_list(self):
        """
        查询当日摘单计划列表
        :return:
        """
        sql = """
        SELECT
            tmp.pickup_no,
            tmp.prod_name,
            tmp.start_point,
            tmp.end_point,
            tmp.city_name,
            tmp.district_name,
            SUM( tmp.total_truck_num ) total_truck_num,
            SUM( tmp.remain_truck_num ) remain_truck_num,
            tmp.total_weight,
            tmp.remain_total_weight,
            tmp.driver_type,
            tmp.pickup_start_time
        FROM
            (
        SELECT
            distinct pickup_no,
            start_point,
            end_point,
            city_name,
            district_name,
            
            total_truck_num,
            remain_truck_num,
            total_weight,
            remain_total_weight,
            prod_name,
            driver_type,
            pickup_start_time
        FROM
            db_ads.zd_pickup_order_driver 
        WHERE pickup_status in ('PUST20', 'PUST30') and driver_type in ('SJLY10','SJLY20')
            ) tmp 
        WHERE
            pickup_start_time < SUBDATE( NOW( ), INTERVAL 5 MINUTE ) 
        GROUP BY
            tmp.pickup_no,
            tmp.start_point,
            tmp.end_point,
            tmp.prod_name
        
        """
        data = self.select_all(sql)
        return [PickPropelling(i) for i in data]

    def select_wait_driver_list(self):
        """
        查询当日摘单计划司机列表
        :return:
        """
        sql = """
        SELECT
            pickup_no,
            driver_id,
            driver_phone,
            district_name,
            prod_name,
            be_order_confirmed as be_confirmed
        FROM
            db_ads.zd_pickup_order_driver
        WHERE
            pickup_status in ('PUST20', 'PUST30') and driver_type in ('SJLY10','SJLY20')
            AND pickup_start_time < SUBDATE( NOW( ), INTERVAL 5 MINUTE )
        """
        data = self.select_all(sql)
        return [PickPropellingDriver(i) for i in data]

    def select_driver_gps_list(self):
        """
        查询调度单司机位置信息
        :return:
        """
        sql = """
        SELECT
            driver_id,
            latitude,
            longitude,
            create_date AS gps_create_date 
        FROM
            db_ads.zd_plan_driver_app_gps 
        WHERE
            latitude IS NOT NULL 
            AND longitude IS NOT NULL 
            AND create_date IS NOT NULL 
        ORDER BY
            create_date DESC
        """
        data = self.select_all(sql)
        return [PickPropellingDriver(i) for i in data]

    def select_black_list(self):
        """
        查询在黑名单的司机列表
        :return:
        """
        sql = """
                SELECT
                    driver_id,
                    district district_name,
                    product_name prod_name,
                    `count` ignore_count,
                    update_time 
                FROM
                    t_driver_no_interest_count 
                WHERE
                    `count` >= 3 
                    AND DATE_SUB(NOW(), INTERVAL 15 DAY) < update_time
        """
        data = self.select_all(sql)
        return [PickBlackListDao(i) for i in data]

    def select_driver_truck(self, driver_id_list):
        """
        查询司机车辆关联表
        :return:
        """
        sql = """
            SELECT
            driver_id,
            vehicle_no
            FROM
            db_cdm.dim_driver_vehicle
            WHERE
            driver_id in ({})
        """
        commodity_values = "'"
        commodity_values += "','".join([i for i in driver_id_list])
        commodity_values += "'"
        sql = sql.format(commodity_values)
        data = self.select_all(sql)
        return [PickPropellingDriver(i) for i in data]

    def select_truck_zjxl_gps(self, driver_id_list):
        """
        查询中交兴路车辆位置信息
        :return:
        """
        sql = """
            SELECT
            tdv.driver_id,
            tdv.vehicle_no,
            tpz.lon longitude,
            tpz.lat latitude,
            tpz.insert_time gps_create_date 
            -- tpz.vno,
            -- tpz.city,
            -- tpz.province
            
            FROM
            db_ods.db_sys_t_driver_vehicle tdv
            LEFT JOIN db_ods.rg_travel_path_zjxl tpz ON tdv.vehicle_no = tpz.vno 
            WHERE
            tpz.province = "山东省" 
            AND tdv.driver_id in ({})
            AND tpz.insert_time > DATE_SUB( NOW( ), INTERVAL 20 MINUTE ) 
            GROUP BY
            tdv.vehicle_no,
            tpz.lon,
            tpz.lat
        """
        commodity_values = "'"
        commodity_values += "','".join([i for i in driver_id_list])
        commodity_values += "'"
        sql = sql.format(commodity_values)
        data = self.select_all(sql)
        return [PickPropellingDriver(i) for i in data]

    def select_driver_weight(self):
        """
        查询司机已接单重量
        :return:
        """
        sql = """
            SELECT 
            tmp.driver_id,
            SUM(tmp.plan_weight) plan_weight
            FROM
            (
            SELECT 
            driver_id,
            plan_no,
            plan_weight
            FROM
            db_ads.zd_plan_driver_app_gps
            GROUP BY driver_id,plan_no
            ) tmp GROUP BY tmp.driver_id
        """
        data = self.select_all(sql)
        return [PickPropellingDriver(i) for i in data]

    def save_pick_log(self, values):
        """
        保存摘单计划推送的记录
        :param values:
        :return:
        """
        sql = """
            INSERT INTO 
                t_propelling_log(
                    pickup_no,
                    prod_name,
                    start_point,
                    end_point,
                    city_name,
                    district_name,
                    total_truck_num,
                    remain_truck_num,
                
                    driver_id,
                    driver_phone,  
                    label_type,
                    create_date,
                    
                    driver_type,
                    latitude,
                    longitude,
                    dist,
                    location_flag
                )
            VALUES( 
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            """
        self.executemany(sql, values)


pick_propelling_dao = PickPropellingDao()
