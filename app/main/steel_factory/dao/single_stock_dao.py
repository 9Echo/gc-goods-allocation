# -*- coding: utf-8 -*-
# Description: 可用库存表
# Created: shaoluyu 2020/06/16
from app.main.steel_factory.entity.truck import Truck
from app.util.base.base_dao import BaseDao
from model_config import ModelConfig


class StockDao(BaseDao):
    def select_stock(self, truck: Truck):
        """
        查询库存
        :return:
        """

        sql = """
        SELECT
            NOTICENUM as notice_num,
            PURCHUNIT as consumer,
            ORITEMNUM as oritem_num, 
            DEVPERIOD as devperiod, 
            SUBSTRING_INDEX(DELIWAREHOUSE,'-',1) as deliware_house,
            DELIWAREHOUSE as deliware_house_name,
            COMMODITYNAME as commodity_name,
            BIG_COMMODITYNAME as big_commodity_name,
            PACK as pack,  
            MATERIAL as mark, 
            STANDARD as specs, 
            DELIWARE as deliware, 
            CONTRACT_NO as contract_no,
            PORTNUM, 
            DETAILADDRESS as detail_address, 
            PROVINCE as province, 
            CITY as city, 
            WAINTFORDELNUMBER as waint_fordel_number, 
            WAINTFORDELWEIGHT as waint_fordel_weight, 
            CANSENDNUMBER, 
            CANSENDWEIGHT, 
            DLV_SPOT_NAME_END as dlv_spot_name_end, 
            PACK_NUMBER, 
            NEED_LADING_NUM, 
            NEED_LADING_WT, 
            OVER_FLOW_WT, 
            LATEST_ORDER_TIME as latest_order_time, 
            PORT_NAME_END as port_name_end,
            priority,
            concat(longitude, latitude) as standard_address,
            CASE
            when
            IFNULL(train_count, '') = ''
            then
            0
            else train_count
            end as load_task_count
        FROM
        db_ads.kc_rg_product_can_be_send_amount
        WHERE 
        CITY = '{}'
        AND 
        (CANSENDNUMBER > 0 OR NEED_LADING_NUM > 0) 
        """
        # 城市条件，不传默认为临沂市
        city_condition = truck.city if truck.city else ModelConfig.RG_DEFAULT_CITY
        sql = sql.format(city_condition)
        # 品种条件
        if truck.big_commodity_name:
            commodity_sql_condition = " and BIG_COMMODITYNAME in ({})"
            commodity_group = ModelConfig.RG_COMMODITY_GROUP_FOR_SQL.get(truck.big_commodity_name, ['未知品种'])
            commodity_values = "'"
            commodity_values += "','".join([i for i in commodity_group])
            commodity_values += "'"
            commodity_sql_condition = commodity_sql_condition.format(commodity_values)
            sql = sql + commodity_sql_condition
            # 厂区条件
            # if truck.big_commodity_name.find('老区') != -1:
            #     sql_condition = " and DELIWAREHOUSE not like 'P%'"
            #     sql = sql + sql_condition
            # else:
            #     sql_condition = " and (DELIWAREHOUSE like 'P%' or DELIWAREHOUSE like 'F10%' or DELIWAREHOUSE like 'F20%')"
            #     sql = sql + sql_condition
        data = self.select_all(sql)
        return data


stock_dao = StockDao()
