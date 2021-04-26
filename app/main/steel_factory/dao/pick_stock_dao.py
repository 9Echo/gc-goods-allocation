# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/9/29 9:14
from app.util.base.base_dao import BaseDao


class PickStockDao(BaseDao):
    """
    摘单数据访问层
    """

    def select_pick_stock(self):
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
                CANSENDNUMBER as can_send_number, 
                CANSENDWEIGHT as can_send_weight, 
                DLV_SPOT_NAME_END as dlv_spot_name_end, 
                PACK_NUMBER as pack_number, 
                NEED_LADING_NUM as need_lading_num, 
                NEED_LADING_WT as need_lading_wt, 
                OVER_FLOW_WT as over_flow_wt, 
                LATEST_ORDER_TIME as latest_order_time, 
                PORT_NAME_END as port_name_end,
                priority,
                concat(longitude, latitude) as standard_address
            FROM
                db_ads.kc_rg_product_can_be_send_amount
            WHERE 
                CITY in ('滨州市','淄博市','济南市','菏泽市')
                and (CANSENDNUMBER > 0 OR NEED_LADING_NUM > 0) 
                and LATEST_ORDER_TIME is not null 
            """
        # # 品种条件
        # if truck.big_commodity_name:
        #     commodity_sql_condition = " and BIG_COMMODITYNAME in ({})"
        #     commodity_group = ModelConfig.RG_COMMODITY_GROUP_FOR_SQL.get(truck.big_commodity_name, ['未知品种'])
        #     commodity_values = "'"
        #     commodity_values += "','".join([i for i in commodity_group])
        #     commodity_values += "'"
        #     commodity_sql_condition = commodity_sql_condition.format(commodity_values)
        #     sql = sql + commodity_sql_condition
        data = self.select_all(sql)
        return data


pick_stock_dao = PickStockDao()
