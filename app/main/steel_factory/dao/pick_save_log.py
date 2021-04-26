# -*- coding: utf-8 -*-
# Description: 保存摘单分货的记录
# Created: jjunf 2020/10/14
from app.util.base.base_dao import BaseDao


class SavePickLog(BaseDao):

    def save_pick_log(self, values):
        """
        保存摘单分货的记录
        :param values:
        :return:
        """
        sql = """
                insert into 
                db_model.t_pick_log(
                                    pick_id,
                                    pick_total_weight,
                                    pick_truck_num,
                                    remark,
                                    
                                    load_task_id,
                                    load_task_type,
                                    total_weight,
                                    total_count,
                                    
                                    weight,
                                    item_count, 
                                    city, 
                                    end_point, 
                                    consumer,
                                    big_commodity, 
                                    commodity, 
                                    outstock_code, 
                                    notice_num, 
                                    oritem_num,
                                    create_date
                                  )
                value( %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s
                      )
                """
        self.executemany(sql, values)

    def save_pick_deduct_log(self, values):
        """
        保存摘单分货的记录
        :param values:
        :return:
        """
        sql = """
                insert into 
                db_model.t_pick_deduct_log(
                                    pick_id,
                                    remark,
                                    city, 
                                    end_point, 
                                    big_commodity, 
                                    across_factory,
                                    create_date
                                  )
                value( %s, %s, %s, %s, %s, 
                       %s, %s
                      )
                """
        self.executemany(sql, values)

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


save_pick_log = SavePickLog()
