# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/24
from app.util.base.base_dao import BaseDao


class SaveLog(BaseDao):

    def save_qingdao_stock_log(self, values):
        """
        保存青岛的原始库存记录
        :param values:
        :return:
        """
        sql = """
                insert into 
                db_model.t_qingdao_stock_log(
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
                                    create_date
                                  )
                value( %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s, %s, %s, %s, 
                       %s, %s 
                      )
                """
        self.executemany(sql, values)

    def save_qingdao_pick_log(self, values):
        """
        保存青岛的模型分货记录
        :param values:
        :return:
        """
        sql = """
                insert into 
                db_model.t_qingdao_pick_log(
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


save_log = SaveLog()
