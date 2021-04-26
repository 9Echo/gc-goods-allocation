# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/01
from datetime import datetime
from threading import Thread
from typing import List
import pandas as pd

from app.main.steel_factory.dao.pick_save_log import save_pick_log
from app.main.steel_factory.dao.pick_stock_dao import pick_stock_dao
from app.main.steel_factory.entity.pick_save_hour_stock import SaveHourStock
from app.util.generate_id import GenerateId


def save_hour_stock():
    """
    根据车辆目的地和可运货物返回库存列表
    """
    # 根据品种查询库存
    all_stock = pick_stock_dao.select_pick_stock()
    # 获取库存列表
    df_stock = pd.DataFrame(all_stock)
    save_dic = df_stock.to_dict(orient="record")
    # 初始化对象列表
    save_stock_list = [SaveHourStock(obj) for obj in save_dic]
    if save_stock_list:
        # 结果保存到数据库
        Thread(target=save_init_stock, args=(save_stock_list,)).start()


def save_init_stock(stock_list:List[SaveHourStock]):
    """
    保存原始库存记录
    :param stock_list:
    :return:
    """
    if not stock_list:
        return None
    values = []
    save_date = datetime.now().strftime("%Y-%m-%d %H") + ':00:00'
    create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 库存记录
    for stock in stock_list:
        stock_id = GenerateId.get_id()
        # if stock.standard_address is not None:
        #     longitude = stock.standard_address.split('_')[0]
        #     latitude = stock.standard_address.split('_')[1]
        # else:
        #     longitude = ' '
        #     latitude = ' '
        longitude = ' '
        latitude = ' '
        item_tuple = (stock_id,
                      stock.notice_num,
                      stock.consumer,
                      stock.oritem_num,
                      stock.devperiod,
                      stock.deliware_house_name,
                      stock.commodity_name,
                      stock.big_commodity_name,
                      stock.pack,
                      stock.mark,
                      stock.specs,
                      stock.deliware,
                      stock.contract_no,
                      stock.PORTNUM,
                      stock.detail_address,
                      stock.province,
                      stock.city,
                      stock.waint_fordel_number,
                      stock.waint_fordel_weight,
                      stock.can_send_number,
                      stock.can_send_weight,
                      stock.dlv_spot_name_end,
                      stock.pack_number,
                      stock.need_lading_num,
                      stock.need_lading_wt,
                      stock.over_flow_wt,
                      stock.latest_order_time,
                      stock.port_name_end,
                      stock.priority,
                      longitude,
                      latitude,
                      save_date,
                      create_date
                      )
        values.append(item_tuple)
    save_pick_log.save_stock_log(values)


if __name__=='__main__':
    save_hour_stock()
