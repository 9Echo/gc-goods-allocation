# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/24
from datetime import datetime
from typing import List

import pandas as pd
from threading import Thread
from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule import pick_early_dispatch_filter
from app.main.steel_factory.service.pick_stock_service import split_pick_stock
from app.test.jjunf.qingdao_compare_analysis.qd_stock import QdStock
from app.test.jjunf.qingdao_compare_analysis.save_log import save_log
from app.test.jjunf.qingdao_compare_analysis.stock_dao import stock_dao
from app.util.code import ResponseCode
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from app.util.my_exception import MyException
from app.util.uuid_util import UUIDUtil
from model_config import ModelConfig


def get_stock_id(obj):
    """
    根据库存信息生成每条库存的唯一id
    """
    if isinstance(obj, Stock):
        return hash(obj.notice_num + obj.oritem_num + obj.deliware_house)
    elif isinstance(obj, LoadTaskItem):
        return hash(obj.notice_num + obj.oritem_num + obj.outstock_code)


def get_pick_stock():
    """
    根据车辆目的地和可运货物返回库存列表
    """
    # 根据品种查询库存
    all_stock_list = stock_dao.select_stock()
    if not all_stock_list:
        raise MyException('找不到符合条件的数据！', ResponseCode.Error)
    # 获取库存列表
    df_stock = pd.DataFrame(all_stock_list)
    '''将初始库存结果保存到数据库-start'''
    save_dic = df_stock.to_dict(orient="record")
    # 初始化对象列表
    save_stock_list = [QdStock(obj) for obj in save_dic]
    # 结果保存到数据库
    Thread(target=save_init_stock, args=(save_stock_list,)).start()
    '''将初始库存结果保存到数据库-end'''
    # 需与卸货的订单地址，数据库中保存的地址及经纬度合并
    # df_stock = merge_stock(df_stock)
    df_stock["CANSENDWEIGHT"] = df_stock["CANSENDWEIGHT"].astype('float64')
    df_stock["CANSENDNUMBER"] = df_stock["CANSENDNUMBER"].astype('int64')
    df_stock["NEED_LADING_WT"] = df_stock["NEED_LADING_WT"].astype('float64')
    df_stock["NEED_LADING_NUM"] = df_stock["NEED_LADING_NUM"].astype('int64')
    df_stock["OVER_FLOW_WT"] = df_stock["OVER_FLOW_WT"].astype('float64')
    df_stock["waint_fordel_number"] = df_stock["waint_fordel_number"].astype('int64')
    df_stock["waint_fordel_weight"] = df_stock["waint_fordel_weight"].astype('float64')
    # 根据公式，计算实际可发重量，实际可发件数
    df_stock["actual_weight"] = (df_stock["CANSENDWEIGHT"] + df_stock["NEED_LADING_WT"]) * 1000
    df_stock["actual_number"] = df_stock["CANSENDNUMBER"] + df_stock["NEED_LADING_NUM"]
    # 根据公式计算件重
    df_stock["piece_weight"] = round(df_stock["actual_weight"] / df_stock["actual_number"])
    # 需短溢处理
    df_stock["OVER_FLOW_WT"] = df_stock["OVER_FLOW_WT"] * 1000
    df_stock.loc[df_stock["OVER_FLOW_WT"] > 0, ["actual_number"]] = df_stock["actual_number"] + (
            -df_stock["OVER_FLOW_WT"] // df_stock["piece_weight"])
    df_stock["actual_weight"] = df_stock["piece_weight"] * df_stock["actual_number"]
    # 计算待生产重量
    df_stock["waint_fordel_weight"] = df_stock["waint_fordel_weight"] * 1000
    df_stock["wait_product_weight"] = df_stock["waint_fordel_weight"] - df_stock["actual_weight"]
    # 筛选出大于0的数据
    df_stock = df_stock.loc[
        (df_stock["actual_weight"] > 0) & (df_stock["actual_number"] > 0) & (
            df_stock["latest_order_time"].notnull())]
    if df_stock.empty:
        raise MyException('找不到符合条件的数据！', ResponseCode.Error)

    def rename(row):
        if row['big_commodity_name'].find('新产品') != -1 or row['big_commodity_name'].find('老区') != -1:
            return row
        # 将所有黑卷置成卷板
        if row['big_commodity_name'] == '黑卷':
            row['big_commodity_name'] = '卷板'
        # 如果是西区开平板，则改为新产品-冷板
        if row['deliware_house'].startswith("P") and row['big_commodity_name'] == '开平板':
            row['big_commodity_name'] = '新产品-冷板'
        # 如果是西区非开平板，则品名前加新产品-
        elif row['deliware_house'].startswith("P") and row['big_commodity_name'] != '开平板':
            row['big_commodity_name'] = '新产品-' + row['big_commodity_name']
        # 如果是外库，且是西区品种，则品名前加新产品-
        elif (row['deliware_house'].find('F10') != -1 or row['deliware_house'].find('F20') != -1) and row[
            'big_commodity_name'] in ['白卷', '窄带', '冷板']:
            row['big_commodity_name'] = '新产品-' + row['big_commodity_name']
        # 其余全部是老区-
        else:
            row['big_commodity_name'] = '老区-' + row['big_commodity_name']
        return row

    df_stock = df_stock.apply(rename, axis=1)
    # 窄带按捆包数计算，实际可发件数 = 捆包数
    df_stock.loc[(df_stock["big_commodity_name"] == "新产品-窄带") & (df_stock["PACK_NUMBER"] > 0), ["actual_number"]] = \
        df_stock["PACK_NUMBER"]
    # 单独计算窄带的件重
    df_stock.loc[
        (df_stock["big_commodity_name"] == "新产品-窄带") & (df_stock["PACK_NUMBER"] > 0), ["piece_weight"]] = round(
        df_stock["actual_weight"] / df_stock["actual_number"])
    # 将终点统一赋值到实际终点，方便后续处理联运
    df_stock["actual_end_point"] = df_stock["dlv_spot_name_end"]
    df_stock.loc[df_stock["deliware"].str.startswith("U"), ["actual_end_point"]] = df_stock["deliware"]
    df_stock.loc[df_stock["deliware"].str.startswith("U"), ["standard_address"]] = df_stock["PORTNUM"]
    df_stock.loc[(df_stock["port_name_end"].isin(ModelConfig.RG_PORT_NAME_END_LYG)) & (
        df_stock["big_commodity_name"].isin(ModelConfig.RG_COMMODITY_LYG)), ["actual_end_point"]] = "U288-岚北港口库2LYG"

    dic = df_stock.to_dict(orient="record")
    # 初始化对象列表
    init_stock_list = [Stock(obj) for obj in dic]
    # 早期处理
    early_load_task_list, early_stock_list, early_wait_list = pick_early_dispatch_filter.early_dispatch_filter(
        init_stock_list)
    stock_list, wait_list = split_pick_stock(init_stock_list)
    stock_list.extend(early_stock_list)
    wait_list.extend(early_wait_list)
    # 筛选出西区的'新产品-卷板', '新产品-白卷'
    west_j_stock = [item for item in stock_list if
                    item.big_commodity_name in ['新产品-卷板', '新产品-白卷'] and item.deliware_house in
                    ModelConfig.RG_WAREHOUSE_GROUP[0]]
    # 筛选出西区中除'新产品-卷板', '新产品-白卷'外的其他货物
    west_stock = [item for item in stock_list if
                  item.big_commodity_name not in ['新产品-卷板', '新产品-白卷'] and item.deliware_house in
                  ModelConfig.RG_WAREHOUSE_GROUP[0]]
    # 筛选出老区的'老区-卷板'
    old_j_stock = [item for item in stock_list if
                   item.big_commodity_name == "老区-卷板" and item.deliware_house in
                   ModelConfig.RG_WAREHOUSE_GROUP[1]]
    # 筛选出老区中除'老区-卷板'外的其他货物
    old_stock = [item for item in stock_list if
                 item.big_commodity_name != "老区-卷板" and item.deliware_house in
                 ModelConfig.RG_WAREHOUSE_GROUP[1]]
    # 筛选出岚北港的卷类
    lbg_j_stock = [item for item in stock_list if
                   item.big_commodity_name in ModelConfig.RG_J_GROUP and item.deliware_house in
                   ModelConfig.RG_WAREHOUSE_GROUP[2]]
    # 筛选出岚北港中除卷类外的其他货物
    lbg_stock = [item for item in stock_list if
                 item.big_commodity_name not in ModelConfig.RG_J_GROUP and item.deliware_house in
                 ModelConfig.RG_WAREHOUSE_GROUP[2]]
    # 不在ModelConfig.RG_WAREHOUSE_GROUP仓库中的货物
    other_warehouse_stock = [item for item in stock_list if
                             item.deliware_house not in ModelConfig.RG_WAREHOUSE_GROUP_LIST]
    return west_stock, old_stock, lbg_stock, west_j_stock, old_j_stock, lbg_j_stock, other_warehouse_stock, wait_list, early_load_task_list


def save_init_stock(stock_list:List[QdStock]):
    """
    保存原始库存记录
    :param stock_list:
    :return:
    """

    if not stock_list:
        return None
    values = []
    create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 库存记录
    for stock in stock_list:
        stock_id = GenerateId.get_id()
        if stock.standard_address is not None:
            longitude = stock.standard_address.split('_')[0]
            latitude = stock.standard_address.split('_')[1]
        else:
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
                      stock.CANSENDNUMBER,
                      stock.CANSENDWEIGHT,
                      stock.dlv_spot_name_end,
                      stock.PACK_NUMBER,
                      stock.NEED_LADING_NUM,
                      stock.NEED_LADING_WT,
                      stock.OVER_FLOW_WT,
                      stock.latest_order_time,
                      stock.port_name_end,
                      stock.priority,
                      longitude,
                      latitude,
                      create_date
                      )
        values.append(item_tuple)
    save_log.save_qingdao_stock_log(values)

