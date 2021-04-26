# -*- coding: utf-8 -*-
# Description: 库存服务
# Created: shaoluyu 2020/03/12
import copy
import pandas as pd
import numpy as np
from app.util.code import ResponseCode
from app.util.get_weight_limit import get_lower_limit
from app.util.my_exception import MyException
from model_config import ModelConfig
from app.main.steel_factory.entity.stock import Stock
from app.util.db_pool import db_pool_ods
from app.util.get_static_path import get_path


def get_stock():
    """
    获取库存
    :param vehicle:
    :return: 库存
    """
    """
    步骤：
    1 读取Excel，省内1  0点库存明细和省内2、3及连云港库存两个sheet页
    2 数据合并
    """
    data_path = get_path("sheet1.xls")
    df_stock1 = pd.read_excel(data_path)
    return df_stock1


def address_latitude_and_longitude():
    """获取数据表中地址经纬度信息

    Args:

    Returns:

    Raise:

    """
    sql1 = """
            select address as 'detail_address',longitude, latitude
            from db_ods.db_sys_t_point
        """
    sql2 = """
            select address as '卸货地址2',longitude, latitude 
            from db_ods.db_sys_t_point
            where longitude is not null
            And latitude is not null
            GROUP BY longitude, latitude
        """
    result_1 = pd.read_sql(sql1, db_pool_ods.connection())
    result_2 = pd.read_sql(sql2, db_pool_ods.connection())
    return result_1, result_2


def deal_stock(data):
    """

    :return:
    """
    if not data:
        raise MyException('输入列表为空', ResponseCode.Error)
    data1, data2 = address_latitude_and_longitude()
    # 存放满足条件的stock结果
    stock_list = []
    # 存放筛除的stock结果
    sift_stock_list = []
    # 存放dataframe的结果
    result = pd.DataFrame()
    # 获取库存
    df_stock = pd.DataFrame(data)
    df_stock = pd.merge(df_stock, data1, on="detail_address", how="left")
    df_stock = pd.merge(df_stock, data2, on=["latitude", "longitude"], how="left")
    df_stock["实际终点"] = df_stock["dlv_spot_name_end"]
    # 根据公式，计算实际可发重量，实际可发件数
    df_stock["实际可发重量"] = df_stock["can_send_weight"] * 1000
    df_stock["实际可发件数"] = df_stock["can_send_number"]
    # # 根据公式计算件重
    df_stock["件重"] = round(df_stock["实际可发重量"] / df_stock["实际可发件数"])
    df_stock["实际可发重量"] = df_stock["件重"] * df_stock["实际可发件数"]
    df_stock.loc[df_stock["deliware"].str.startswith("U"), ["实际终点"]] = df_stock["deliware"]
    df_stock.loc[(df_stock["port_name_end"].isin(ModelConfig.RG_PORT_NAME_END_LYG)) & (
        df_stock["big_commodity_name"].isin(ModelConfig.RG_COMMODITY_LYG)), ["实际终点"]] = "U288-岚北港口库2LYG"
    df_stock.loc[df_stock["deliware"].str.startswith("U"), ["卸货地址2"]] = df_stock["portnum"]
    df_stock.loc[df_stock["priority"].isnull(), ["priority"]] = ""
    df_stock["sort"] = 3
    df_stock.loc[
        (df_stock["实际可发重量"] <= ModelConfig.RG_MAX_WEIGHT) & (
                df_stock["实际可发重量"] >= ModelConfig.RG_MIN_WEIGHT), ["sort"]] = 2
    df_stock.loc[
        (df_stock["件重"] >= ModelConfig.RG_SECOND_MIN_WEIGHT) & (df_stock["件重"] < ModelConfig.RG_J_MIN_WEIGHT) &
        (df_stock["big_commodity_name"].isin(ModelConfig.RG_COMMODITY_LYG)), ["sort"]] = 1
    result = result.append(df_stock)
    result = rename_pd(result)
    # 如果标准地址没有匹配到，那么就是用详细地址代替
    result.loc[result["standard_address"].isnull(), ["standard_address"]] = result["detail_address"]
    result['actual_weight'].fillna(0, inplace=True)
    result['actual_number'].fillna(0, inplace=True)
    result['piece_weight'][np.isinf(result['piece_weight'])] = -1
    dic = result.to_dict(orient="record")
    count_parent = 0
    # 对每个stock处理
    for record in dic:
        count_parent += 1
        stock = Stock(record)
        stock.parent_stock_id = count_parent
        stock.actual_number = int(stock.actual_number)
        stock.actual_weight = int(stock.actual_weight)
        stock.piece_weight = int(stock.piece_weight)
        stock.priority = ModelConfig.RG_PRIORITY.get(stock.priority, 4)
        # 过滤掉不符合条件的stock
        if stock.actual_number <= 0 or stock.actual_weight <= 0 or not stock.latest_order_time \
                or (stock.actual_number < stock.waint_fordel_number and get_lower_limit(stock.big_commodity_name)
                    <= stock.waint_fordel_weight <= ModelConfig.RG_MAX_WEIGHT):
            sift_stock_list.append(stock)
            continue
        # 组数
        target_group_num = 0
        # 临时组数
        temp_group_num = 0
        # 最后一组件数
        target_left_num = 0
        # 一组几件
        target_num = 0
        for weight in range(get_lower_limit(stock.big_commodity_name), ModelConfig.RG_MAX_WEIGHT + 1000, 1000):
            # 一组几件
            num = weight // stock.piece_weight
            if num < 1 or num > stock.actual_number:
                target_num = num
                continue
            # 如果还没轮到最后，并且标准组重量未达到标载，就跳过
            if weight < ModelConfig.RG_MAX_WEIGHT and (num * stock.piece_weight) < get_lower_limit(
                    stock.big_commodity_name):
                continue
            # 组数
            group_num = stock.actual_number // num
            # 最后一组件数
            left_num = stock.actual_number % num
            # 如果最后一组符合标载条件，临时组数加1
            temp_num = 0
            if (left_num * stock.piece_weight) >= get_lower_limit(stock.big_commodity_name):
                temp_num = 1
            # 如果分的每组件数更多，并且组数不减少，就替换
            if (group_num + temp_num) >= temp_group_num:
                target_group_num = group_num
                temp_group_num = group_num + temp_num
                target_left_num = left_num
                target_num = num
        # 将货物分成若干份
        # num = ModelConfig.RG_MAX_WEIGHT // stock.piece_weight
        # 首先去除 件重大于33000的货物
        if target_num < 1:
            sift_stock_list.append(stock)
            continue
        # 其次如果可装的件数大于实际可发件数，不用拆分，直接添加到stock_list列表中
        elif target_num > stock.actual_number:
            stock.limit_mark = 0
            stock_list.append(stock)
        # 最后不满足则拆分
        else:
            limit_mark = 1
            for q in range(int(target_group_num)):
                copy_2 = copy.deepcopy(stock)
                copy_2.actual_weight = target_num * stock.piece_weight
                copy_2.actual_number = int(target_num)
                if copy_2.actual_weight < get_lower_limit(stock.big_commodity_name):
                    limit_mark = 0
                else:
                    limit_mark = 1
                if get_lower_limit(stock.big_commodity_name) <= copy_2.actual_weight <= ModelConfig.RG_MAX_WEIGHT:
                    copy_2.sort = 2
                stock_list.append(copy_2)
            if target_left_num:
                copy_1 = copy.deepcopy(stock)
                copy_1.actual_number = int(target_left_num)
                copy_1.actual_weight = target_left_num * stock.piece_weight
                copy_1.limit_mark = limit_mark
                if get_lower_limit(stock.big_commodity_name) <= copy_1.actual_weight <= ModelConfig.RG_MAX_WEIGHT:
                    copy_1.sort = 2
                stock_list.append(copy_1)
    # 按照优先发运和最新挂单时间排序
    stock_list.sort(key=lambda x: (x.sort, x.priority, x.latest_order_time, x.actual_weight * -1), reverse=False)
    count = 1
    # 为排序的stock对象赋Id
    for num in copy.copy(stock_list):
        num.stock_id = count
        count += 1

    if not stock_list:
        raise MyException('输入可发库存无效', ResponseCode.Error)
    return stock_list, sift_stock_list


def rename_pd(dataframe):
    """
    更改列名
    Args:
        dataframe: dataframe数据

    Returns:

    Raise:

    """
    dataframe.rename(index=str,
                     columns={
                         "material": "mark",
                         "standard": "specs",
                         "实际可发重量": "actual_weight",
                         "实际可发件数": "actual_number",
                         "件重": "piece_weight",
                         "卸货地址2": "standard_address",
                         "实际终点": "actual_end_point"
                     },
                     inplace=True)
    return dataframe


if __name__ == "__main__":
    a = deal_stock()
    # for i, j in b.items():
    #     for k in j:
    #         print(i, k.priority, k.actual_weight)
    # for i in a:
    #     print(i.sort, i.priority, i.latest_order_time)
    # if i.priority not in (1, 2, 3):
    #     print(i.stock_id, i.priority, i.latest_order_time, i.actual_weight, i.piece_weight, i.actual_number)
