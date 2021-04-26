import xlrd

# -*- coding: utf-8 -*-
# Description: 摘单库存服务
# Created: jjunf 2020/09/29
import copy
import pandas as pd
from app.main.steel_factory.dao.pick_stock_dao import pick_stock_dao
from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.stock import Stock
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig

from collections import defaultdict
from typing import List

from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.pick_task import PickTask
from app.main.steel_factory.rule import pick_goods_dispatch_filter
import pandas as pd
from app.main.steel_factory.entity.stock import Stock


flag = False


def dispatch():
    """
    摘单分货入口
    :return:
    """
    """
    1.库存预处理
    2.将库存分为西区、老区两个部分，并且切分好
    3.将每个区的库存，按区县、客户分类排序，同区县同客户的按品种、出库仓库再分类排序
    4.同区县同客户的货物先组合配载，同品种优先组合
    5.同客户不同厂区、同厂区同区县不同客户 之间货物组合（哪一个更优？）
    """
    # 获取经过预处理的库存列表(西区库存列表、老区库存列表)
    west_stock_list, old_stock_list, west_j_list, old_j_list = get_pick_stock()
    # 配载，得到配载列表、无法配载的剩余尾货列表
    result_load_task_list, tail_list = pick_goods_dispatch_filter.dispatch_filter(west_stock_list, old_stock_list,
                                                                                  west_j_list, old_j_list)
    # 合并
    pick_list = merge(result_load_task_list)
    # 导出为excel
    to_excel(result_load_task_list, tail_list, pick_list)
    return pick_list, tail_list


def merge(load_task_list):
    """
    按区县、品种、件重、件数合并
    :param load_task_list:
    :return:
    """
    # 合并后的结果列表
    pick_list: List[PickTask] = []
    # 将总重量转为整数
    for load_task in load_task_list:
        load_task.total_weight = round(load_task.total_weight)
    # 将load_task_list按车次的品种是否是一样的分为两类
    same_commodity_load_task_list, not_same_commodity_load_task_list = split_by_commodity(load_task_list)
    # 将same_type_load_task_list按照区县、品种、件数、总重量、装卸类型分类
    load_task_dict = defaultdict(list)
    for load_task in same_commodity_load_task_list:
        item = load_task.items[0]
        key = load_task.city + load_task.end_point + ',' + item.big_commodity + ',' + str(load_task.count) + ',' + str(
            load_task.total_weight) + ',' + load_task.load_task_type
        load_task_dict[key].append(load_task)
    # 111将相同品种的车次合并为一条摘单记录
    for key in load_task_dict.keys():
        pick_task = PickTask()
        for load_task in load_task_dict[key]:
            item = load_task.items[0]
            pick_task.total_weight += load_task.total_weight
            pick_task.truck_num = len(load_task_dict[key])
            pick_task.city = load_task.city
            pick_task.end_point = load_task.end_point
            pick_task.big_commodity = item.big_commodity
            pick_task.commodity = item.commodity
            pick_task.remark = item.big_commodity + str(load_task.count) + '件,一车' + str(
                round(load_task.total_weight)) + '吨左右'
        pick_list.append(pick_task)
    # 222将不同品种的车次 生成 摘单记录
    pick_list.extend(not_same_commodity_pick_record_product(not_same_commodity_load_task_list))
    return pick_list


def split_by_commodity(load_task_list):
    """
    将一车的品种是否是一样的分为两类
    :param load_task_list:
    :return:
    """
    same_commodity_load_task_list = []
    not_same_commodity_load_task_list = []
    for load_task in load_task_list:
        big_commodity_set: set = set([load_task_item.big_commodity for load_task_item in load_task.items])
        if len(big_commodity_set) == 1:
            same_commodity_load_task_list.append(load_task)
        else:
            not_same_commodity_load_task_list.append(load_task)
    return same_commodity_load_task_list, not_same_commodity_load_task_list


def not_same_commodity_pick_record_product(load_task_list):
    """
    将不同品种的车次 生成 摘单记录
    :param load_task_list:
    :return:
    """
    # 不同品种的车次 生成 摘单记录
    pick_list: List[PickTask] = []
    pick_task = PickTask()
    for load_task in load_task_list:
        pick_task.total_weight = round(load_task.total_weight)
        pick_task.truck_num = 1
        pick_task.city = load_task.city
        pick_task.end_point = load_task.end_point
        remark = []
        item_list = []
        for item in load_task.items:
            item_list.append(item)
            remark.append(item.big_commodity + str(load_task.count) + '件' + str(load_task.total_weight) + '吨左右')
        # 当一车有多个品种时，取重量最大的品种作为pick_task的品种字段
        temp_item = item_list[0]
        for i in item_list:
            if i.total_weight > temp_item.total_weight:
                temp_item = i
        pick_task.big_commodity = temp_item.big_commodity
        pick_task.commodity = temp_item.commodity
        remark.append('一车' + str(round(load_task.total_weight)) + '吨左右')
        pick_task.remark = ','.join(remark)
        pick_list.append(pick_task)
    return pick_list


def to_excel(result_load_task_list, tail_list, pick_list):
    # 将result_load_task_list导出为excel
    df = pd.DataFrame()
    if result_load_task_list:
        tid = 0
        truck_id = []
        total_weight = []
        load_task_type = []
        priority_grade = []
        big_commodity = []
        commodity = []
        city = []
        end_point = []
        consumer = []
        weight = []
        count = []
        priority = []
        latest_order_time = []
        out_stock_code = []
        for load_task in result_load_task_list:
            tid = tid + 1
            load_task_item: LoadTaskItem
            for load_task_item in load_task.items:
                truck_id.append(tid)
                total_weight.append(load_task.total_weight)
                load_task_type.append(load_task.load_task_type)
                priority_grade.append(load_task.priority_grade)
                big_commodity.append(load_task_item.big_commodity)
                commodity.append(load_task_item.commodity)
                end_point.append(load_task.end_point)
                city.append(load_task_item.city)
                consumer.append(load_task_item.consumer)
                weight.append(load_task_item.weight)
                count.append(load_task_item.count)
                priority.append(load_task_item.priority)
                latest_order_time.append(load_task_item.latest_order_time)
                out_stock_code.append(load_task_item.outstock_code)
        df.insert(0, 'commodity', commodity)
        df.insert(0, 'big_commodity', big_commodity)
        df.insert(0, 'out_stock_code', out_stock_code)
        df.insert(0, 'consumer', consumer)
        df.insert(0, 'end_point', end_point)
        df.insert(0, 'city', city)
        df.insert(0, 'priority', priority)
        df.insert(0, 'load_task_type', load_task_type)
        df.insert(0, 'priority_grade', priority_grade)
        df.insert(0, 'latest_order_time', latest_order_time)
        df.insert(0, 'count', count)
        df.insert(0, 'weight', weight)
        df.insert(0, 'total_weight', total_weight)
        df.insert(0, 'truck_id', truck_id)
    df.to_excel('result-result_load_task_list.xls')

    # 将tail_list导出为excel
    df = pd.DataFrame()
    if tail_list:
        actual_number = []
        actual_weight = []
        big_commodity_name = []
        commodity_name = []
        city = []
        dlv_spot_name_end = []
        consumer = []
        deliware_house = []
        stock: Stock
        for stock in tail_list:
            actual_number.append(stock.actual_number)
            actual_weight.append(stock.actual_weight)
            big_commodity_name.append(stock.big_commodity_name)
            commodity_name.append(stock.commodity_name)
            city.append(stock.city)
            consumer.append(stock.consumer)
            deliware_house.append(stock.deliware_house)
            dlv_spot_name_end.append(stock.dlv_spot_name_end)
        df.insert(0, 'deliware_house', deliware_house)
        df.insert(0, 'consumer', consumer)
        df.insert(0, 'dlv_spot_name_end', dlv_spot_name_end)
        df.insert(0, 'city', city)
        df.insert(0, 'commodity_name', commodity_name)
        df.insert(0, 'big_commodity_name', big_commodity_name)
        df.insert(0, 'actual_weight', actual_weight)
        df.insert(0, 'actual_number', actual_number)
    df.to_excel('result-tail_list.xls')

    # 将pick_list导出为excel
    df = pd.DataFrame()
    if pick_list:
        total_weight = []
        truck_num = []
        city = []
        end_point = []
        big_commodity = []
        commodity = []
        remark = []
        pick_task: PickTask
        for pick_task in pick_list:
            total_weight.append(pick_task.total_weight)
            truck_num.append(pick_task.truck_num)
            city.append(pick_task.city)
            end_point.append(pick_task.end_point)
            big_commodity.append(pick_task.big_commodity)
            commodity.append(pick_task.commodity)
            remark.append(pick_task.remark)
        df.insert(0, 'remark', remark)
        df.insert(0, 'commodity', commodity)
        df.insert(0, 'big_commodity', big_commodity)
        df.insert(0, 'end_point', end_point)
        df.insert(0, 'city', city)
        df.insert(0, 'truck_num', truck_num)
        df.insert(0, 'total_weight', total_weight)
    df.to_excel('result-pick_list.xls')


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
    all_stock_list = read_excel()
    if not all_stock_list:
        return []
    # 库存列表预处理
    df_stock = pd.DataFrame(all_stock_list)
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
        return []
    global flag
    flag = False

    def rename(row):
        global flag
        if not flag:
            flag = True
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
    # df_stock.loc[df_stock["deliware"].str.startswith("U"), ["standard_address"]] = df_stock["PORTNUM"]
    df_stock.loc[(df_stock["port_name_end"].isin(ModelConfig.RG_PORT_NAME_END_LYG)) & (
        df_stock["big_commodity_name"].isin(ModelConfig.RG_COMMODITY_LYG)), ["actual_end_point"]] = "U288-岚北港口库2LYG"
    # df_stock.loc[df_stock["standard_address"].isnull(), ["standard_address"]] = df_stock["detail_address"]
    # 筛选出西区的'新产品-卷板', '新产品-白卷'
    df_west_j_stock = df_stock.loc[df_stock["big_commodity_name"].isin(['新产品-卷板', '新产品-白卷'])]
    # 筛选出西区中除'新产品-卷板', '新产品-白卷'外的其他货物
    df_west_stock = df_stock.loc[
        df_stock["deliware_house"].isin(ModelConfig.RG_WAREHOUSE_GROUP[0]) & ~df_stock["big_commodity_name"].isin(
            ['新产品-卷板', '新产品-白卷'])]
    # 筛选出老区的'老区-卷板'
    df_old_j_stock = df_stock.loc[df_stock["big_commodity_name"] == "老区-卷板"]
    # 筛选出老区中除'老区-卷板'外的其他货物
    df_old_stock = df_stock.loc[
        df_stock["deliware_house"].isin(ModelConfig.RG_WAREHOUSE_GROUP[1]) & (
                    df_stock["big_commodity_name"] != "老区-卷板")]
    return split_pick_stock(df_west_stock), split_pick_stock(df_old_stock), split_pick_stock(
        df_west_j_stock), split_pick_stock(df_old_j_stock)


def split_pick_stock(df_stock):
    """
    货物切分
    :param df_stock:
    :return:
    """
    dic = df_stock.to_dict(orient="record")
    # 存放stock的结果
    stock_list = []
    for record in dic:
        stock = Stock(record)
        # 如果可发小于待发，并且待发在标载范围内，就不参与配载
        if stock.actual_number < stock.waint_fordel_number and get_lower_limit(stock.big_commodity_name) <= \
                stock.waint_fordel_weight <= ModelConfig.RG_MAX_WEIGHT:
            continue
        stock.parent_stock_id = get_stock_id(stock)
        stock.actual_number = int(stock.actual_number)
        stock.actual_weight = int(stock.actual_weight)
        stock.piece_weight = int(stock.piece_weight)
        stock.wait_product_weight = int(stock.wait_product_weight)
        stock.priority = ModelConfig.RG_PRIORITY.get(stock.priority, 4)
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
        # 首先去除 件重大于33000的货物
        if target_num < 1:
            continue
        # 其次如果可装的件数大于实际可发件数，不用拆分，直接添加到stock_list列表中
        elif target_num > stock.actual_number:
            # 可装的件数大于实际可发件数，并且达到标载
            if stock.actual_weight >= get_lower_limit(stock.big_commodity_name):
                stock.limit_mark = 1
            else:
                stock.limit_mark = 0
            stock_list.append(stock)
        # 最后不满足则拆分
        else:
            for q in range(int(target_group_num)):
                copy_2 = copy.deepcopy(stock)
                copy_2.actual_weight = target_num * stock.piece_weight
                copy_2.actual_number = int(target_num)
                if copy_2.actual_weight < get_lower_limit(stock.big_commodity_name):
                    copy_2.limit_mark = 0
                else:
                    copy_2.limit_mark = 1
                stock_list.append(copy_2)
            if target_left_num:
                copy_1 = copy.deepcopy(stock)
                copy_1.actual_number = int(target_left_num)
                copy_1.actual_weight = target_left_num * stock.piece_weight
                if copy_1.actual_weight < get_lower_limit(stock.big_commodity_name):
                    copy_1.limit_mark = 0
                else:
                    copy_1.limit_mark = 1
                stock_list.append(copy_1)
    # 按照优先发运和最新挂单时间排序
    # stock_list.sort(key=lambda x: (x.priority, x.latest_order_time, x.actual_weight * -1), reverse=False)
    return stock_list


def read_excel():
    # 打开excel表，填写路径
    book = xlrd.open_workbook("../static/10月14日8时库存可发量.xls")
    # 找到sheet页
    table = book.sheet_by_name("Sheet2")
    # 获取总行数总列数
    row_Num = table.nrows
    col_Num = table.ncols
    list = []
    key = table.row_values(0)  # 这是第一行数据，作为字典的key值

    if row_Num <= 1:
        print("没数据")
        return list
    for i in range(1, row_Num):
        row_dict = {}
        values = table.row_values(i)
        for x in range(col_Num):
            # 把key值对应的value赋值给key，每行循环
            row_dict[key[x]] = values[x]
        # 把字典加到列表中
        list.append(row_dict)
    print(list)
    return list


if __name__ == '__main__':
    dispatch()

