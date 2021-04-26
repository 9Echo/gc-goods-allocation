# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/10
from collections import defaultdict
from typing import List

import pandas as pd

from app.test.jjunf.pick_result_analysis.driver import Driver
from app.test.jjunf.pick_result_analysis.get_date import get_date_before_830, get_date
from app.test.jjunf.pick_result_analysis.get_date_2 import get_one_day_before_now, get_one_day_before_830, \
    get_time_interval
from app.test.jjunf.pick_result_analysis.pick_analysis_config import PickAnalysisConfig
from app.test.jjunf.pick_result_analysis.stock import Stock, Gather, ModelPick, HourStock
from app.test.jjunf.pick_result_analysis.stock_dao_2 import select_dao_2
from datetime import datetime

from app.test.jjunf.pick_result_analysis.stock_dao import select_dao
from app.util.percent_util import percent_util
from app.util.split_group_util import split_group_util
from model_config import ModelConfig


def pick_result_analysis():
    T1 = datetime.now()
    print("T1 = ", T1)
    # 查询模型生成、人工录入和人工调度的详细信息
    df_pick_result = select_dao_2.select_pick_result()
    T2 = datetime.now()
    print("T2 = ", T2)
    # 查询推荐司机信息
    df_driver = select_dao_2.select_driver()
    T3 = datetime.now()
    print("T3 = ", T3)
    # # 查询模型生成的摘单计划
    # df_model_pick = select_dao.select_model_pick()
    T4 = datetime.now()
    print("T4 = ", T4)
    # 每天的结果
    pick_result_dic = df_pick_result.to_dict(orient="record")
    pick_result_list = [Stock(obj) for obj in pick_result_dic]
    # 如果pick_result_list为空
    if not pick_result_list:
        print('无结果！')
        return None
    # 司机
    driver_dic = df_driver.to_dict(orient="record")
    driver_list = [Driver(obj) for obj in driver_dic]
    # 模型生成的计划
    # model_pick_dic = df_model_pick.to_dict(orient="record")
    # model_pick_list = [ModelPick(obj) for obj in model_pick_dic]
    # # 对model_pick_list的预处理
    # model_pick_list = deal_model_pick(model_pick_list)
    # 对pick_result_list的预处理
    pick_result_list = early_deal(pick_result_list, driver_list)
    # 1模型生成的
    model_list = [stock for stock in pick_result_list if stock.fs == '模型生成']
    # 删除掉相同车次的记录
    model_list = delete_same_trains_no(model_list)
    model_dict = split_group(model_list)
    # 2人工录入的
    people_add_list = [stock for stock in pick_result_list if stock.fs == '人工录入']
    # 删除掉相同车次的记录
    people_add_list = delete_same_trains_no(people_add_list)
    people_add_dict = split_group(people_add_list)
    # 3人工调度的(由于跨区的，需要开两个单子，司机抢单后需要人工在补一个单子，所以从人工调度中去除跟模型生成、人工录入中车次号相同的)
    #           (跨区的，一般都是一个车次号，但是可能由于调度人员误操作，生成了两个车次号，因此再判断一下司机是否相同)
    # 模型生成的车次号列表
    model_trains_list = [train.trains_no for train in model_list]
    # 人工录入的车次号列表
    people_add_trains_list = [train.trains_no for train in people_add_list]
    # 模型生成的司机id列表
    model_drivers_list = [driver.driver_id for driver in model_list]
    # 人工录入的司机id列表
    people_add_drivers_list = [driver.driver_id for driver in people_add_list]
    people_dispatch_list = [stock for stock in pick_result_list
                            if stock.fs == '人工调度'
                            and stock.trains_no not in model_trains_list
                            and stock.trains_no not in people_add_trains_list
                            and stock.driver_id not in model_drivers_list
                            and stock.driver_id not in people_add_drivers_list
                            ]
    # 删除掉相同车次的记录
    people_dispatch_list = delete_same_trains_no(people_dispatch_list)
    people_dispatch_dict = split_group(people_dispatch_list)

    # 总车次数m
    m = len(model_list) + len(people_add_list) + len(people_dispatch_list)
    # 按调度来源汇总
    gather_model_list = get_gather(model_dict, '摘单调度', '模型生成', m, len(model_list))
    gather_people_add_list = get_gather(people_add_dict, '摘单调度', '人工录入', m, len(people_add_list))
    gather_people_dispatch_list = get_gather(people_dispatch_dict, '人工调度', '人工调度', m, len(people_dispatch_list))
    '''汇总中的信息：统计时间、统计路线、统计车次数'''
    str_list = gather_information(m, people_dispatch_list, model_list, people_add_list)
    # 导出excel
    to_excel(str_list, gather_model_list + gather_people_add_list + gather_people_dispatch_list, pick_result_list,
             driver_list, len(model_list) + len(people_add_list))


def delete_same_trains_no(temp_list):
    """
    删除掉相同车次的记录
    :param temp_list:
    :return:
    """
    if not temp_list:
        return temp_list
    result_list = []
    temp_dict = split_group_util(temp_list, ['trains_no'])
    for value_list in temp_dict.values():
        temp = value_list[0]
        # 取重量最大的一个
        for item in value_list:
            if item.plan_weight > temp.plan_weight:
                temp = item
        result_list.append(temp)
    return result_list


def gather_information(m, people_dispatch_list, model_list, people_add_list):
    """
    汇总中的信息：统计时间、统计路线、统计车次数
    :param m:
    :param people_dispatch_list:
    :param model_list:
    :param people_add_list:
    :return:
    """
    str_list = []
    # 标题
    title_str = '摘单分货情况分析'  # '会好运单车4条线路摘单情况分析'
    str_list.append(title_str)
    # 统计时间
    if PickAnalysisConfig.DAY_TOTAL == 1:
        time_str = '统计时间：' + str(get_one_day_before_now()) + ' - 23:59:59'
    else:
        time_str = '统计时间：' + str(get_one_day_before_now(PickAnalysisConfig.DAY_TOTAL - 1)) + ' - ' + str(
            get_one_day_before_now().strftime("%Y-%m-%d")) + ' 23:59:59'
    str_list.append(time_str)
    # 统计路线
    # 如果需要查询淄博的
    if PickAnalysisConfig.ZIBO_FLAG:
        route_str = '统计线路：滨州、菏泽、济南、淄博'
    # 不查询淄博的
    else:
        route_str = '统计线路：滨州、菏泽、济南（淄博不走摘单）'
    str_list.append(route_str)
    # 统计车次数
    truck_str = ('总共发运' + str(m) + '车次：其中人工调度APP派单' + str(len(people_dispatch_list)) + '车，' +
                 '通过摘单功能司机摘单' + str(len(model_list) + len(people_add_list)) + '车，其中模型分货摘单' +
                 str(len(model_list)) + '车')
    str_list.append(truck_str)
    # 本次结果查询时间
    now_time_str = '本次结果查询时间：' + datetime.now().strftime("%Y-%m-%d %H:%M")
    str_list.append(now_time_str)
    return str_list


def early_deal(pick_result_list: List[Stock], driver_list):
    # 对pick_result_list的预处理
    for pick in pick_result_list:
        # 1判断调度来源
        if pick.plan_source == 'DDLY40':
            pick.fs = '人工调度'
        else:
            if not pick.source_name:
                pick.fs = '人工录入'
            else:
                pick.fs = '模型生成'
        # 2判断司机是否是此区县的推荐司机
        for driver in driver_list:
            if pick.driver_id == driver.user_id and pick.district == driver.district:
                pick.vehicle_type = '推荐范围车辆'
                break
        if not pick.vehicle_type:
            pick.vehicle_type = '新车'
        # 3判断是否跨厂区
        # 默认为否
        pick.two_factory = '否'
        for temp_pick in pick_result_list:
            # 如果只查询1天的数据，除了车次号相同的为跨厂区之外，由于操作人员操作不当也有车次号不同但司机id相同的其实也是跨厂区
            # 又考虑到一个司机一天也可能拉两次或多次货物，所以在判断司机id相同时，需要两次的载重小于一个阈值
            if (PickAnalysisConfig.DAY_TOTAL == 1 and
                    (pick.trains_no == temp_pick.trains_no or
                     pick.driver_id == temp_pick.driver_id and
                     pick.plan_weight + temp_pick.plan_weight < PickAnalysisConfig.WEIGHT_UP_LIMIT) and
                    pick.commodity_name.split('-')[0] != temp_pick.commodity_name.split('-')[0]):
                pick.two_factory = '是'
            # 如果查询多天的数据，就不能只看司机id是否相同了，还要考虑时间
            elif ((pick.trains_no == temp_pick.trains_no or
                   pick.driver_id == temp_pick.driver_id
                   and (max(pick.create_date, temp_pick.create_date) - min(pick.create_date,
                                                                           temp_pick.create_date) < get_time_interval())
                   and pick.plan_weight + temp_pick.plan_weight < PickAnalysisConfig.WEIGHT_UP_LIMIT) and
                  pick.commodity_name.split('-')[0] != temp_pick.commodity_name.split('-')[0]):
                pick.two_factory = '是'
        # 4对于空车牌的说明
        if not pick.vehicle_no:
            pick.vehicle_no = ('当前状态为' +
                               PickAnalysisConfig.PLAN_STATUS_DICT.get(pick.plan_status, pick.plan_status) +
                               ',未提供车牌')
        if pick.fs != '模型生成':
            """
            模型生成备注：
            模型生成时间：
            模型与实际的区别：'有优先发运需求'、'指定特殊发运规格'
            说明：'此时模型还未生成摘单计划'、滨州不需要模型分配的货物、'滨州跨厂区的不需要模型生成'、'大吨位'、'模型生成时无库存数据'
            """
            '''跨区县、城市拼货'''
            # 统计标注中区县的个数
            district_num = 0
            for district in PickAnalysisConfig.POINT[0]:
                if pick.remark.find(district) != -1:
                    district_num += 1
            if district_num >= 2:
                pick.explain = '跨区县拼货'
                continue
            # 统计标注中城市的个数
            city_num = 0
            for city in PickAnalysisConfig.POINT[1]:
                if pick.remark.find(city) != -1:
                    city_num += 1
            if city_num >= 2:
                pick.explain = '跨城市拼货'
                continue
            '''接下线'''
            if judge_if_in_hour_stock(pick.city, pick.district, pick.commodity_name, pick.create_date,
                                      pick.plan_weight):
                pick.explain = '接下线'
                continue
            '''在模型生成摘单计划之前发布的计划'''
            if str(get_one_day_before_now()) < str(pick.create_date) < str(get_one_day_before_830()):
                pick.explain_list.append('此时模型还未生成摘单计划')
            '''滨州不需要模型分配的货物：窄带、冷板'''
            if pick.city == '滨州市' and pick.commodity_name not in ModelConfig.RG_BZ_GROUP:
                pick.explain_list.append('滨州市的' + pick.commodity_name + '不需要模型生成')
            '''滨州跨厂区的不需要模型生成'''
            if pick.city == '滨州市' and pick.two_factory == '是':
                pick.explain_list.append('滨州跨厂区的不需要模型生成')
            '''模型生成时无库存数据：接下线、库存延迟'''
            # 查询模型摘单分货记录中的 城市，区县，品种  拼接的列表
            pick_concat_list = select_dao_2.select_pick_concat()
            concat_str = pick.city + ',' + pick.district + ',' + pick.commodity_name
            # 城市、区县、品种相同，但是无货
            if concat_str not in pick_concat_list:
                pick.explain_list.append('模型生成时无库存数据')
            '''大吨位'''
            if pick.city != '滨州市' and pick.plan_weight > 35.5:
                pick.explain_list.append('大吨位')
            if not pick.explain_list:
                '''优先发运'''
                if pick.remark.find('优先') != -1:
                    pick.difference = '有优先发运需求'
                '''特殊规格'''
                if pick.remark.find('规格') != -1:
                    pick.difference = '指定特殊发运规格'
            # 查找模型生成的备注、日期
            if not pick.explain_list or (len(pick.explain_list) == 1 and '大吨位' in pick.explain_list):
                pick.model_remark = '，'.join(
                    select_dao_2.select_pick_remark(pick.city, pick.district, pick.commodity_name))
                # pick.model_remark不为空，即模型生成了摘单计划
                if pick.model_remark:
                    pick.model_create_data = select_dao_2.select_model_create_data()
        pick.explain = ','.join(pick.explain_list)
    return pick_result_list


def judge_if_in_hour_stock(city, district, commodity: str, create_date, plan_weight):
    # 判断是否接下线（无库存数据）：接下线返回1，否则返回0
    big_commodity = ModelConfig.PICK_REMARK.get(commodity, commodity)
    date = datetime.strftime(create_date, "%Y-%m-%d %H:00:00")
    T5 = datetime.now()
    # 按条件查询库存
    hour_stock = select_dao.select_hour_stock(city, district, big_commodity, date)
    T6 = datetime.now()
    print("T = ", T6 - T5)
    if not hour_stock:
        return 1
    # 获取库存列表
    df_stock = pd.DataFrame(hour_stock)
    df_stock["CANSENDWEIGHT"] = df_stock["CANSENDWEIGHT"].astype('float64')
    df_stock["CANSENDNUMBER"] = df_stock["CANSENDNUMBER"].astype('int64')
    df_stock["NEED_LADING_WT"] = df_stock["NEED_LADING_WT"].astype('float64')
    df_stock["NEED_LADING_NUM"] = df_stock["NEED_LADING_NUM"].astype('int64')
    df_stock["OVER_FLOW_WT"] = df_stock["OVER_FLOW_WT"].astype('float64')
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
    df_stock["actual_weight"] = df_stock["actual_weight"] / float(1000)
    # 筛选出大于0的数据
    df_stock = df_stock.loc[
        (df_stock["actual_weight"] > 0) & (df_stock["actual_number"] > 0) & (
            df_stock["latest_order_time"].notnull())]
    if df_stock.empty:
        return 1

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
    dic = df_stock.to_dict(orient="record")
    hour_stock_list = [HourStock(obj) for obj in dic]
    # 按城市、区县、品种分组计算总重量
    hour_stock_dict = {}
    for stock in hour_stock_list:
        key = stock.city + ',' + stock.district + ',' + stock.big_commodity_name
        if key in hour_stock_dict.keys():
            hour_stock_dict[key] += stock.actual_weight
        else:
            hour_stock_dict[key] = stock.actual_weight
    # # 判断是否有库存
    # 库存不足（为了防止误差plan_weight - 2）
    if hour_stock_dict[city + ',' + district + ',' + commodity] < plan_weight - 2:
        return 1
    else:
        return 0


def to_excel(str_list, gather_list, pick_result_list, driver_list, zd_num):
    # df列表
    df_list = []
    # sheet名称列表
    sheet_list = []
    # 将gather_list导出为excel
    if gather_list:
        df_gather = pd.DataFrame()
        str1 = []
        str2 = []
        city = []
        district = []
        commodity_name = []
        number = []
        total_number = []
        percent = []
        gather: Gather
        for gather in gather_list:
            str1.append(gather.str1)
            str2.append(gather.str2)
            city.append(gather.city)
            district.append(gather.district)
            commodity_name.append(gather.commodity_name)
            number.append(gather.number)
            total_number.append(gather.total_number)
            percent.append(gather.percent)
        # 添加汇总中的信息：统计时间、统计路线、统计车次数
        str1.append('')
        str1.append('')
        str1.extend(str_list)
        for i in range(2 + len(str_list)):
            str2.append('')
            city.append('')
            district.append('')
            commodity_name.append('')
            number.append('')
            total_number.append('')
            percent.append('')
        df_gather.insert(0, '占比', percent)
        df_gather.insert(0, '汇总', total_number)
        df_gather.insert(0, '车次数', number)
        df_gather.insert(0, '品种', commodity_name)
        df_gather.insert(0, '区县', district)
        df_gather.insert(0, '城市', city)
        df_gather.insert(0, '', str2)
        df_gather.insert(0, '调度来源', str1)
        # df_gather.to_excel('gather.xls', sheet_name='总汇')
        df_list.append(df_gather)
        sheet_list.append('总汇')

    # 将pick_result_list导出为excel
    if pick_result_list:
        df_pick = pd.DataFrame()
        fs = []
        trains_no = []
        plan_weight = []
        driver_id = []
        vehicle_no = []
        remark = []
        city = []
        district = []
        commodity_name = []
        create_date = []
        pickup_time = []
        pick_create_date = []
        pick_time_interval = []
        vehicle_type = []
        two_factory = []
        model_remark = []
        model_create_data = []
        difference = []
        explain = []
        pickup_no = []
        adjacent_transport = []
        message_propel = []
        pick: Stock
        for pick in pick_result_list:
            fs.append(pick.fs)
            trains_no.append(pick.trains_no)
            plan_weight.append(pick.plan_weight)
            driver_id.append(pick.driver_id)
            vehicle_no.append(pick.vehicle_no)
            remark.append(pick.remark)
            city.append(pick.city)
            district.append(pick.district)
            commodity_name.append(pick.commodity_name)
            create_date.append(pick.create_date)
            pickup_time.append(pick.pickup_time)
            pick_create_date.append(pick.pick_create_date)
            pick_time_interval.append(pick.pick_time_interval)
            vehicle_type.append(pick.vehicle_type)
            two_factory.append(pick.two_factory)
            model_remark.append(pick.model_remark)
            model_create_data.append(pick.model_create_data)
            difference.append(pick.difference)
            explain.append(pick.explain)
            pickup_no.append(pick.pickup_no)
            adjacent_transport.append(pick.adjacent_transport)
            message_propel.append(pick.message_propel)
        # 如果没有摘单记录时，不导出摘单相关的字段
        if zd_num:
            df_pick.insert(0, '是否消息推送后摘单', message_propel)
            df_pick.insert(0, '是否相邻线路运力', adjacent_transport)
            df_pick.insert(0, '摘单号', pickup_no)
            df_pick.insert(0, '说明', explain)
            df_pick.insert(0, '模型与实际的区别', difference)
            df_pick.insert(0, '模型生成时间', model_create_data)
            df_pick.insert(0, '模型生成备注', model_remark)
        df_pick.insert(0, '是否跨厂区', two_factory)
        df_pick.insert(0, '车辆类型', vehicle_type)
        if zd_num:
            df_pick.insert(0, '摘单时间段', pick_time_interval)
            df_pick.insert(0, '摘单下发时间', pick_create_date)
            df_pick.insert(0, '摘单时间', pickup_time)
        df_pick.insert(0, '创建时间', create_date)
        df_pick.insert(0, '品种', commodity_name)
        df_pick.insert(0, '区县', district)
        df_pick.insert(0, '城市', city)
        df_pick.insert(0, '备注', remark)
        df_pick.insert(0, '车牌', vehicle_no)
        df_pick.insert(0, '司机id', driver_id)
        df_pick.insert(0, '重量', plan_weight)
        df_pick.insert(0, '车次号', trains_no)
        df_pick.insert(0, '调度来源', fs)
        # df_pick.to_excel('pick_result.xls', sheet_name='模型生成+人工录入+人工调度')
        df_list.append(df_pick)
        if zd_num:
            sheet_list.append('模型生成+人工录入+人工调度')
        else:
            sheet_list.append('人工调度')

    # 如果没有摘单记录时，不导出推荐司机列表
    if zd_num:
        # 将driver_list导出为excel
        if driver_list:
            df_driver = pd.DataFrame()
            user_id = []
            name = []
            vehicle_no = []
            city = []
            district = []
            driver: Driver
            for driver in driver_list:
                user_id.append(driver.user_id)
                name.append(driver.name)
                vehicle_no.append(driver.vehicle_no)
                city.append(driver.city)
                district.append(driver.district)
            df_driver.insert(0, '区县', district)
            df_driver.insert(0, '城市', city)
            df_driver.insert(0, '车牌号', vehicle_no)
            df_driver.insert(0, '司机', name)
            df_driver.insert(0, '司机id', user_id)
            # df_driver.to_excel('driver.xls', sheet_name='各线路推荐司机列表')
            df_list.append(df_driver)
            sheet_list.append('各线路推荐司机列表')

    # df = obj_to_df(pick_result_list)
    # df.to_excel('pick_result_list.xls', sheet_name='各线路推荐司机列表', index=False)

    # 指定excel的名字
    if PickAnalysisConfig.DAY_TOTAL == 1:
        excel_name = str(get_one_day_before_now().strftime("%m%d")) + '摘单分析详情.xls'
    else:
        excel_name = str(get_one_day_before_now(PickAnalysisConfig.DAY_TOTAL - 1).strftime("%m%d")) + '-' + str(
            get_one_day_before_now().strftime("%m%d")) + '摘单分析详情.xls'
    # 创建空的excel
    df = pd.DataFrame()
    df.to_excel(excel_name)
    # 将多个sheet保存到一个excel中
    with pd.ExcelWriter(excel_name) as writer:
        # df_list = [df_gather, df_pick, df_driver]
        # sheet_list = ['总汇', '模型生成+人工录入+人工调度', '各线路推荐司机列表']
        for i in range(len(df_list)):
            df_list[i].to_excel(writer, sheet_name=sheet_list[i], index=False)
    writer.save()
    writer.close()


def obj_to_df(obj_list: List[object]):
    df = pd.DataFrame()
    dic = defaultdict(list)
    for obj in obj_list:
        for attr in obj.__dict__.keys():
            dic[attr].append(getattr(obj, attr))
    key_list: List = list(dic.keys())
    key_list.reverse()
    for key in key_list:
        df.insert(0, key, dic[key])
    return df


def get_gather(temp_dict, attr1, attr2, m, n):
    """
    汇总结果
    :param temp_dict:
    :param attr1:
    :param attr2:
    :param m: 总车次数
    :param n: 当前类别车次数
    :return:
    """
    gather_list = []
    for temp_list in temp_dict.values():
        temp_stock: Stock
        temp_stock = temp_list[0]
        gather = Gather()
        gather.str1 = attr1
        gather.str2 = attr2
        gather.city = temp_stock.city
        gather.district = temp_stock.district
        gather.commodity_name = temp_stock.commodity_name
        gather.number = len(temp_list)
        gather.percent = percent_util(n, m, 2)
        gather_list.append(gather)
    # 汇总list中的总车次数
    for gather in gather_list:
        gather.total_number = sum([i.number for i in gather_list])
    return gather_list


def split_group(temp_list):
    """
    将temp_list按照城市、区县、品种分组为字典
    :param temp_list:
    :return:
    """
    temp_dict = defaultdict(list)
    pick: Stock
    for pick in temp_list:
        key = pick.city + ',' + pick.district + ',' + pick.commodity_name
        temp_dict[key].append(pick)
    return temp_dict


if __name__ == '__main__':
    pick_result_analysis()
