# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/24
from typing import List
import pandas as pd
from geopy import distance
from app.test.jjunf.pick_result_analysis.driver import Driver
from app.test.jjunf.pick_result_analysis.get_date_2 import get_one_day_before_now, get_time_interval
from app.test.jjunf.pick_result_analysis.pick_analysis_config import PickAnalysisConfig
from app.test.jjunf.pick_result_analysis.stock import Stock, Gather2
from app.test.jjunf.pick_result_analysis.stock_dao_2 import select_dao_2
from datetime import datetime
from app.util.obj_list_to_df_util import obj_list_to_df_util
from app.util.percent_util import percent_util
from app.util.round_util import round_util, round_util_by_digit
from app.util.split_group_util import split_group_util
from model_config import ModelConfig


def pick_result_analysis():
    print("start")
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
    # 对pick_result_list的预处理
    pick_result_list = early_deal(pick_result_list, driver_list)

    # 1.济南市的
    jn_list = [stock for stock in pick_result_list if stock.city == '济南市']
    # 删除掉相同车次的记录
    jn_list = delete_same_trains_no(jn_list)
    jn_dict = split_group_util(jn_list, ['fs', 'district', 'commodity_name'])
    gather_jn_list = get_gather(jn_dict, jn_list)
    # 2.滨州市的
    bz_list = [stock for stock in pick_result_list if stock.city == '滨州市']
    # 删除掉相同车次的记录
    bz_list = delete_same_trains_no(bz_list)
    bz_dict = split_group_util(bz_list, ['fs', 'district', 'commodity_name'])
    gather_bz_list = get_gather(bz_dict, bz_list)
    # 3.菏泽市的
    hz_list = [stock for stock in pick_result_list if stock.city == '菏泽市']
    # 删除掉相同车次的记录
    hz_list = delete_same_trains_no(hz_list)
    hz_dict = split_group_util(hz_list, ['fs', 'district', 'commodity_name'])
    gather_hz_list = get_gather(hz_dict, hz_list)
    # 4.淄博市的
    zb_list = [stock for stock in pick_result_list if stock.city == '淄博市']
    # 删除掉相同车次的记录
    zb_list = delete_same_trains_no(zb_list)
    zb_dict = split_group_util(zb_list, ['fs', 'district', 'commodity_name'])
    gather_zb_list = get_gather(zb_dict, zb_list)

    # 1模型生成的
    model_list = [stock for stock in pick_result_list if stock.fs == '模型生成']
    # 删除掉相同车次的记录
    model_list = delete_same_trains_no(model_list)
    # model_dict = split_group_util(model_list,['city'])
    # 2人工录入的
    people_add_list = [stock for stock in pick_result_list if stock.fs == '人工录入']
    # 删除掉相同车次的记录
    people_add_list = delete_same_trains_no(people_add_list)
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
    # 总车次数m
    m = len(model_list) + len(people_add_list) + len(people_dispatch_list)
    '''汇总中的信息：统计时间、统计路线、统计车次数'''
    str_list = gather_information(m, people_dispatch_list, model_list, people_add_list)
    # 导出excel
    to_excel(str_list, gather_jn_list + gather_bz_list + gather_hz_list + gather_zb_list, pick_result_list,
             driver_list, len(model_list) + len(people_add_list))
    T5 = datetime.now()
    print("T5 = ", T5)
    print("end")


def get_gather(temp_dict, stock_list):
    """
    汇总结果
    :param stock_list:
    :param temp_dict:
    :return:
    """
    # 1模型生成的
    model_list = [stock for stock in stock_list if stock.fs == '模型生成']
    # 2人工录入的
    people_add_list = [stock for stock in stock_list if stock.fs == '人工录入']
    # 3人工调度的
    people_dispatch_list = [stock for stock in stock_list if stock.fs == '人工调度']
    gather_list = []
    for temp_list in temp_dict.values():
        temp_stock: Stock
        temp_stock = temp_list[0]
        gather = Gather2()
        gather.city = temp_stock.city
        gather.district = temp_stock.district
        gather.commodity_name = temp_stock.commodity_name
        gather.stock_weight = select_dao_2.select_stock_weight_8(temp_stock.city, temp_stock.district,
                                                                 temp_stock.commodity_name)
        gather.need_truck_num = round_util(gather.stock_weight / 32)
        gather.fs = temp_stock.fs
        gather.truck_num = len(temp_list)
        gather_list.append(gather)
    # 汇总
    for gather in gather_list:
        # 计算各种调度方式的百分比、车次数
        if gather.fs == '模型生成':
            gather.percent = percent_util(len(model_list), len(stock_list), 2)
            gather.total_number = sum([i.truck_num for i in gather_list if i.fs == '模型生成'])
        elif gather.fs == '人工录入':
            gather.percent = percent_util(len(people_add_list), len(stock_list), 2)
            gather.total_number = sum([i.truck_num for i in gather_list if i.fs == '人工录入'])
        elif gather.fs == '人工调度':
            gather.percent = percent_util(len(people_dispatch_list), len(stock_list), 2)
            gather.total_number = sum([i.truck_num for i in gather_list if i.fs == '人工调度'])
    return gather_list


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
    # 说明
    explain_str = '分线路，统计以上时间段内，各线路的库存吨数、库存所需车辆数、调度来源、摘单情况如下所示：'
    str_list.append(explain_str)
    # # 本次结果查询时间
    # now_time_str = '本次结果查询时间：' + datetime.now().strftime("%Y-%m-%d %H:%M")
    # str_list.append(now_time_str)
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
                     pick.plan_weight + temp_pick.plan_weight < PickAnalysisConfig.WEIGHT_UP_LIMIT and
                     (max(pick.create_date, temp_pick.create_date) -
                      min(pick.create_date, temp_pick.create_date) < get_time_interval())) and
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
                               ',未提供车牌(' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ')')
        # 5计算摘单时APP距离日钢的距离
        if pick.fs != '人工调度':
            # 摘单时的纬度、经度
            first_tuple = (pick.latitude, pick.longitude)
            # 日钢的纬度、经度
            final_tuple = (ModelConfig.PICK_RG_LAT['日钢纬度'], ModelConfig.PICK_RG_LON['日钢经度'])
            # 计算距离：千米
            pick.distance = round_util_by_digit(distance.great_circle(first_tuple, final_tuple).km, 3) + '(km)'
        # 6下发摘单时选择的司机来源
        if pick.driver_type == 'SJLY10':
            pick.driver_type = '按模型动态推荐'
        elif pick.driver_type == 'SJLY20':
            pick.driver_type = '装点附近司机'
        elif pick.driver_type == 'SJLY30':
            pick.driver_type = '人工设定司机池'
        if pick.fs != '模型生成':
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
            # 统计标注中城市、区县的个数
            city_district_num = 0
            for city_district in PickAnalysisConfig.POINT[1] + PickAnalysisConfig.POINT[0]:
                if pick.remark.find(city_district) != -1:
                    city_district_num += 1
            if city_district_num >= 2:
                pick.explain_list.append('跨城市拼货')
            '''滨州不需要模型分配的货物：窄带、冷板'''
            if pick.city == '滨州市' and pick.commodity_name not in ModelConfig.RG_BZ_GROUP:
                pick.explain_list.append('滨州市的' + pick.commodity_name + '不需要模型生成')
            '''大吨位'''
            if pick.city != '滨州市' and pick.plan_weight > 35.5:
                pick.explain_list.append('大吨位')
            if not pick.explain_list:
                '''优先发运'''
                if pick.remark.find('优先') != -1:
                    pick.explain_list.append('有优先发运需求')
                '''特殊规格'''
                if pick.remark.find('规格') != -1:
                    pick.explain_list.append('指定特殊发运规格')
        pick.explain = ','.join(pick.explain_list)
    return pick_result_list


def to_excel(str_list, gather_list, pick_result_list, driver_list, zd_num):
    # df列表
    df_list = []
    # sheet名称列表
    sheet_list = []
    # # 将gather_list导出为excel
    # if gather_list:
    #     df_gather = pd.DataFrame()
    #     city = []
    #     fs = []
    #     district = []
    #     commodity_name = []
    #     stock_weight = []
    #     need_truck_num = []
    #     truck_num = []
    #     total_number = []
    #     percent = []
    #     gather: Gather2
    #     for gather in gather_list:
    #         city.append(gather.city)
    #         fs.append(gather.fs)
    #         district.append(gather.district)
    #         commodity_name.append(gather.commodity_name)
    #         stock_weight.append(gather.stock_weight)
    #         need_truck_num.append(gather.need_truck_num)
    #         truck_num.append(gather.truck_num)
    #         total_number.append(gather.total_number)
    #         percent.append(gather.percent)
    #     # 添加汇总中的信息：统计时间、统计路线、统计车次数
    #     city.append('')
    #     city.append('')
    #     city.extend(str_list)
    #     for i in range(2 + len(str_list)):
    #         fs.append('')
    #         district.append('')
    #         commodity_name.append('')
    #         stock_weight.append('')
    #         need_truck_num.append('')
    #         truck_num.append('')
    #         total_number.append('')
    #         percent.append('')
    #     df_gather.insert(0, '占比', percent)
    #     df_gather.insert(0, '汇总', total_number)
    #     df_gather.insert(0, '发运车次数', truck_num)
    #     df_gather.insert(0, '8点库存所需车辆数', need_truck_num)
    #     df_gather.insert(0, '8点库存吨数', stock_weight)
    #     df_gather.insert(0, '品种', commodity_name)
    #     df_gather.insert(0, '区县', district)
    #     df_gather.insert(0, '调度来源', fs)
    #     df_gather.insert(0, '城市', city)
    #     # df_gather.to_excel('gather.xls', sheet_name='总汇')
    #     df_list.append(df_gather)
    #     sheet_list.append('总汇')
    # # 将pick_result_list导出为excel
    # if pick_result_list:
    #     df_pick = pd.DataFrame()
    #     fs = []
    #     trains_no = []
    #     plan_weight = []
    #     driver_id = []
    #     vehicle_no = []
    #     remark = []
    #     city = []
    #     district = []
    #     commodity_name = []
    #     create_date = []
    #     pickup_time = []
    #     pick_create_date = []
    #     pick_time_interval = []
    #     vehicle_type = []
    #     two_factory = []
    #     explain = []
    #     driver_type = []
    #     dist = []
    #     pickup_no = []
    #     pick: Stock
    #     for pick in pick_result_list:
    #         fs.append(pick.fs)
    #         trains_no.append(pick.trains_no)
    #         plan_weight.append(pick.plan_weight)
    #         driver_id.append(pick.driver_id)
    #         vehicle_no.append(pick.vehicle_no)
    #         remark.append(pick.remark)
    #         city.append(pick.city)
    #         district.append(pick.district)
    #         commodity_name.append(pick.commodity_name)
    #         create_date.append(pick.create_date)
    #         pickup_time.append(pick.pickup_time)
    #         pick_create_date.append(pick.pick_create_date)
    #         pick_time_interval.append(pick.pick_time_interval)
    #         vehicle_type.append(pick.vehicle_type)
    #         two_factory.append(pick.two_factory)
    #         explain.append(pick.explain)
    #         driver_type.append(pick.driver_type)
    #         dist.append(pick.distance)
    #         pickup_no.append(pick.pickup_no)
    #     # 如果没有摘单记录时，不导出摘单相关的字段
    #     if zd_num:
    #         df_pick.insert(0, '摘单号', pickup_no)
    #         df_pick.insert(0, '摘单时APP距离日钢的距离', dist)
    #         df_pick.insert(0, '司机来源', driver_type)
    #     #     df_pick.insert(0, '说明', explain)
    #     df_pick.insert(0, '是否跨厂区', two_factory)
    #     df_pick.insert(0, '车辆类型', vehicle_type)
    #     if zd_num:
    #         df_pick.insert(0, '摘单时间段', pick_time_interval)
    #         df_pick.insert(0, '摘单下发时间', pick_create_date)
    #         df_pick.insert(0, '摘单时间', pickup_time)
    #     df_pick.insert(0, '创建时间', create_date)
    #     df_pick.insert(0, '品种', commodity_name)
    #     df_pick.insert(0, '区县', district)
    #     df_pick.insert(0, '城市', city)
    #     df_pick.insert(0, '备注', remark)
    #     df_pick.insert(0, '车牌', vehicle_no)
    #     df_pick.insert(0, '司机id', driver_id)
    #     df_pick.insert(0, '重量', plan_weight)
    #     df_pick.insert(0, '车次号', trains_no)
    #     df_pick.insert(0, '调度来源', fs)
    #     # df_pick.to_excel('pick_result.xls', sheet_name='模型生成+人工录入+人工调度')
    #     df_list.append(df_pick)
    #     if zd_num:
    #         sheet_list.append('模型生成+人工录入+人工调度')
    #     else:
    #         sheet_list.append('人工调度')
    # # 如果没有摘单记录时，不导出推荐司机列表
    # if zd_num:
    #     # 将driver_list导出为excel
    #     if driver_list:
    #         df_driver = pd.DataFrame()
    #         user_id = []
    #         name = []
    #         vehicle_no = []
    #         city = []
    #         district = []
    #         driver: Driver
    #         for driver in driver_list:
    #             user_id.append(driver.user_id)
    #             name.append(driver.name)
    #             vehicle_no.append(driver.vehicle_no)
    #             city.append(driver.city)
    #             district.append(driver.district)
    #         df_driver.insert(0, '区县', district)
    #         df_driver.insert(0, '城市', city)
    #         df_driver.insert(0, '车牌号', vehicle_no)
    #         df_driver.insert(0, '司机', name)
    #         df_driver.insert(0, '司机id', user_id)
    #         # df_driver.to_excel('driver.xls', sheet_name='各线路推荐司机列表')
    #         df_list.append(df_driver)
    #         sheet_list.append('各线路推荐司机列表')

    '''1.将gather_list导出为excel'''
    gather_attr_dict = {
        'city': '城市',
        'fs': '调度来源',
        'district': '区县',
        'commodity_name': '品种',
        'stock_weight': '8点库存吨数',
        'need_truck_num': '8点库存所需车辆数',
        'truck_num': '发运车次数',
        'total_number': '汇总',
        'percent': '占比'
    }
    df_gather = obj_list_to_df_util(gather_list, gather_attr_dict)
    # 将gather_attr_dict的键值对互换为str_dict
    str_dict = {}
    for i in range(len(gather_attr_dict.values())):
        str_dict[list(gather_attr_dict.values())[i]] = list(gather_attr_dict.keys())[i]
    '''添加str_list的内容-start'''
    for i in range(len(str_list)):
        # 空一行
        if i == 0:
            for j in range(len(str_dict.keys())):
                str_dict[list(str_dict.keys())[j]] = ''
            df_str = pd.DataFrame(str_dict, index=[i])
            df_gather = df_gather.append(df_str, ignore_index=True, sort=False)
        # 添加str_list中每行的内容
        for j in range(len(str_dict.keys())):
            if j == 0:
                str_dict[list(str_dict.keys())[j]] = str_list[i]
            else:
                str_dict[list(str_dict.keys())[j]] = ''
        df_str = pd.DataFrame(str_dict, index=[i])
        df_gather = df_gather.append(df_str, ignore_index=True, sort=False)
    '''添加str_list的内容-end'''
    df_list.append(df_gather)
    sheet_list.append('总汇')
    '''2.将pick_result_list导出为excel'''
    if pick_result_list:
        if zd_num:
            pick_attr_dict = {
                'fs': '调度来源',
                'trains_no': '车次号',
                'plan_weight': '重量',
                'driver_id': '司机id',
                'vehicle_no': '车牌',
                'remark': '备注',
                'city': '城市',
                'district': '区县',
                'commodity_name': '品种',
                'create_date': '创建时间',
                'pickup_time': '摘单时间',
                'pick_create_date': '摘单下发时间',
                'pick_time_interval': '摘单时间段',
                'vehicle_type': '车辆类型',
                'two_factory': '是否跨厂区',
                'driver_type': '司机来源',
                'distance': '摘单时APP距离日钢的距离',
                'pickup_no': '摘单号'
            }
        # 如果没有摘单记录时，不导出摘单相关的字段
        else:
            pick_attr_dict = {
                'fs': '调度来源',
                'trains_no': '车次号',
                'plan_weight': '重量',
                'driver_id': '司机id',
                'vehicle_no': '车牌',
                'remark': '备注',
                'city': '城市',
                'district': '区县',
                'commodity_name': '品种',
                'create_date': '创建时间',
                'vehicle_type': '车辆类型',
                'two_factory': '是否跨厂区'
            }
        df_pick = obj_list_to_df_util(pick_result_list, pick_attr_dict)
        df_list.append(df_pick)
        if zd_num:
            sheet_list.append('模型生成+人工录入+人工调度')
        else:
            sheet_list.append('人工调度')
    '''3.将driver_list导出为excel'''
    # 如果没有摘单记录时，不导出推荐司机列表
    if zd_num and driver_list:
        driver_attr_dict = {
            'user_id': '司机id',
            'name': '司机',
            'vehicle_no': '车牌号',
            'city': '城市',
            'district': '区县'
        }
        df_driver = obj_list_to_df_util(driver_list, driver_attr_dict)
        df_list.append(df_driver)
        sheet_list.append('各线路推荐司机列表')

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
        for i in range(len(df_list)):
            df_list[i].to_excel(writer, sheet_name=sheet_list[i], index=False)
    writer.save()
    writer.close()


if __name__ == '__main__':
    pick_result_analysis()
    # # 摘单时的纬度、经度
    # first_tuple = (35.22559109491746, 119.3637822870587)
    # # 日钢的纬度、经度
    # final_tuple = (ModelConfig.PICK_RG_LAT['日钢纬度'], ModelConfig.PICK_RG_LON['日钢经度'])
    # distance = round_util_by_digit(distance.great_circle(first_tuple, final_tuple).km, 3) + '(km)'
    # print(distance)
