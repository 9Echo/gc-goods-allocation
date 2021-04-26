# -*- coding: utf-8 -*-
# Description: 创建摘单记录
# Created: jjunf 2020/10/26
from collections import defaultdict
from typing import List
import datetime

from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.pick_task import PickTask
from app.main.steel_factory.entity.pick_task_item import PickTaskItem
from app.util.date_util import get_now_date
from app.util.round_util import round_util
from app.util.uuid_util import UUIDUtil
from model_config import ModelConfig


def create_pick_task(load_task_list):
    """
    生成 摘单记录
    :param load_task_list:
    :return:
    """
    # 预处理，将load_task_list转换为pick_list
    pick_list, bz_old_compose_west_list = deal_pick_task(load_task_list)
    # 分组，将相同的记录合并，结果为字典：{键:[相同记录的列表]}
    pick_task_dict = split_group(pick_list)
    # 生成备注， 并且生成摘单计划
    pick_task_list = merge_and_remark(pick_task_dict)
    return pick_task_list, bz_old_compose_west_list


def deal_pick_task(load_task_list):
    # 预处理
    pick_list: List[PickTask] = []
    # 滨州的不需要老区配西区的结果不返回，直接推入的尾货列表
    bz_old_compose_west_list: List[LoadTaskItem] = []
    # 生成模板号的时间点
    time_code = get_now_date().strftime("%Y%m%d%H%M%S")
    for load_task in load_task_list:
        # pick_task对象
        pick_task = PickTask()
        pick_task.total_weight = load_task.total_weight
        pick_task.truck_num = 1
        pick_task.province = load_task.province
        pick_task.city = load_task.city
        pick_task.end_point = load_task.end_point
        pick_task.source_name = load_task.items[0].source_name
        # 卸点区县列表（主要是考虑到跨区县卸货时有多个区县）
        end_point_list = [i.end_point for i in load_task.items]
        for i in end_point_list:
            pick_task.district_set.add(i)
        item_list = []
        # 装点：宝华（西区）、厂内（老区）、岚北港
        # load_task.items的出库仓库在哪些区：按西区、老区、岚北港的顺序，0表示不在该区，1表示在该区，未知仓库将显示未知厂区
        item_district_dict = {'宝华': 0, '厂内': 0, '岚北港': 0, '未知厂区': 0}
        for item in load_task.items:
            item_list.append(item)
            '''找出load_task中有哪些区的货物'''
            # item中有西区的货物
            if item.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[0]:
                item_district_dict['宝华'] = 1
            # item中有老区的货物
            elif item.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[1]:
                item_district_dict['厂内'] = 1
            # item中有岚北港的货物
            elif item.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[2]:
                item_district_dict['岚北港'] = 1
            # else:
            #     item_district_dict['未知厂区'] = 1
            # 如果是济南市Z1、Z2库24h内的热卷，则标记：必须铁架子，
            if item.city == '济南市' and (item.outstock_code == 'Z1' or item.outstock_code == 'Z2'):
                time_now = datetime.datetime.now()
                time_last_day = (time_now - datetime.timedelta(hours=24)).__format__('%Y-%m-%d %H:%M:%S')
                if item.latest_order_time > time_last_day:
                    pick_task.hot_j = '必须铁架子，'
            # 特别的备注信息：(木托窄带)、(窄带卷)、(热镀锌)
            special_remark = get_special_remark(item)
            '''求出各品名的总件数、总重量-start'''
            key = item.big_commodity
            # 如果当前货物是滨州市的，并且是热镀锌钢卷，在备注中新产品-白卷、新产品-白卷(热镀锌)合并在一起计算总件数和总重量，最后加一个(包括热镀锌)的备注
            if item.city == '滨州市' and special_remark == '(热镀锌)':
                pick_task.bz_rdx = '(包括热镀锌)'
            else:
                # key为品名+特殊标记
                key += special_remark
            # 跨区县时，加上区县名称
            if len(pick_task.district_set)>1:
                key = item.end_point + key
            # 如果品名不在pick_task中
            if key not in pick_task.commodity_count.keys():
                pick_task.commodity_count[key] = item.count
                pick_task.commodity_weight[key] = item.weight
            # 如果品名在pick_task中
            else:
                pick_task.commodity_count[key] += item.count
                pick_task.commodity_weight[key] += item.weight
            '''求出各品名的总件数、总重量-end'''
        # 找出当前货物所在的厂区
        for key in item_district_dict.keys():
            if item_district_dict[key] == 1:
                pick_task.deliware_district.append(key)
        # # 滨州的不需要老区配西区，但是需要留出预留量，因此将其配载的结果不返回，而是直接推入尾货
        # if (load_task.city == '滨州市' and ModelConfig.RG_WAREHOUSE_NAME[0] in pick_task.deliware_district and
        #         ModelConfig.RG_WAREHOUSE_NAME[1] in pick_task.deliware_district):
        #     # 将LoadTask格式的数据转化成Stock
        #     bz_old_compose_west_list = swap_load_to_stock_rule.collect_to_list(load_task)
        #     continue
        # pick_task的明细pick_task_item对象
        pick_task_item_list: List[PickTaskItem] = []
        '''是否跨厂区、品种的处理-start'''
        # pick_task对象的品名是由明细的品名拼接而成的
        big_commodity_list = []
        commodity_list = []
        load_task_item = item_list[0]
        # 当货物都在1个厂区或者是新仓库len(pick_task.deliware_district)=0时
        if len(pick_task.deliware_district) <= 1:
            # 取重量最大的品种作为pick_task的品种字段
            for i in item_list:
                if i.weight > load_task_item.weight:
                    load_task_item = i
            # 创建pick_task的明细pick_task_item对象并赋值
            pick_task_item = create_pick_task_item(load_task, load_task_item, load_task.total_weight)
            pick_task_item_list.append(pick_task_item)
            # pick_task对象的货源名称、大品名、小品名赋值
            pick_task.source_name = load_task_item.source_name
            big_commodity_list.append(load_task_item.big_commodity)
            commodity_list.append(load_task_item.commodity)
        # 当货物在多个厂区时
        else:
            """    找出有多少个厂，每个厂区有哪些货物，如果有多种货物，按上面的取重量最大的品种作为品种    """
            # 标记跨厂区
            pick_task.group_flag = True
            dic = defaultdict(list)
            # 将item_list中的货物按厂区分组
            for i in item_list:
                # 宝华
                if i.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[0]:
                    dic['宝华'].append(i)
                # 厂内
                elif i.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[1]:
                    dic['厂内'].append(i)
                # 岚北港
                else:
                    dic['岚北港'].append(i)
            # pick_task的明细pick_task_item的生成
            for value_list in dic.values():
                load_task_item: LoadTaskItem = value_list[0]
                # 大小品名的赋值
                if load_task_item.big_commodity not in big_commodity_list:
                    big_commodity_list.append(load_task_item.big_commodity)
                if load_task_item.commodity not in commodity_list:
                    commodity_list.append(load_task_item.commodity)
                # 如果一个厂区有多条货物记录，取重量最大的
                if len(value_list) > 1:
                    for value in value_list:
                        if value.weight > load_task_item.weight:
                            load_task_item = value
                        # 大小品名的赋值
                        if value.big_commodity not in big_commodity_list:
                            big_commodity_list.append(value.big_commodity)
                        if value.commodity not in commodity_list:
                            commodity_list.append(value.commodity)
                # 明细pick_task_item的重量
                total_weight = sum([value_weight.weight for value_weight in value_list])
                # 创建pick_task的明细pick_task_item对象并赋值
                pick_task_item = create_pick_task_item(load_task, load_task_item, total_weight)
                pick_task_item_list.append(pick_task_item)
                # pick_task对象的货源名称赋值
                pick_task.source_name = load_task_item.source_name
        # pick_task对象的大品名、小品名赋值
        pick_task.big_commodity = ','.join(big_commodity_list)
        pick_task.commodity = ','.join(commodity_list)
        '''是否跨厂区、品种的处理-end'''
        '''生成模板号-start'''
        # 城市+时间点
        # city_code = ModelConfig.RG_DISTRICT_CODE.get(load_task.city, load_task.city)
        pick_task.template_no = load_task.city + '_' + time_code
        # 城市区县+厂区+品种+件数+重量
        # district_code = ModelConfig.RG_DISTRICT_CODE.get(load_task.city + load_task.end_point,
        #                                                  load_task.city + load_task.end_point)
        # warehouse_code_list = [ModelConfig.RG_WAREHOUSE_CODE.get(deliware, deliware)
        #                        for deliware in pick_task.deliware_district]
        # warehouse_code = '&'.join(warehouse_code_list)
        # commodity_code_list = [ModelConfig.RG_COMMODITY_CODE.get(key, key) +
        #                        str(round_util(pick_task.commodity_count[key])) +
        #                        str(round_util(pick_task.commodity_weight[key]))
        #                        for key in pick_task.commodity_count.keys()]
        # commodity_code = '&'.join(commodity_code_list)
        # pick_task.template_no = district_code + '_' + warehouse_code + '_' + commodity_code
        '''生成模板号-end'''
        # 在pick_task的items中添加摘单子明细列表pick_task_item_list
        pick_task.items.extend(pick_task_item_list)
        pick_list.append(pick_task)
    return pick_list, bz_old_compose_west_list


def split_group(pick_list):
    """
    按pick_list_item中的城市、区县、品种、件数、总重量、装点库区等分类
    :param pick_list:
    :return:
    """
    # 合并后的结果字典：{键:[相同记录的列表]}
    pick_task_dict = defaultdict(list)
    for pick_task in pick_list:
        # 分组条件
        key_list = [pick_task.city, pick_task.end_point]
        # 跨区县卸货时，将所有区县加入分组条件
        if len(pick_task.district_set)>1:
            district_list = [i for i in pick_task.district_set]
            district_list.sort()
            key_list.append(','.join(district_list))
        # # 如果装点为岚北港
        # if '岚北港' in pick_task.deliware_district:
        #     key_list.append(','.join(pick_task.deliware_district))
        # 装点厂区分组条件
        for deliware in pick_task.deliware_district:
            key_list.append(deliware)
        # # 如果是热卷，则加一个分组条件：必须铁架子，
        if pick_task.hot_j:
            key_list.append('必须铁架子，')
        # # 如果包括滨州市的热镀锌钢卷，则加一个分组条件：(包括热镀锌)
        # if pick_task.bz_rdx:
        #     key_list.append(pick_task.bz_rdx)
        # key_list.extend(pick_task.deliware_district)
        for commodity_key in pick_task.commodity_weight.keys():
            key_j = commodity_key.split('(')[0]
            # 单品种货物，分组时非卷类只看品种，不看件数
            if len(pick_task.commodity_weight) == 1:
                # 对于卷类：1件，24<=重量<29吨，给其备注时统称26吨左右
                if (key_j in ModelConfig.RG_J_GROUP and pick_task.commodity_count[commodity_key] == 1 and
                        ModelConfig.RG_SECOND_MIN_WEIGHT <= pick_task.commodity_weight[
                            commodity_key] * 1000 < ModelConfig.RG_J_MIN_WEIGHT):
                    pick_task.commodity_weight[commodity_key] = round_util(
                        ModelConfig.RG_J_PIECE_MIN_WEIGHT / 1000)
                    pick_task.total_weight = round_util(ModelConfig.RG_J_PIECE_MIN_WEIGHT / 1000)
                # 对于卷类:品种+件数
                if key_j in ModelConfig.RG_J_GROUP:
                    key_list.append(commodity_key + str(pick_task.commodity_count[commodity_key]))
                # 对于非卷类:品种
                else:
                    key_list.append(commodity_key)
            # 多品种货物，分组时看品种+件数
            else:
                key_list.append(commodity_key + str(pick_task.commodity_count[commodity_key]))
            # # 总重量分组条件
            # if (get_lower_limit(pick_task.big_commodity) <= pick_task.total_weight * 1000
            #         <= ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT):
            #     key_list.append('32')
            # else:
            #     key_list.append(str(pick_task.total_weight))
        # 必须排序：厂区、品种的先后顺序可能不一样
        key_list.sort()
        pick_task_dict[','.join(key_list)].append(pick_task)
    return pick_task_dict


def merge_and_remark(pick_task_dict):
    """
    生成备注， 并且生成摘单计划
    :param pick_task_dict:{键:[相同记录的列表]}
    :return:
    """
    # 将相同的记录合并， 生成摘单计划
    pick_task_list: List[PickTask] = []
    for key in pick_task_dict.keys():
        temp_pick_task = PickTask()
        # temp_pick_task中的明细列表
        temp_pick_task_item_list: List[PickTaskItem] = []
        # 备注
        remark = []
        # 一车的总件数
        count_list = []
        # 一车的总重量
        weight_list = []
        # 一车各品种的重量列表字典：{品种：[重量1，重量2]}
        commodity_weight_dict = defaultdict(list)
        # 如果是滨州市的热镀锌钢卷，则添加备注：(包括热镀锌)
        bz_rdx_str = ''
        # 如果是热卷，则添加备注：必须铁架子，
        hot_j_str = ''
        pick_task = PickTask()
        for pick_task in pick_task_dict[key]:
            temp_pick_task.pick_id = UUIDUtil.create_id('pick')
            temp_pick_task.total_weight += pick_task.total_weight
            temp_pick_task.truck_num = len(pick_task_dict[key])
            temp_pick_task.province = pick_task.province
            temp_pick_task.city = pick_task.city
            temp_pick_task.end_point = pick_task.end_point
            temp_pick_task.source_name = pick_task.source_name
            temp_pick_task.big_commodity = pick_task.big_commodity
            temp_pick_task.commodity = pick_task.commodity
            temp_pick_task.group_flag = pick_task.group_flag
            temp_pick_task.deliware_district = pick_task.deliware_district
            temp_pick_task.commodity_weight = pick_task.commodity_weight
            temp_pick_task.commodity_count = pick_task.commodity_count
            temp_pick_task.template_no = pick_task.template_no
            temp_pick_task.bz_rdx = pick_task.bz_rdx
            temp_pick_task.hot_j = pick_task.hot_j
            temp_pick_task.district_set = pick_task.district_set
            count_list.append(sum(pick_task.commodity_count.values()))
            # 求该条摘单记录中各车次的总重量列表
            weight_list.append(round_util(sum(pick_task.commodity_weight.values())))
            # 求该条摘单记录中各车次中的各品种的重量列表字典：{品种：[重量1，重量2]}
            for k_c in pick_task.commodity_weight.keys():
                commodity_weight_dict[k_c].append(round_util(pick_task.commodity_weight[k_c]))
            # 如果是滨州市的热镀锌钢卷
            if pick_task.bz_rdx:
                bz_rdx_str = pick_task.bz_rdx
            # 如果是热卷
            if pick_task.hot_j:
                hot_j_str = pick_task.hot_j
            # temp_pick_task中的明细列表
            temp_pick_task_item_list.extend(pick_task.items)
        # 重量取整
        temp_pick_task.total_weight = round_util(temp_pick_task.total_weight)
        # 生成备注
        for commodity_key in pick_task.commodity_weight.keys():
            # 备注的总重量
            weight_str = str(max(weight_list)) if len(set(weight_list)) == 1 else (
                    str(min(weight_list)) + '-' + str(max(weight_list)))
            # 单品种货物
            if len(pick_task.commodity_weight) == 1:
                # 件数唯一时
                if len(set(count_list)) == 1:
                    remark.append(ModelConfig.PICK_REMARK.get(commodity_key, commodity_key) + str(
                        count_list[0]) + '件' + weight_str + '吨左右')
                # 有多件时，比如有的13件、有的14件
                else:
                    remark.append(
                        ModelConfig.PICK_REMARK.get(commodity_key, commodity_key) + str(min(count_list)) + '-' + str(
                            max(count_list)) + '件' + weight_str + '吨左右')
            # 多品种货物
            else:
                # 品种的重量唯一时
                if len(set(commodity_weight_dict[commodity_key])) == 1:
                    remark.append(ModelConfig.PICK_REMARK.get(commodity_key, commodity_key) + str(
                        pick_task.commodity_count[commodity_key]) + '件' + str(
                        max(commodity_weight_dict[commodity_key])) + '吨左右')
                # 品种有多种重量时，比如有的24t、有的25t
                else:
                    remark.append(ModelConfig.PICK_REMARK.get(commodity_key, commodity_key) + str(
                        pick_task.commodity_count[commodity_key]) + '件' + str(
                        min(commodity_weight_dict[commodity_key])) + '-' + str(
                        max(commodity_weight_dict[commodity_key])) + '吨左右')
        temp_pick_task.remark = hot_j_str + '拼'.join(remark)
        # # 备注的总重量
        # weight_str = str(max(weight_list)) if len(set(weight_list)) == 1 else (
        #         str(min(weight_list)) + '-' + str(max(weight_list)))
        # 如果是滨州市的热镀锌钢卷
        temp_pick_task.remark += bz_rdx_str
        # # 如果装点为岚北港，则添加厂区的备注
        # special_deliware_str = ''
        # if '岚北港' in pick_task.deliware_district:
        #     special_deliware_str = '[' + ','.join(pick_task.deliware_district) + ']'
        # 添加厂区的备注
        deliware_district_str = ''
        # 厂区备注不为空时，因为当有新的仓库时，厂区可能为空
        if pick_task.deliware_district:
            deliware_district_str = '[' + ','.join(pick_task.deliware_district) + ']'
        temp_pick_task.remark += deliware_district_str
        # 加入temp_pick_task的子明细
        temp_pick_task.items.extend(merge_pick_task_item(temp_pick_task_item_list, temp_pick_task))
        pick_task_list.append(temp_pick_task)
    return pick_task_list


def get_special_remark(item):
    """
    特别的备注信息
    :param item:
    :return:
    """
    special_remark = ''
    # 窄带的特殊备注：窄带卷、木托
    if item.big_commodity == '新产品-窄带':
        if item.pack in ModelConfig.RG_NB_PACK[0]:
            special_remark = '(木托窄带)'
        elif item.pack in ModelConfig.RG_NB_PACK[1]:
            special_remark = '(窄带卷)'
    # 白卷的特殊备注：‘热镀锌钢卷’需要在备注特别标注
    if item.big_commodity == '新产品-白卷' and item.commodity == '热镀锌钢卷':
        special_remark = '(热镀锌)'
    return special_remark


def create_pick_task_item(load_task, load_task_item, total_weight):
    """
    创建pick_task_item对象并赋值
    :param total_weight:
    :param load_task:
    :param load_task_item:
    :return:
    """
    # 创建pick_task的明细pick_task_item对象并赋值
    pick_task_item = PickTaskItem()
    pick_task_item.source_name = load_task_item.source_name
    pick_task_item.total_weight = total_weight
    pick_task_item.truck_num = 1
    pick_task_item.province = load_task_item.province
    pick_task_item.city = load_task_item.city
    pick_task_item.end_point = load_task_item.end_point
    pick_task_item.big_commodity = load_task_item.big_commodity
    pick_task_item.commodity = load_task_item.commodity
    pick_task_item.items.append(load_task)
    '''找出所在厂区-start'''
    # load_task_item中有西区的货物
    if load_task_item.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[0]:
        pick_task_item.deliware_district = '宝华'
    # load_task_item中有老区的货物
    elif load_task_item.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[1]:
        pick_task_item.deliware_district = '厂内'
    # load_task_item中有岚北港的货物
    elif load_task_item.outstock_code in ModelConfig.RG_WAREHOUSE_GROUP[2]:
        pick_task_item.deliware_district = '岚北港'
    '''找出所在厂区-end'''
    return pick_task_item


def merge_pick_task_item(pick_task_item_list, pick_task):
    """
    将pick_task中的明细列表，按城市、区县、品种等合并
    :param pick_task:
    :param pick_task_item_list:
    :return:
    """
    result_pick_task_item_list = []
    pick_task_item_dict = defaultdict(list)
    # 分组
    for pick_task_item in pick_task_item_list:
        pick_task_item_dict[pick_task_item.deliware_district].append(pick_task_item)
    # 合并相同类型的明细
    for value_list in pick_task_item_dict.values():
        temp_pick_task_item: PickTaskItem = value_list[0]
        # 创建pick_task_item对象并赋值
        pick_task_item = PickTaskItem()
        pick_task_item.pick_id = pick_task.pick_id
        pick_task_item.source_name = temp_pick_task_item.source_name
        pick_task_item.truck_num = len(value_list)
        pick_task_item.truck_count = sum(pick_task.commodity_count.values())
        pick_task_item.province = temp_pick_task_item.province
        pick_task_item.city = temp_pick_task_item.city
        pick_task_item.end_point = temp_pick_task_item.end_point
        pick_task_item.deliware_district = temp_pick_task_item.deliware_district
        pick_task_item.big_commodity = temp_pick_task_item.big_commodity
        pick_task_item.commodity = temp_pick_task_item.commodity
        pick_task_item.remark = pick_task.remark
        for value in value_list:
            # 合并
            pick_task_item.total_weight += value.total_weight
            pick_task_item.items.extend(value.items)
        # 重量取整
        pick_task_item.total_weight = round_util(pick_task_item.total_weight)
        result_pick_task_item_list.append(pick_task_item)
    return result_pick_task_item_list
