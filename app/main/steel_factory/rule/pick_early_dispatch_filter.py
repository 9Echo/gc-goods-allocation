# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/10/29 11:01
import copy
from typing import List

from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule import pick_split_group_rule
from app.main.steel_factory.rule.pick_compose_public_method import get_weight
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.main.steel_factory.service import pick_stock_service
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from app.util.get_weight_limit import get_lower_limit
from model_config import ModelConfig
from param_config import ParamConfig


def early_dispatch_filter(init_stock_list: List[Stock]):
    """
    早期处理，处理大于200的切分和流向剩余量少的情况
    :param init_stock_list:
    :return:
    """
    # 车次列表
    early_load_task_list: List[LoadTask] = []
    # 尾货：件重大于重量上限的货物 + 可发小于待发，并且待发在标载范围内的货物 + Z1、Z2库最新挂单时间未超过24小时的货物 + 滨州市不需要分配的货物
    early_wait_list: List[Stock] = []
    # 切分好的库存列表
    early_stock_list: List[Stock] = []
    # 移除等货的数据
    for init_stock in copy.copy(init_stock_list):
        init_stock.parent_stock_id = pick_stock_service.get_stock_id(init_stock)

        """####特殊库存扣除配置"""
        # # 扣除特殊品种的货物
        # for deduct_commodity in ParamConfig.RG_DEDUCT_STOCK['special_commodity']:
        #     if init_stock.city + ',' + init_stock.big_commodity_name == deduct_commodity:
        #         early_wait_list.append(init_stock)
        #         init_stock_list.remove(init_stock)
        # # 扣除特殊客户的货物
        # for deduct_consumer in ParamConfig.RG_DEDUCT_STOCK['special_consumer']:
        #     if (init_stock.city + ',' + init_stock.dlv_spot_name_end + ',' +
        #             init_stock.big_commodity_name + ',' + init_stock.consumer == deduct_consumer):
        #         early_wait_list.append(init_stock)
        #         init_stock_list.remove(init_stock)
        # # 如果可发小于待发，并且待发在标载范围内，就不参与配载
        # min_weight, max_weight = get_weight(init_stock)
        # if (init_stock.actual_number < init_stock.waint_fordel_number
        #         and min_weight <= init_stock.waint_fordel_weight <= max_weight):
        #     early_wait_list.append(init_stock)
        #     init_stock_list.remove(init_stock)
        # # 需要将下面的去掉

        # 筛选出滨州市不需要分配的货物
        if init_stock.city == '滨州市' and init_stock.big_commodity_name not in ModelConfig.RG_BZ_GROUP:
            early_wait_list.append(init_stock)
            init_stock_list.remove(init_stock)
        # 如果可发小于待发，并且待发在标载范围内，就不参与配载
        elif (init_stock.actual_number <
              init_stock.waint_fordel_number and get_lower_limit(init_stock.big_commodity_name)
              <= init_stock.waint_fordel_weight <= ModelConfig.RG_MAX_WEIGHT):
            early_wait_list.append(init_stock)
            init_stock_list.remove(init_stock)
        # # 济南市Z1、Z2热轧成品库库的货物，最新挂单时间未超过24小时的，不能拉走，否则可能出事故，(人工可以线下确认哪部分货物挂单时间超过24小时)
        # elif init_stock.city == '济南市' and (init_stock.deliware_house == 'Z1' or init_stock.deliware_house == 'Z2'):
        #     time_now = datetime.datetime.now()
        #     time_last_day = (time_now - datetime.timedelta(hours=24)).__format__('%Y-%m-%d %H:%M:%S')
        #     if init_stock.latest_order_time > time_last_day:
        #         early_wait_list.append(init_stock)
        #         init_stock_list.remove(init_stock)
        else:
            pass
    # 按区县分组
    actual_end_point_stock_dict = pick_split_group_rule.split_group(init_stock_list, 'actual_end_point')
    for value in actual_end_point_stock_dict.values():
        total_weight = sum([i.actual_weight for i in value])
        # 此流向货物总量<=40吨时
        if total_weight <= ModelConfig.RG_EARLY_MAX_WEIGHT and value[0].city != '济南市':
            flag = True
            temp_stock_big_commodity_name = None
            # 将未完全生产完的货物移除
            new_value = [i for i in value if i.waint_fordel_number <= i.actual_weight]
            # 二次求和
            total_weight = sum([i.actual_weight for i in new_value])
            # 考虑品种搭配、客户数量、仓库数量,包括两装两卸  -> 去重
            big_commodity_name_set = set([i.big_commodity_name for i in new_value])
            deliware_house_set = set([i.deliware_house for i in new_value])
            consumer_set = set([i.consumer for i in new_value])
            # 判断品种可拼
            for big_commodity_name in big_commodity_name_set:
                if not temp_stock_big_commodity_name:
                    temp_stock_big_commodity_name = big_commodity_name
                else:
                    compose_list = ModelConfig.RG_COMMODITY_GROUP.get(temp_stock_big_commodity_name)
                    if big_commodity_name not in compose_list:
                        flag = False
            # 最差的情况：两装两卸
            if len(deliware_house_set) > 2 or len(consumer_set) > 2:
                flag = False
            if flag and total_weight >= ModelConfig.RG_MIN_WEIGHT:
                # 判断装卸类型
                if len(deliware_house_set) == 1 and len(consumer_set) == 1:
                    load_task_type = LoadTaskType.TYPE_1.value
                elif len(deliware_house_set) == 1 and len(consumer_set) == 2:
                    load_task_type = LoadTaskType.TYPE_4.value
                elif len(deliware_house_set) == 2 and not judge_if_across_factory(deliware_house_set) and len(
                        consumer_set) == 1:
                    load_task_type = LoadTaskType.TYPE_2.value
                elif len(deliware_house_set) == 2 and judge_if_across_factory(deliware_house_set) and len(
                        consumer_set) == 1:
                    load_task_type = LoadTaskType.TYPE_3.value
                elif len(deliware_house_set) == 2 and not judge_if_across_factory(deliware_house_set) and len(
                        consumer_set) == 2:
                    load_task_type = LoadTaskType.TYPE_6.value
                elif len(deliware_house_set) == 2 and judge_if_across_factory(deliware_house_set) and len(
                        consumer_set) == 2:
                    load_task_type = LoadTaskType.TYPE_7.value
                else:
                    load_task_type = LoadTaskType.TYPE_0.value
                early_load_task_list.append(create_load_task(new_value, GenerateId.get_id(), load_task_type))
                for delete_stock in new_value:
                    init_stock_list.remove(delete_stock)
        # 首先判断整个区县有没有200吨以上的货
        # elif total_weight > ModelConfig.RG_CONSUMER_WEIGHT:
        #     # 客户、品名分组
        #     consumer_big_commodity_name_stock_dict = pick_split_group_rule.split_group(init_stock_list, 'consumer',
        #                                                                                'big_commodity_name')
        #     # 寻找某客户某品种总量大于200吨的情况
        #     for consumer_big_commodity_list in consumer_big_commodity_name_stock_dict.values():
        #         consumer_big_commodity_total_weight = sum([i.actual_weight for i in consumer_big_commodity_list])
        #         if consumer_big_commodity_total_weight > ModelConfig.RG_CONSUMER_WEIGHT:
        #             # 切分
        #             early_split_pick_stock(consumer_big_commodity_list, early_stock_list, early_wait_list)
        #             # 原列表移除
        #             for cb_delete_stock in consumer_big_commodity_list:
        #                 init_stock_list.remove(cb_delete_stock)
        else:
            pass

    can_be_sent_stock_list = init_stock_list
    return early_load_task_list, can_be_sent_stock_list, early_wait_list


def judge_if_across_factory(deliware_house_set):
    """
    判断deliware_house_set中的仓库是否跨厂区，跨厂区返回True，否则返回False
    :param deliware_house_set:
    :return:
    """
    factory_set = set()
    for deliware_house in deliware_house_set:
        if deliware_house in ModelConfig.RG_WAREHOUSE_GROUP[0]:
            factory_set.add('宝华')
        elif deliware_house in ModelConfig.RG_WAREHOUSE_GROUP[1]:
            factory_set.add('厂内')
        elif deliware_house in ModelConfig.RG_WAREHOUSE_GROUP[2]:
            factory_set.add('岚北港')
        else:
            factory_set.add('未知厂区')
    if len(factory_set) > 1:
        return True
    else:
        return False


def early_split_pick_stock(consumer_big_commodity_list, early_stock_list, early_wait_list):
    """
    早期的切分，拿最大重量切
    :param consumer_big_commodity_list:
    :param early_stock_list:
    :param early_wait_list:
    :return:
    """
    for stock in consumer_big_commodity_list:
        # 一组几件
        num = (ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT) // stock.piece_weight
        # 组数
        group_num = stock.actual_number // num
        # 最后一组件数
        left_num = stock.actual_number % num
        # 首先去除 件重大于35500的货物，保存到尾货
        if num < 1:
            early_wait_list.append(stock)
        # 其次如果可装的件数大于实际可发件数，不用拆分，直接添加到stock_list列表中
        elif num > stock.actual_number:
            # 可装的件数大于实际可发件数，并且达到标载
            if stock.actual_weight >= get_lower_limit(stock.big_commodity_name):
                stock.limit_mark = 1
            else:
                stock.limit_mark = 0
            stock.stock_id = GenerateId.get_stock_id()
            early_stock_list.append(stock)
        # 最后不满足则拆分
        else:
            for _ in range(group_num):
                copy_2 = copy.deepcopy(stock)
                copy_2.actual_weight = num * stock.piece_weight
                copy_2.actual_number = num
                if copy_2.actual_weight < get_lower_limit(stock.big_commodity_name):
                    copy_2.limit_mark = 0
                else:
                    copy_2.limit_mark = 1
                copy_2.stock_id = GenerateId.get_stock_id()
                early_stock_list.append(copy_2)
            if left_num:
                copy_1 = copy.deepcopy(stock)
                copy_1.actual_number = left_num
                copy_1.actual_weight = left_num * stock.piece_weight
                if copy_1.actual_weight < get_lower_limit(stock.big_commodity_name):
                    copy_1.limit_mark = 0
                else:
                    copy_1.limit_mark = 1
                copy_1.stock_id = GenerateId.get_stock_id()
                early_stock_list.append(copy_1)

    """####切分时重量配置"""  # 替换上面所有的
    # for stock in consumer_big_commodity_list:
    #     # 根据stock获取重量上下限
    #     min_weight, max_weight = get_weight(stock)
    #     # 一组几件
    #     num = max_weight // stock.piece_weight
    #     # 组数
    #     group_num = stock.actual_number // num
    #     # 最后一组件数
    #     left_num = stock.actual_number % num
    #     # 首先去除 件重大于35500的货物，保存到尾货
    #     if num < 1:
    #         early_wait_list.append(stock)
    #     # 其次如果可装的件数大于实际可发件数，不用拆分，直接添加到stock_list列表中
    #     elif num > stock.actual_number:
    #         # 可装的件数大于实际可发件数，并且达到标载
    #         if stock.actual_weight >= min_weight:
    #             stock.limit_mark = 1
    #         else:
    #             stock.limit_mark = 0
    #         stock.stock_id = GenerateId.get_stock_id()
    #         early_stock_list.append(stock)
    #     # 最后不满足则拆分
    #     else:
    #         for _ in range(group_num):
    #             copy_2 = copy.deepcopy(stock)
    #             copy_2.actual_weight = num * stock.piece_weight
    #             copy_2.actual_number = num
    #             if copy_2.actual_weight < min_weight:
    #                 copy_2.limit_mark = 0
    #             else:
    #                 copy_2.limit_mark = 1
    #             copy_2.stock_id = GenerateId.get_stock_id()
    #             early_stock_list.append(copy_2)
    #         if left_num:
    #             copy_1 = copy.deepcopy(stock)
    #             copy_1.actual_number = left_num
    #             copy_1.actual_weight = left_num * stock.piece_weight
    #             if copy_1.actual_weight < min_weight:
    #                 copy_1.limit_mark = 0
    #             else:
    #                 copy_1.limit_mark = 1
    #             copy_1.stock_id = GenerateId.get_stock_id()
    #             early_stock_list.append(copy_1)
