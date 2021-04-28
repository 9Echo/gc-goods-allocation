# 作者： pingyu
# 日期： 2021/1/17
# 时间： 21:50   

from typing import List
from app.main.steel_factory.entity.load_task import LoadTask
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule import pick_group_by_huge_and_tail_rule, pick_split_group_rule, pick_compose
from app.main.steel_factory.rule import last_j_handler_rule, pick_compose_2_to_2
from app.main.steel_factory.rule import pick_compose_in_same_district_and_deliware_house
from app.main.steel_factory.rule import pick_compose_in_same_district_and_consumer
from app.main.steel_factory.rule.pick_compose_across_district import across_district_compose_in_one_factory
from app.main.steel_factory.rule.pick_compose_across_district import across_district_compose_in_two_factory
from app.main.steel_factory.rule.pick_compose_public_method import get_weight_with_city, can_generate_load_task, \
    merge_split_stock, get_weight
from app.main.steel_factory.rule.pick_create_load_task_rule import create_load_task
from app.main.steel_factory.rule.pick_goods_dispatch_Gene_algorithm_filter import GA
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from model_config import ModelConfig


def zero_one_knapsnack_dispatch_filter(stock_list: List[Stock]):
    """
    传入的数据类型 List[Stock]
    :param stock_list: 当前所有的可发库存
    :return: 返回的是对象数组而非纯数组
    """

    """
    1. 遍历库存中所有的城市，根据城市将货物分类{city:[city_stock]}
    2. 对对应城市库存进行货物配载
    """

    city_stock_dic = dict()
    for item in stock_list:
        key = item.city + "-" + item.dlv_spot_name_end
        if key not in city_stock_dic.keys():
            city_stock_dic[key]: List[Stock] = []
        city_stock_dic[key].append(item)

    city_load_task_list = []
    city_tail_list = []

    for city in city_stock_dic.keys():
        city_commodity_dic = dict()
        for item in city_stock_dic[city]:
            key = item.big_commodity_name
            # 将只能与自身品种拼货的品种单独生成一份库存列表
            if len(ModelConfig.RG_COMMODITY_GROUP[key]) > 1:
                key = "hybrid"
            if key not in city_commodity_dic.keys():
                city_commodity_dic[key] = []
            city_commodity_dic[key].append(item)

        min_weight, max_weight = get_weight(stock_list[0])
        for commodity in city_commodity_dic.keys():
            city_load_task_list, city_tail_list = GA_algorithm(city_commodity_dic[commodity], min_weight, max_weight)

    return city_load_task_list, city_tail_list


def GA_algorithm(city_stock: List[Stock], min_weight, max_weight):

    # 参数分别表示：交叉概率、变异概率、繁衍次数、种群数
    CXPB, MUTPB, NGEN, popsize = 0.8, 0.5, 3000, 100

    # 载重上下限
    up = max_weight
    low = min_weight
    parameter = [CXPB, MUTPB, NGEN, popsize, low, up, city_stock]

    run = GA(parameter)
    cargo_truck_state, max_profit = run.GA_main()
    print(cargo_truck_state, max_profit)
    city_load_task_list = []
    stock_be_used = []

    truck_number = len(cargo_truck_state[0])
    cargo_number = len(cargo_truck_state)
    for truck_index in range(truck_number):
        truck_state = [truck[truck_index] for truck in cargo_truck_state]
        pre_load_task_list: List[Stock] = []
        for cargo_index in range(cargo_number):
            if sum(truck_state) == 0:
                continue
            if truck_state[cargo_index]:
                pre_load_task_list.append(city_stock[cargo_index])
        if pre_load_task_list:
            stock_be_used.append(pre_load_task_list)
            city_truck_stock = merge_split_stock(pre_load_task_list)
            city_load_task_list.append(create_load_task(city_truck_stock, GenerateId.get_id(), ''))

    for pre_load_task_list in stock_be_used:
        for pre in pre_load_task_list:
            city_stock.remove(pre)

    city_tail_list = city_stock

    return city_load_task_list, city_tail_list


def getProfits(curr_stock: Stock, stock_list_curr_truck: List[Stock]):
    """
    计算当前货物放入该车次列表下的收益变化量
    1. 重量变化量：货物自身重量
    2. 车辆数是否增加（判断当前车次列表是否为空）
    3. 装卸地数目变化量
    :param curr_stock:
    :param stock_list_curr_truck:
    :return:
    """

    delta_number_truck = 0
    delta_deli_consumer = 0
    deliware_consumer_set = set()

    delta_weight = curr_stock.piece_weight

    if not stock_list_curr_truck:
        delta_number_truck = 1

    for stock in stock_list_curr_truck:
        deliware_consumer_set.add(stock.deliware_house)

    if curr_stock.deliware_house in deliware_consumer_set and curr_stock.consumer not in deliware_consumer_set:
        delta_deli_consumer = 1
    elif curr_stock.deliware_house not in deliware_consumer_set and curr_stock.consumer in deliware_consumer_set:
        delta_deli_consumer = 1
    elif curr_stock.deliware_house not in deliware_consumer_set and curr_stock.consumer not in deliware_consumer_set:
        delta_deli_consumer = 2

    profit_weight = [10, 8, 2]

    delta_profit = delta_weight * profit_weight[0] + delta_number_truck * profit_weight[1] + delta_deli_consumer * profit_weight[2]

    return delta_profit

