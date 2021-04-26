# -*- coding: utf-8 -*-
# Description: 摘单库存服务
# Created: jjunf 2020/09/29
import copy
import pandas as pd
from app.main.steel_factory.entity.load_task_item import LoadTaskItem
from app.main.steel_factory.entity.stock import Stock
from app.main.steel_factory.rule import pick_early_dispatch_filter
from app.main.steel_factory.rule.pick_compose_public_method import get_weight
from app.util.code import ResponseCode
from app.util.generate_id import GenerateId
from app.util.get_weight_limit import get_lower_limit
from app.util.my_exception import MyException
from model_config import ModelConfig
from param_config import ParamConfig


def get_stock_id(obj):
    """
    根据库存信息生成每条库存的唯一id
    """
    if isinstance(obj, Stock):
        return hash(obj.notice_num + obj.oritem_num + obj.deliware_house)
    elif isinstance(obj, LoadTaskItem):
        return hash(obj.notice_num + obj.oritem_num + obj.outstock_code)


def get_pick_stock(all_stock_list):
    """
    根据车辆目的地和可运货物返回库存列表
    """
    # 库存列表预处理
    df_stock = pd.DataFrame(all_stock_list)
    df_stock['deliware_house'] = df_stock['deliware_house'].apply(lambda x: x.split('-')[0])

    """####客户统一配置"""
    # df_stock['consumer'] = df_stock['consumer'].apply(lambda x: ParamConfig.RG_SAME_CONSUMER_DICT.get(x, x))

    df_stock['consumer'] = df_stock['consumer'].apply(lambda x: ModelConfig.CONSUMER_DICT.get(x, x))
    # # 根据公式，计算实际可发重量，实际可发件数
    df_stock["actual_weight"] = df_stock["can_split_weight"] * 1000
    df_stock["actual_number"] = df_stock["can_split_number"] * 1
    df_stock = df_stock.loc[df_stock["actual_number"] > 0]
    # 根据公式计算件重
    df_stock["piece_weight"] = round(df_stock["actual_weight"] / df_stock["actual_number"])

    df_stock = df_stock[df_stock["piece_weight"] <= ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT]
    wait_list = df_stock[df_stock["piece_weight"] > ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT]
    df_stock["actual_weight"] = df_stock["piece_weight"] * df_stock["actual_number"]
    # 将待发重量转换为kg
    df_stock["waint_fordel_weight"] = df_stock["waint_fordel_weight"] * 1000
    # 筛选出大于0的数据
    df_stock = df_stock.loc[
        (df_stock["actual_weight"] > 0) & (df_stock["actual_number"] > 0) & (
            df_stock["latest_order_time"].notnull())]

    """
    将库存里每一件货物的件数转化为一件
    """
    # 将index设置为列，作为后续同一条数据的标记
    df_stock['index'] = df_stock.index
    df_stock_dic = df_stock.to_dict(orient="records")
    for row in range(len(df_stock_dic)):
        stock_each_row = df_stock_dic[row]
        stock_actual_number = int(stock_each_row['actual_number'])
        for number in range(stock_actual_number - 1):
            df_stock = df_stock.append(stock_each_row, ignore_index=True)

    df_stock['actual_weight'] = df_stock['piece_weight']
    df_stock['actual_number'] = 1

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
    # 将终点统一赋值到实际终点，方便后续处理联运
    df_stock["actual_end_point"] = df_stock["dlv_spot_name_end"]

    """
    将dataframe类型转化为数组字典，[{col1:value1,col2:value2},{col1:value1,...,}]
    再遍历该数组字典，将每一行的列名对应值转化为对象，即得到一个数组，该数组每一个值都为一个对象。
    """
    # df_stock["actual_weight"] = round(df_stock["actual_weight"] / 100)
    # wait_list["actual_weight"] = round(wait_list["actual_weight"] / 100)
    # df_stock["piece_weight"] = round(df_stock["piece_weight"] / 100)
    # wait_list["piece_weight"] = round(wait_list["piece_weight"] / 100)

    dic = df_stock.to_dict(orient="records")
    wait_list_dic = wait_list.to_dict(orient="records")
    # 初始化对象列表
    init_stock_list = [Stock(obj) for obj in dic]
    init_wait_list = [Stock(obj) for obj in wait_list_dic]

    for item in init_stock_list:
        item.parent_stock_id = get_stock_id(item)

    for item in init_wait_list:
        item.parent_stock_id = get_stock_id(item)

    # 早期处理
    # early_load_task_list, stock_list, early_wait_list = pick_early_dispatch_filter.early_dispatch_filter(
    #     init_stock_list)
    early_load_task_list, stock_list, early_wait_list = pick_early_dispatch_filter.early_dispatch_filter(
        init_stock_list)
    # stock_list, wait_list = split_pick_stock(init_stock_list)
    # stock_list.extend(early_stock_list)
    init_wait_list.extend(early_wait_list)

    '''####仓库配置'''
    # # 筛选出西区的'新产品-卷板', '新产品-白卷'
    # west_j_stock = [item for item in stock_list
    #                 if item.big_commodity_name in ['新产品-卷板', '新产品-白卷']
    #                 and item.deliware_house in ParamConfig.RG_WAREHOUSE_GROUP[0]]
    # # 筛选出西区中除'新产品-卷板', '新产品-白卷'外的其他货物
    # west_stock = [item for item in stock_list
    #               if item.big_commodity_name not in ['新产品-卷板', '新产品-白卷']
    #               and item.deliware_house in ParamConfig.RG_WAREHOUSE_GROUP[0]]
    # # 筛选出老区的'老区-卷板'
    # old_j_stock = [item for item in stock_list
    #                if item.big_commodity_name == "老区-卷板"
    #                and item.deliware_house in ParamConfig.RG_WAREHOUSE_GROUP[1]]
    # # 筛选出老区中除'老区-卷板'外的其他货物
    # old_stock = [item for item in stock_list
    #              if item.big_commodity_name != "老区-卷板"
    #              and item.deliware_house in ParamConfig.RG_WAREHOUSE_GROUP[1]]
    # # 筛选出岚北港的卷类
    # lbg_j_stock = [item for item in stock_list
    #                if item.big_commodity_name in ModelConfig.RG_J_GROUP
    #                and item.deliware_house in ParamConfig.RG_WAREHOUSE_GROUP[2]]
    # # 筛选出岚北港中除卷类外的其他货物
    # lbg_stock = [item for item in stock_list
    #              if item.big_commodity_name not in ModelConfig.RG_J_GROUP
    #              and item.deliware_house in ParamConfig.RG_WAREHOUSE_GROUP[2]]
    # # 不在ParamConfig.RG_WAREHOUSE_GROUP仓库中的货物
    # other_warehouse_stock = [item for item in stock_list
    #                          if item.deliware_house not in ParamConfig.RG_WAREHOUSE_GROUP[0] +
    #                          ParamConfig.RG_WAREHOUSE_GROUP[1] + ParamConfig.RG_WAREHOUSE_GROUP[2]]

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
    # 不在ModelConfig.RG_WAREHOUSE_LIST仓库中的货物
    other_warehouse_stock = [item for item in stock_list if
                             item.deliware_house not in ModelConfig.RG_WAREHOUSE_GROUP_LIST]

    # 将货物按照四个城市分为四类
    zibo_stock = [item for item in stock_list if
                  item.city == "淄博市"]
    binzhou_stock = [item for item in stock_list if
                     item.city == "滨州市"]
    jinan_stock = [item for item in stock_list if
                   item.city == "济南市"]
    heze_stock = [item for item in stock_list if
                  item.city == "菏泽市"]

    return stock_list, init_wait_list, early_load_task_list

    # return (west_stock, old_stock, lbg_stock, west_j_stock, old_j_stock, lbg_j_stock, other_warehouse_stock,
    # init_wait_list, early_load_task_list)


def split_pick_stock(init_stock_list):
    """
    货物切分
    :param init_stock_list:
    :return:
    """
    stock_list = []
    # 将件重大于重量上限的货物保存到尾货
    wait_list = []
    for stock in init_stock_list:
        stock.parent_stock_id = get_stock_id(stock)
        # 组数
        target_group_num = 0
        # 临时组数
        temp_group_num = 0
        # 最后一组件数
        target_left_num = 0
        # 一组几件
        target_num = 0
        # 平衡分组
        for weight in range(get_lower_limit(stock.big_commodity_8name), ModelConfig.RG_MAX_WEIGHT + 2000, 1000):
            if weight == ModelConfig.RG_MAX_WEIGHT + 1000:
                weight = ModelConfig.RG_MAX_WEIGHT + ModelConfig.RG_SINGLE_UP_WEIGHT
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
        # 标准件每组件数
        count_list = [target_num] * target_group_num
        # 如果还有尾货
        if target_left_num:
            # 将每组进行检查
            for group_index in range(target_group_num):
                # 当每组件数*件重<=最大重量时
                while (((count_list[group_index] + 1) * stock.piece_weight <= ModelConfig.RG_MAX_WEIGHT)
                       and target_left_num > 0):
                    # 当前组件数+1
                    count_list[group_index] += 1
                    target_left_num -= 1
        # 首先去除 件重大于35500的货物，保存到尾货
        if target_num < 1:
            wait_list.append(stock)
        # 其次如果可装的件数大于实际可发件数，不用拆分，直接添加到stock_list列表中
        elif target_num > stock.actual_number:
            # 可装的件数大于实际可发件数，并且达到标载
            if stock.actual_weight >= get_lower_limit(stock.big_commodity_name):
                stock.limit_mark = 1
            else:
                stock.limit_mark = 0
            stock.stock_id = GenerateId.get_stock_id()
            stock_list.append(stock)
        # 最后不满足则拆分
        else:
            for count in count_list:
                copy_2 = copy.deepcopy(stock)
                copy_2.actual_weight = count * stock.piece_weight
                copy_2.actual_number = int(count)
                if copy_2.actual_weight < get_lower_limit(stock.big_commodity_name):
                    copy_2.limit_mark = 0
                else:
                    copy_2.limit_mark = 1
                copy_2.stock_id = GenerateId.get_stock_id()
                stock_list.append(copy_2)
            if target_left_num:
                copy_1 = copy.deepcopy(stock)
                copy_1.actual_number = int(target_left_num)
                copy_1.actual_weight = target_left_num * stock.piece_weight
                if copy_1.actual_weight < get_lower_limit(stock.big_commodity_name):
                    copy_1.limit_mark = 0
                else:
                    copy_1.limit_mark = 1
                copy_1.stock_id = GenerateId.get_stock_id()
                stock_list.append(copy_1)
    return stock_list, wait_list


"""####切分时重量配置"""  # 替换split_pick_stock
# def split_pick_stock(init_stock_list):
#     """
#     货物切分
#     :param init_stock_list:
#     :return:
#     """
#     stock_list = []
#     # 将件重大于重量上限的货物保存到尾货
#     wait_list = []
#     for stock in init_stock_list:
#         stock.parent_stock_id = get_stock_id(stock)
#         # 组数
#         target_group_num = 0
#         # 临时组数
#         temp_group_num = 0
#         # 最后一组件数
#         target_left_num = 0
#         # 一组几件
#         target_num = 0
#         # 根据stock获取重量上下限
#         min_weight, max_weight = get_weight(stock)
#         # 平衡分组
#         for weight in range(min_weight, max_weight + 1000, 1000):
#             """注：max_weight中已经加了上浮重量0.4，所以这里rang的右区间只能是max_weight+1000，而不能是max_weight+2000"""
#             """注：min_weight不一定是1000的整倍，可能是31.2吨什么的，用（if weight > max_weight）条件来判断"""
#             if weight > max_weight:
#                 weight = max_weight
#             # 一组几件
#             num = weight // stock.piece_weight
#             if num < 1 or num > stock.actual_number:
#                 target_num = num
#                 continue
#             # 如果还没轮到最后，并且标准组重量未达到标载，就跳过
#             if weight < max_weight and (num * stock.piece_weight) < min_weight:
#                 continue
#             # 组数
#             group_num = stock.actual_number // num
#             # 最后一组件数
#             left_num = stock.actual_number % num
#             # 如果最后一组符合标载条件，临时组数加1
#             temp_num = 0
#             if (left_num * stock.piece_weight) >= min_weight:
#                 temp_num = 1
#             # 如果分的每组件数更多，并且组数不减少，就替换
#             if (group_num + temp_num) >= temp_group_num:
#                 target_group_num = group_num
#                 temp_group_num = group_num + temp_num
#                 target_left_num = left_num
#                 target_num = num
#         # 标准件每组件数
#         count_list = [target_num] * target_group_num
#         # 如果还有尾货
#         if target_left_num:
#             # 将每组进行检查
#             for group_index in range(target_group_num):
#                 # 当每组件数*件重<=最大重量时
#                 while (((count_list[group_index] + 1) * stock.piece_weight <= max_weight)
#                        and target_left_num > 0):
#                     # 当前组件数+1
#                     count_list[group_index] += 1
#                     target_left_num -= 1
#         # 首先去除 件重大于重量上限的货物，保存到尾货
#         if target_num < 1:
#             wait_list.append(stock)
#         # 其次如果可装的件数大于实际可发件数，不用拆分，直接添加到stock_list列表中
#         elif target_num > stock.actual_number:
#             # 可装的件数大于实际可发件数，并且达到标载
#             if stock.actual_weight >= min_weight:
#                 stock.limit_mark = 1
#             else:
#                 stock.limit_mark = 0
#             stock.stock_id = GenerateId.get_stock_id()
#             stock_list.append(stock)
#         # 最后不满足则拆分
#         else:
#             for count in count_list:
#                 copy_2 = copy.deepcopy(stock)
#                 copy_2.actual_weight = count * stock.piece_weight
#                 copy_2.actual_number = int(count)
#                 if copy_2.actual_weight < min_weight:
#                     copy_2.limit_mark = 0
#                 else:
#                     copy_2.limit_mark = 1
#                 copy_2.stock_id = GenerateId.get_stock_id()
#                 stock_list.append(copy_2)
#             if target_left_num:
#                 copy_1 = copy.deepcopy(stock)
#                 copy_1.actual_number = int(target_left_num)
#                 copy_1.actual_weight = target_left_num * stock.piece_weight
#                 if copy_1.actual_weight < min_weight:
#                     copy_1.limit_mark = 0
#                 else:
#                     copy_1.limit_mark = 1
#                 copy_1.stock_id = GenerateId.get_stock_id()
#                 stock_list.append(copy_1)
#     return stock_list, wait_list
