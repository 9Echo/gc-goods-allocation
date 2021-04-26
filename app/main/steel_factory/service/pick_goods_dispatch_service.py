# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/9/29 9:12
from threading import Thread
from typing import List
from flask import current_app, json
from app.main.steel_factory.dao.pick_plan_dao import pick_plan_dao
from app.main.steel_factory.dao.pick_save_log import save_pick_log
from app.main.steel_factory.entity.pick_task import PickTask
from app.main.steel_factory.entity.plan import Plan
from app.main.steel_factory.rule import pick_goods_dispatch_filter, create_pick_task_rule
from app.main.steel_factory.rule import pick_goods_dispatch_zero_one_knapsnack_filter, create_pick_task_rule
from app.main.steel_factory.rule.pick_compose_public_method import merge_split_stock
from app.main.steel_factory.rule.pick_split_group_rule import split_group
from app.main.steel_factory.service import pick_stock_service
from app.util.date_util import get_now_date
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from app.util.result import Result
from app.util.round_util import round_util
from app.util.uuid_util import UUIDUtil


def dispatch(json_data):
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
    # 重置车次id
    GenerateId.set_id()
    # 获取经过预处理的库存列表 这里的wait_list属于不发的货物（还没开始进行配载） (west_stock_list, old_stock_list, lbg_stock_list, west_j_list,
    # old_j_list, lbg_j_list, other_warehouse_stock_list, wait_list, early_load_task_list) = (
    # pick_stock_service.get_pick_stock(json_data))

    can_be_send_stock_list, wait_list, early_load_task_list = (
        pick_stock_service.get_pick_stock(json_data))

    # 优化方案(多个背包的01背包问题）的配载结果，得到配载列表、无法配载的剩余尾货列表
    # 配载完成之后返回两个list,result_load_task_list：生成的预装车清单，tail_list：配载完成剩下的货物
    result_load_task_list, tail_list = pick_goods_dispatch_zero_one_knapsnack_filter.zero_one_knapsnack_dispatch_filter(
        can_be_send_stock_list)

    # # 配载，得到配载列表、无法配载的剩余尾货列表
    # result_load_task_list, tail_list = pick_goods_dispatch_filter.dispatch_filter(west_stock_list, old_stock_list,
    #                                                                               lbg_stock_list, west_j_list,
    #                                                                               old_j_list, lbg_j_list,
    #                                                                               other_warehouse_stock_list)
    # 最后剩余的库存 = 尾货 + wait_list
    # wait_list中包括：件重大于重量上限的货物 + 可发小于待发，并且待发在标载范围内的货物 +
    #                 济南市Z1、Z2库最新挂单时间未超过24小时的货物 + 滨州市不需要分配的货物
    tail_list.extend(wait_list)
    result_load_task_list.extend(early_load_task_list)

    # # 合并，并且从中筛选出滨州老区配西区的货物列表
    # pick_list, bz_old_compose_west_list = merge(result_load_task_list)
    # # 将滨州老区配西区的货物列表添加进尾货
    # tail_list = swap_load_to_stock_rule.swap_load_to_stock(tail_list, bz_old_compose_west_list)

    pick_list, bz_old_compose_west_list = create_pick_task_rule.create_pick_task(result_load_task_list)
    # 将tail_list中相同的订单合并
    tail_list = merge_split_stock(tail_list)
    # 摘单记录、尾货保存到数据库
    Thread(target=save_result, args=(pick_list, tail_list,)).start()
    # 格式转换，未开单有效调度单扣除操作
    result_dic, no_plan_pick_list = data_format(pick_list, tail_list)
    # 被扣除的已调度未开单的摘单记录保存到数据库
    Thread(target=save_deduct_pick, args=(no_plan_pick_list,)).start()
    return Result.success(data=result_dic)


def data_format(pick_list: List[PickTask], tail_list):
    """
    格式转换
    :param tail_list:
    :param pick_list:
    :return:
    """
    result_dic = {}
    # 未开单对应的调度单列表
    no_plan_pick_list = []
    # 摘单记录的格式转换
    p_list = []
    if pick_list:
        # 未开单有效调度单
        # plan_list: List[Plan] = pick_plan_dao.get_plan()
        # current_app.logger.info('摘单未开单明细：' + json.dumps([i.as_dict() for i in plan_list], ensure_ascii=False))
        # plan_dict = split_group(plan_list, "trains_no")
        # for key in plan_dict.keys():
        #     no_plan = weight_delete(plan_dict[key], pick_list)
        #     no_plan_pick_list.extend(no_plan)
        for pick_task in pick_list:
            # 如果上面扣除后摘单计划车次小于等于0，不生成最后的摘单计划
            if pick_task.truck_num <= 0:
                continue
            p_dic = {
                "sourceName": pick_task.source_name,
                "totalWeight": pick_task.total_weight,
                "truckNum": pick_task.truck_num,
                "province": pick_task.province,
                "city": pick_task.city,
                "endPoint": pick_task.end_point,
                "bigCommodity": pick_task.big_commodity,
                "commodity": pick_task.commodity,
                "remark": pick_task.remark,
                "groupFlag": pick_task.group_flag,
                "templateNo": pick_task.template_no,
                "items": [{
                    "sourceName": item.source_name,
                    "totalWeight": item.total_weight,
                    "truckNum": item.truck_num,
                    "truckCount": item.truck_count,
                    "province": pick_task.province,
                    "city": item.city,
                    "endPoint": item.end_point,
                    "bigCommodity": item.big_commodity,
                    "commodity": item.commodity,
                    "remark": item.remark
                } for item in pick_task.items]
            }
            p_list.append(p_dic)
    result_dic['pick_list'] = p_list
    # 尾货记录的格式转换
    s_list = []
    if tail_list:
        for stock in tail_list:
            temp_number = stock.can_split_number - stock.actual_number
            s_dict = {
                "sourceNumber": stock.source_number,
                "noticeNum": stock.notice_num,
                "oritemNum": stock.oritem_num,
                "deliwareHouse": stock.deliware_house,
                "sendNumber": temp_number,
                "sendWeight": 0 if temp_number == 0 else round(stock.can_split_weight - stock.actual_weight / 1000, 3)
            }
            s_list.append(s_dict)
    result_dic['tail_list'] = s_list
    return result_dic, no_plan_pick_list


def save_result(pick_list, tail_list):
    """
    保存摘单分货的记录
    :param tail_list:
    :param pick_list:
    :return:
    """
    if not pick_list and not tail_list:
        return None
    values = []
    create_date = get_now_date()
    # 摘单记录
    if pick_list:
        # pick_list_item = []
        for pick in pick_list:
            # pick_list_item.extend(pick.items)
            pick_list_item = [pick.items[0]]
            """
            保存到数据库时可能存在问题，如果pick_task_item中把整车的货物都放进去，那么在跨厂区时会重复存储
            """
            for pick_item in pick_list_item:
                for load_task in pick_item.items:
                    for load_task_item in load_task.items:
                        item_tuple = (pick.pick_id,
                                      pick.total_weight,
                                      pick.truck_num,
                                      pick.remark,
                                      load_task.load_task_id,
                                      load_task.load_task_type,
                                      load_task.total_weight,
                                      load_task.count,
                                      load_task_item.weight,
                                      load_task_item.count,
                                      load_task_item.city,
                                      load_task_item.end_point,
                                      load_task_item.consumer,
                                      load_task_item.big_commodity,
                                      load_task_item.commodity,
                                      load_task_item.outstock_code,
                                      load_task_item.notice_num,
                                      load_task_item.oritem_num,
                                      create_date
                                      )
                        values.append(item_tuple)
    # 尾货
    if tail_list:
        for stock in tail_list:
            tail_id = UUIDUtil.create_id('tail')
            train_id = GenerateId.get_id()
            item_tuple = (tail_id,
                          -1,
                          -1,
                          '',
                          train_id,
                          LoadTaskType.TYPE_5.value,
                          -1,
                          -1,
                          stock.actual_weight / 1000,
                          stock.actual_number,
                          stock.city,
                          stock.dlv_spot_name_end,
                          stock.consumer,
                          stock.big_commodity_name,
                          stock.commodity_name,
                          stock.deliware_house,
                          stock.notice_num,
                          stock.oritem_num,
                          create_date
                          )
            values.append(item_tuple)
    save_pick_log.save_pick_log(values)


def save_deduct_pick(deduct_pick_list):
    """
    保存被扣除的已调度未开单的摘单记录
    :param deduct_pick_list: 被扣除的已调度未开单的摘单记录
    :return:
    """
    if not deduct_pick_list:
        return None
    create_date = get_now_date()
    # 被扣除的已调度未开单的摘单记录
    deduct_values = []
    for pick in deduct_pick_list:
        across_factory = 1 if len(pick.deliware_district) > 1 else 0
        pick_item = pick.items[0]
        load_task = pick_item.items[0]
        # 如果跨厂区，则保存每个厂区的记录
        if len(pick.deliware_district) > 1:
            for load_task_item in load_task.items:
                item_tuple = (pick.pick_id,
                              pick.remark,
                              load_task_item.city,
                              load_task_item.end_point,
                              load_task_item.big_commodity,
                              across_factory,
                              create_date
                              )
                deduct_values.append(item_tuple)
        # 否则只保留一条记录
        else:
            load_task_item = load_task.items[0]
            item_tuple = (pick.pick_id,
                          pick.remark,
                          load_task_item.city,
                          load_task_item.end_point,
                          load_task_item.big_commodity,
                          across_factory,
                          create_date
                          )
            deduct_values.append(item_tuple)
    save_pick_log.save_pick_deduct_log(deduct_values)


def weight_delete(plan: List[Plan], pick_list):
    """
    已调度未开单的扣除操作
    :param plan:
    :param pick_list:
    :return:
    """
    no_plan = []
    for pick in pick_list:
        if pick.truck_num <= 0:
            continue
        match_or_not = [pick_item for pick_item in pick.items if
                        pick_item.city == plan[0].city and
                        pick_item.end_point == plan[0].district_name and
                        pick_item.big_commodity == plan[0].prodname and
                        pick_item.truck_count == plan[0].plan_quantity]
        if match_or_not:
            # 排除 跨厂区 和 不跨厂区 匹配成功的情况
            if len(plan) == len(pick.items):
                for item in pick.items:
                    # 平均一车重量
                    weight = item.total_weight / item.truck_num
                    # 摘单计划明细车次数减一
                    item.truck_num -= 1
                    pick.total_weight -= item.total_weight
                    # 重新计算重量
                    item.total_weight = round_util(weight * item.truck_num)
                    # 重新计算摘单计划重量
                    pick.total_weight += item.total_weight
                no_plan.append(pick)
                # 摘单计划车次数减一
                pick.truck_num -= 1
                return no_plan
    for pick in pick_list:
        if pick.truck_num <= 0:
            continue
        match_or_not = [pick_item for pick_item in pick.items if
                        pick_item.city == plan[0].city and
                        pick_item.end_point == plan[0].district_name and
                        pick_item.big_commodity == plan[0].prodname]
        if match_or_not:
            # 排除 跨厂区 和 不跨厂区 匹配成功的情况
            if len(plan) == len(pick.items):
                for item in pick.items:
                    # 平均一车重量
                    weight = item.total_weight / item.truck_num
                    # 摘单计划明细车次数减一
                    item.truck_num -= 1
                    pick.total_weight -= item.total_weight
                    # 重新计算重量
                    item.total_weight = round_util(weight * item.truck_num)
                    # 重新计算摘单计划重量
                    pick.total_weight += item.total_weight
                no_plan.append(pick)
                # 摘单计划车次数减一
                pick.truck_num -= 1
                return no_plan
    return no_plan
