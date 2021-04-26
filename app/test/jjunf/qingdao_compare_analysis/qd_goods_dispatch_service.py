# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/11/24
from datetime import datetime
from threading import Thread
from app.main.steel_factory.rule import pick_goods_dispatch_filter, swap_load_to_stock_rule
from app.main.steel_factory.rule.pick_compose_public_method import merge_split_stock
from app.main.steel_factory.service.pick_goods_dispatch_service import merge
from app.test.jjunf.qingdao_compare_analysis import stock_service
from app.test.jjunf.qingdao_compare_analysis.save_log import save_log
from app.util.enum_util import LoadTaskType
from app.util.generate_id import GenerateId
from app.util.result import Result
from app.util.uuid_util import UUIDUtil


def dispatch():
    """
    青岛  摘单分货入口
    :return:
    """
    # 重置车次id
    GenerateId.set_id()
    # 获取经过预处理的库存列表
    west_stock_list, old_stock_list, lbg_stock_list, west_j_list, old_j_list, lbg_j_list, other_warehouse_stock_list, wait_list, early_load_task_list = (
        stock_service.get_pick_stock())
    # 配载，得到配载列表、无法配载的剩余尾货列表
    result_load_task_list, tail_list = pick_goods_dispatch_filter.dispatch_filter(west_stock_list, old_stock_list,
                                                                                  lbg_stock_list, west_j_list,
                                                                                  old_j_list, lbg_j_list,
                                                                                  other_warehouse_stock_list)
    # 最后剩余的库存 = 尾货 + wait_list
    # wait_list中包括：件重大于重量上限的货物 + 可发小于待发，并且待发在标载范围内的货物 + Z1、Z2库最新挂单时间未超过24小时的货物 + 滨州市不需要分配的货物
    tail_list.extend(wait_list)
    result_load_task_list.extend(early_load_task_list)
    # 合并，并且从中筛选出滨州老区配西区的货物列表
    pick_list, bz_old_compose_west_list = merge(result_load_task_list)
    # 将滨州老区配西区的货物列表添加进尾货
    tail_list = swap_load_to_stock_rule.swap_load_to_stock(tail_list, bz_old_compose_west_list)
    # 格式转换
    # result_dic = data_format(pick_list, tail_list)
    # 结果保存到数据库
    Thread(target=save_result, args=(pick_list, tail_list,)).start()
    # return Result.success(data=result_dic)


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
    create_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 摘单记录
    if pick_list:
        pick_list_item = []
        for pick in pick_list:
            pick_list_item.extend(pick.items)
        for pick in pick_list_item:
            for load_task in pick.items:
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
        tail_list = merge_split_stock(tail_list)
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
    save_log.save_qingdao_pick_log(values)


if __name__ == '__main__':
    dispatch()
