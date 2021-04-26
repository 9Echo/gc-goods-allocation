# -*- coding: utf-8 -*-
# Description: 确认发货通知单
# Created: shaoluyu 2019/11/13
from app.main.pipe_factory.entity.delivery_item import DeliveryItem
from app.util.code import ResponseCode
from app.main.pipe_factory.dao.delivery_log_dao import delivery_log_dao
from app.main.pipe_factory.entity.delivery_log import DeliveryLog
from app.main.pipe_factory.service.redis_service import get_delivery_list
from app.util.my_exception import MyException


def generate_delivery(delivery_data):
    """
    根据json数据生成对应的发货通知单
    """
    delivery_item_list = []

    for item in delivery_data.get('items'):
        delivery_item_model = DeliveryItem(item)
        delivery_item_list.append(delivery_item_model)

    return delivery_item_list


def confirm(company_id, batch_no, delivery_item_list):
    """
    将新数据删除、添加、更新的项写入log表
    :param: delivery是传过来的发货通知单对象列表
    :return:发货通知单对象列表
    """
    # 判断批次号的存在
    if not batch_no:
        raise MyException('批次号为空！', ResponseCode.Error)
    result_data = get_delivery_list(batch_no)
    # 如果没获取到原数据，结束操作
    if not result_data:
        return
    # 原明细数据
    old_item_list = result_data.data
    # 插入列表
    insert_list = list(filter(lambda i: not i.delivery_item_no, delivery_item_list))
    # 删除列表
    delete_list = list(
        filter(lambda i: i.delivery_item_no not in [j.delivery_item_no for j in delivery_item_list], old_item_list))
    # 更新列表
    new_update_list = list(
        filter(lambda i: i.delivery_item_no in [j.delivery_item_no for j in old_item_list], delivery_item_list))
    old_update_list = list(
        filter(lambda i: i.delivery_item_no in [j.delivery_item_no for j in delivery_item_list], old_item_list))
    # 合并列表
    log_list = merge(insert_list, delete_list, new_update_list, old_update_list, company_id)
    # 数据库操作
    delivery_log_dao.insert(log_list)
    return True


def merge(insert_list, delete_list, new_update_list, old_update_list, company_id):
    """
    合并列表，并做更新前后的对比
    :param insert_list:
    :param delete_list:
    :param new_update_list:
    :param old_update_list:
    :param company_id:
    :return:
    """
    # update状态
    log_update_list = []
    for i in old_update_list:
        for j in new_update_list:
            if i.delivery_item_no == j.delivery_item_no:
                if int(i.quantity) != int(j.quantity) or int(i.free_pcs) != int(j.free_pcs):
                    log_update_list.append(
                        DeliveryLog(
                            {"company_id": company_id, "delivery_no": i.delivery_no,
                             "delivery_item_no": i.delivery_item_no, "op": 2,
                             "quantity_before": i.quantity, "quantity_after": j.quantity,
                             "free_pcs_before": i.free_pcs,
                             "free_pcs_after": j.free_pcs}))
    # insert状态
    log_insert_list = [
        DeliveryLog(
            {"company_id": company_id, "delivery_no": i.delivery_no, "delivery_item_no": i.delivery_item_no, "op": 1,
             "quantity_before": 0, "quantity_after": i.quantity, "free_pcs_before": 0,
             "free_pcs_after": i.free_pcs}) for i in insert_list]
    # delete状态
    log_delete_list = [
        DeliveryLog(
            {"company_id": company_id, "delivery_no": i.delivery_no, "delivery_item_no": i.delivery_item_no, "op": 0,
             "quantity_before": i.quantity, "quantity_after": 0,
             "free_pcs_before": i.free_pcs,
             "free_pcs_after": 0}) for i in delete_list]
    # 合并
    log_list = log_delete_list + log_insert_list + log_update_list
    return log_list


