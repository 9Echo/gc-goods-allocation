# -*- coding: utf-8 -*-
# @Time    : 2020/03/26 16:14
# @Author  : zhouwentao
from app.main.pipe_factory.dao.delivery_item_dao import delivery_item_dao
from app.main.pipe_factory.dao.delivery_sheet_dao import delivery_sheet_dao
from app.main.pipe_factory.entity.delivery_sheet import DeliverySheet
from app.main.pipe_factory.entity.delivery_item import DeliveryItem
from app.util.uuid_util import UUIDUtil


def generate_sheets(sheets):
    """根据json数据生成对应的发货通知单"""
    sheets_list = []
    for sheet in sheets:
        delivery_sheet = DeliverySheet(sheet)
        for index in range(len(delivery_sheet.items)):
            delivery_sheet.items[index] = DeliveryItem(delivery_sheet.items[index])
        sheets_list.append(delivery_sheet)

    return sheets_list


def save_sheets(result_list):
    """

    :param result_list:
    :return:
    """
    items = list()
    for i in result_list:
        i.delivery_no = UUIDUtil.create_id("de")
        for j in i.items:
            j.delivery_no = i.delivery_no
            j.delivery_item_no = UUIDUtil.create_id("di")
        items.extend(i.items)
    delivery_sheet_dao.batch_insert(result_list)
    delivery_item_dao.batch_insert(items)
