import copy

from flask import g

from app.main.pipe_factory.rule import weight_rule
from model_config import ModelConfig


def dispatch_load_task(sheets: list, min_delivery_items=None):
    """
    将发货单根据重量组合到对应的车次上
    :param min_delivery_items:
    :param sheets:
    :return:
    """

    left_sheets = []
    for sheet in sheets:
        # 如果已经生成车次的sheet，则跳过不处理
        if sheet.load_task_id:
            continue
        else:
            left_sheets.append(sheet)
    # 如果有小管，统计小管总重量
    min_total_weight = 0
    if min_delivery_items:
        min_total_weight = sum(i.weight for i in min_delivery_items)
    # 记录是否有未分车的单子
    while left_sheets:
        total_weight = 0
        total_volume = 0
        g.LOAD_TASK_ID += 1
        for sheet in copy.copy(left_sheets):
            # 如果提货单体积系数为1，小管剩余搭配重量大于0
            if sheet.volume == 1 and min_total_weight > 0:
                # 计算剩余小管重量
                min_total_weight -= (g.MAX_WEIGHT - sheet.weight)
                # 不超重时将当前发货单装到车上
                sheet.load_task_id = g.LOAD_TASK_ID
                # 将拼车成功的单子移除
                left_sheets.remove(sheet)
                break
            total_weight += sheet.weight
            total_volume += sheet.volume
            # 初始重量
            new_max_weight = g.MAX_WEIGHT
            # 如果当前车次总体积占比超出，计算剩余体积比例进行重量切单
            if total_volume > ModelConfig.MAX_VOLUME:
                # 按照体积比例计算，在最新的最大重量限制下还可以放多少重量
                limit_volume_weight = ((ModelConfig.MAX_VOLUME - total_volume + sheet.volume)
                                       / sheet.volume * sheet.weight)
                # 在最新的最大重量限制下还可以放多少重量
                limit_weight_weight = new_max_weight - (total_weight - sheet.weight)
                # 取较小的
                limit_weight = min(limit_weight_weight, limit_volume_weight)
                sheet, new_sheet = split_sheet(sheet, limit_weight)
                if new_sheet:
                    # 分单成功时旧单放入当前车上，新单放入等待列表
                    sheet.load_task_id = g.LOAD_TASK_ID
                    # 删除原单子
                    left_sheets.remove(sheet)
                    # 加入切分后剩余的新单子
                    left_sheets.insert(0, new_sheet)
                    # 原始单子列表加入新拆分出来的单子
                    sheets.append(new_sheet)
                break
            # 体积不超，处理重量
            else:
                # 如果总重量小于最大载重
                if total_weight <= new_max_weight:
                    # 不超重时将当前发货单装到车上
                    sheet.load_task_id = g.LOAD_TASK_ID
                    # 将拼车成功的单子移除
                    left_sheets.remove(sheet)
                    if new_max_weight - total_weight < ModelConfig.TRUCK_SPLIT_RANGE:
                        # 接近每车临界值时停止本次装车
                        break
                # 如果超重
                else:
                    # 超重时对发货单进行分单
                    if sheet.weight < ModelConfig.TRUCK_SPLIT_RANGE:
                        # 重量不超过1吨（可配置）的发货单不分单
                        break
                    # 如果大于1吨
                    else:
                        # 对满足条件的发货单进行分单
                        limit_weight = new_max_weight - (total_weight - sheet.weight)
                        sheet, new_sheet = split_sheet(sheet, limit_weight)
                        if new_sheet:
                            # 分单成功时旧单放入当前车上，新单放入等待列表
                            sheet.load_task_id = g.LOAD_TASK_ID
                            # 删除原单子
                            left_sheets.remove(sheet)
                            # 加入切分后剩余的新单子
                            left_sheets.insert(0, new_sheet)
                            # 原始单子列表加入新拆分出来的单子
                            sheets.append(new_sheet)
                        break


def split_sheet(sheet, limit_weight):
    """
    对超重的发货单进行分单
    limit_weight：当前车次重量剩余载重
    total_volume：当前车次目前体积占比
    """
    total_weight = 0
    # 切分出的新单子
    new_sheet = copy.copy(sheet)
    # 原单子最终的明细
    sheet_items = []
    # 新单子的明细
    new_sheet_items = copy.copy(sheet.items)
    for item in sheet.items:
        # 计算发货单中的哪一子项超重
        total_weight += item.weight
        if total_weight <= limit_weight:
            # 原单子追加明细
            sheet_items.append(item)
            # 新单子减少明细
            new_sheet_items.remove(item)
        #  如果当前车次超重
        else:
            item, new_item = weight_rule.split_item(item, total_weight - limit_weight)
            if new_item:
                # 原单子追加明细
                sheet_items.append(item)
                # 新单子减少明细
                new_sheet_items.remove(item)
                # 新单子加入新切分出来的明细
                new_sheet_items.insert(0, new_item)
            break
    if sheet_items:
        # 原单子明细重新赋值
        sheet.items = sheet_items
        sheet.weight = sum([i.weight for i in sheet.items])
        sheet.total_pcs = sum([i.total_pcs for i in sheet.items])
        sheet.volume = sum([i.volume for i in sheet.items])
        # 新单子明细赋值
        new_sheet.items = new_sheet_items
        new_sheet.weight = sum([i.weight for i in new_sheet.items])
        new_sheet.total_pcs = sum([i.total_pcs for i in new_sheet.items])
        new_sheet.volume = sum([i.volume for i in new_sheet.items])
        return sheet, new_sheet
    else:
        return sheet, None
