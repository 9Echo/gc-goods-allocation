import copy

from flask import g

from app.main.pipe_factory.rule import product_type_rule
from app.util import weight_calculator


def combine_sheets(sheets, types=None):
    '''
    合并因拼单被打散的发货单
    合并场景1：品类和物资代码相同的子发货单合并为1个子发货单
    合并场景2：品类相同物资代码不同的子发货单合并为1个发货单
    合并场景3：品类不同但是同属于一个similar group内的子发货单合并为1个发货单
    合并后如果被合并的发货单中没有剩余子单则删除该发货单
    :param types: 比如types=='weight'，表明当以重量优先时要做额外的一些处理
    :param sheets:
    :return:
    '''

    # 先根据车次号将发货单分组，然后对每组内发货单进行合并
    load_task_dict = {}
    for sheet in sheets:
        load_task_dict.setdefault(sheet.load_task_id, []).append(sheet)
    for load_task_id, sheet_group in load_task_dict.items():
        product_dict = {}
        for current in sheet_group:
            source = None
            # 取出当前组内的发货单，根据发货单中第一个子单的品类映射到product_dict上，每个品类对应的发货单作为合并的母体
            product_type = product_type_rule.get_product_type(current.items[0])
            if not product_dict.__contains__(product_type):
                product_dict[product_type] = current
                source = current
            else:
                source = product_dict[product_type]
                # 先将current的所有子单合并到source中，然后从sheets中删除被合并的发货单
                source.items.extend(current.items)
                source.weight += current.weight
                source.total_pcs += current.total_pcs
                source.volume += current.volume
                sheets.remove(current)
            # 再判断物资代码是否相同，如果相同则认为同一种产品，将子单合并
            item_id_dict = {}
            for citem in copy.copy(source.items):
                # 如果发现重量为0的明细，移除
                if citem.weight == 0:
                    source.items.remove(citem)
                    continue
                if not item_id_dict.__contains__('{},{},{}'.format(citem.item_id, citem.material, citem.f_loc)):
                    item_id_dict['{},{},{}'.format(citem.item_id, citem.material, citem.f_loc)] = citem
                else:
                    sitem = item_id_dict['{},{},{}'.format(citem.item_id, citem.material, citem.f_loc)]
                    sitem.quantity += citem.quantity
                    sitem.free_pcs += citem.free_pcs
                    sitem.total_pcs += citem.total_pcs
                    sitem.weight += citem.weight
                    sitem.volume += citem.volume
                    source.items.remove(citem)
        # 对当前车次做完合并后，重新对单号赋值
        current_sheets = [i for i in sheets if i.load_task_id == load_task_id]
        no = 0
        for sheet in current_sheets:
            item_no = 0
            no += 1
            sheet.delivery_no = g.DOC_TYPE + str(load_task_id) + '-' + str(no)
            for j in sheet.items:
                item_no += 1
                j.delivery_no = sheet.delivery_no
                j.delivery_item_no = sheet.delivery_no + '-' + str(item_no)
                j.weight = weight_calculator.calculate_weight(j.product_type, j.item_id, j.quantity, j.free_pcs)
            sheet.weight = sum(i.weight for i in sheet.items)
