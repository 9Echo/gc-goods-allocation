import math

from flask import g
import copy
from app.main.pipe_factory.entity.loading_floor import LoadingFloor
from app.main.pipe_factory.entity.loading_truck import LoadingTruck
from app.main.pipe_factory.entity.loading_item import LoadingItem
from app.util.aspect.method_before import get_item_a

"""
车型：13米，车宽2.4米，车厢高1.5米
1.确定摆放顺序，（将大、小管分类）
2.获取打包参数，确定摆放位置：[第几层，放什么，放几件，放几根，层高，本层剩余宽度]
"""


@get_item_a
def loading(sheets, car_info):
    # 车长
    truck_length = car_info[0]
    # 车宽
    truck_width = car_info[1]
    # 车侧板高
    truck_height = car_info[2]

    # 发货通知单转配载列表
    load_list = sheets_to_load_list(sheets)
    # 装配货物,先按整件处理
    already_loading_trucks = load_quantity_goods(truck_length, truck_width, load_list)
    # 装配货物,散根处理
    loading_trucks = load_free_pcs_goods(truck_length, truck_width,already_loading_trucks, load_list)
    # 对装配数据处理，转为对象
    trucks_list = truck_list_to_object(loading_trucks)

    return trucks_list


def load_quantity_goods(truck_length, truck_width, load_list):
    """
        根据传入的list装配货物
        :param load_list: 车辆的装载情况字典 {"1":{left_width_in:??,left_width_out:??,height_in:??,height_out:??,goods_in:[??]，goods_out:[??]}, "2":...}
        :param truck_length: 车长
        :param truck_width: 车宽
        :return:loading_trucks(字典)"""
    loading_trucks = []
    load_list_backup=copy.deepcopy(load_list)
    for index in range(len(load_list)):
        # 存放所装的货物
        box_list = {}
        # item为发货单中的每一种货物
        for i in range(len(load_list[index]) - 1):
            item = load_list[index][i]
            # 货物长
            product_length = float(item.pipe_length)
            # 表示该货物在车长面能够放几段
            item.segment = int(truck_length // product_length)
            # 摆放货物
            box_list = put_quantity_goods(box_list, item, truck_width)
        # 计算车内货物的总高度
        total_height_in, total_height_out,total_weight,total_quantity,total_pcs = \
        caculate_total_quantity_height(load_list_backup[index],box_list)

        # 调整数据格式，返回内层、外层货物列表汇总
        goods_in_list, goods_out_list = box_list_to_goods(box_list)
        loading_trucks.append({
            "load_task_id": load_list[index][-1],
            "total_weight": total_weight,
            "total_quantity": total_quantity,
            "total_pcs": total_pcs,
            "box_list":box_list,
            "goods_in": goods_in_list,
            "goods_out": goods_out_list,
            "total_height_in": total_height_in,
            "total_height_out": total_height_out
        })

    return loading_trucks

def load_free_pcs_goods(truck_length, truck_width,already_loading_trucks, left_load_list):
    """
        处理剩余的散根货物，从最上层开始装
        :param load_list: 车辆的装载情况字典 {"1":{left_width_in:??,left_width_out:??,height_in:??,height_out:??,goods_in:[??]，goods_out:[??]}, "2":...}
        :param truck_length: 车长
        :param truck_width: 车宽
        :param already_loading_trucks: 已装载的车货
        :param left_load_list: 剩余待装散根货物
        :return:loading_trucks(字典)"""
    loading_trucks=[]
    load_list_backup=copy.deepcopy(left_load_list)
    for index in range(len(left_load_list)):
        # 获取车辆已安装的货物情况
        box_list = already_loading_trucks[index]["box_list"]
        # item为发货单中的每一种货物
        for i in range(len(left_load_list[index]) - 1):
            item = left_load_list[index][i]
            # 散根不为0,则开始装载
            if item.free_pcs!=0:
                box_list = put_free_pcs_goods(box_list, item, truck_width)
        # 计算车内货物的总高度
        total_height_in, total_height_out,total_weight,total_quantity,total_pcs = \
        caculate_total_quantity_height(load_list_backup[index],box_list)

        # 调整数据格式，返回内层、外层货物列表汇总
        goods_in_list, goods_out_list = box_list_to_goods(box_list)
        loading_trucks.append({
            "load_task_id": left_load_list[index][-1],
            "total_weight": total_weight,
            "total_quantity": total_quantity,
            "total_pcs": total_pcs,
            "goods_in": goods_in_list,
            "goods_out": goods_out_list,
            "total_height_in": total_height_in,
            "total_height_out": total_height_out
        })

    return loading_trucks

def put_quantity_goods(box_list, item, truck_width):
    """
    根据传入的item摆放货物(整件放)
    :param box_list: 车辆的装载情况字典 {"1":{left_width_in:??,left_width_out:??,height_in:??,height_out:??,goods_in:[??]，goods_out:[??]}, "2":...}
    :param item: 货物  [品名，尺寸，物资代码，件数，散根，总根数，外径，成件的截面形状]
    :param truck_width: 车宽
    :return:
    """
    # 货物的高
    item_height = float(item.quantity_size.split("*")[0])
    # 货物的宽
    item_width = float(item.quantity_size.split("*")[1])
    # 得到当前的层数
    floor_num = len(box_list)
    # 判断box_list是否为空
    if floor_num != 0:
        # 遍历每一层
        for i in range(1, floor_num + 1):
            # 得到这一层里面的剩余宽度
            left_width_in = float(box_list[i]["left_width_in"])
            # 得到这一层外面的剩余宽度
            left_width_out = float(box_list[i]["left_width_out"])
            # 得到这一层里面的高度
            height_in = float(box_list[i]["height_in"])
            # 得到这一层外面的高度
            height_out = float(box_list[i]["height_out"])
            if item.segment == 2:
                # 判断空间是否足够放下该item
                quantity_overspread(item_height, item_width, height_in, left_width_in, item, box_list, "left_width_in",i)
                quantity_overspread(item_height, item_width, height_out, left_width_out, item, box_list, "left_width_out",i)
            else:
                quantity_overspread(item_height, item_width, height_out, left_width_out, item, box_list, "left_width_out",i)
        # 已有层都已经摆放过后 ， 继续向新一层摆放
        while item.quantity:
            floor_num += 1
            new_floor_quantity(box_list, truck_width, item_width, item_height, item, floor_num)
    else:
        while item.quantity:
            floor_num += 1
            new_floor_quantity(box_list, truck_width, item_width, item_height, item, floor_num)
    return box_list


def put_free_pcs_goods(box_list, item, truck_width):
    """
    根据传入的item摆放货物(散根数)
    :param box_list: 车辆的装载情况字典 {"1":{left_width_in:??,left_width_out:??,height_in:??,height_out:??,goods_in:[??]，goods_out:[??]}, "2":...}
    :param item: 货物  [品名，尺寸，物资代码，件数，散根，总根数，外径，成件的截面形状]
    :param truck_width: 车宽
    :return:
    """
    delattr(item, "total_pcs")
    # 货物的高
    item_height = float(item.free_pcs_size.split("*")[0])
    # 货物的宽
    item_width = float(item.free_pcs_size.split("*")[1])
    # 得到当前的层数
    floor_num = len(box_list)
    if floor_num != 0:
        # 只从最上层开始摆放
        # 得到这一层里面的剩余宽度
        last_floor=len(box_list)
        left_width_in = float(box_list[last_floor]["left_width_in"])
        # 得到这一层外面的剩余宽度
        left_width_out = float(box_list[last_floor]["left_width_out"])
        # 得到这一层里面的高度
        height_in = float(box_list[last_floor]["height_in"])
        # 得到这一层外面的高度
        height_out = float(box_list[last_floor]["height_out"])
        if item.segment == 2:
            # 判断空间是否足够放下该item
            free_pcs_overspread(item_height, item_width, height_in, left_width_in, item, box_list, "left_width_in",last_floor)
            free_pcs_overspread(item_height, item_width, height_out, left_width_out, item, box_list, "left_width_out",last_floor)
        else:
            free_pcs_overspread(item_height, item_width, height_out, left_width_out, item, box_list, "left_width_out",last_floor)
        # 已有层都已经摆放过后 ， 继续向新一层摆放
        while item.free_pcs:
            floor_num += 1
            new_floor_free_pcs(box_list, truck_width, item_width, item_height, item, floor_num)
    else:
        while item.free_pcs:
            floor_num += 1
            new_floor_free_pcs(box_list, truck_width, item_width, item_height, item, floor_num)
    return box_list


def quantity_overspread(item_height, item_width, height, left_width, item, box_list, left_width_io, floor):
    """
    将传入的item摆放在剩余的的宽度(整件)
    :param item_height: 待摆放货物的高
    :param item_width: 待摆放货物的宽
    :param height: 该层的货物高度
    :param left_width: 该层剩余宽度
    :param item: 待放货物 type: list
    :param box_list: 车层清单 type: dict
    :param left_width_io: left_width_in/left_width_out , 用来区别内外两层
    :param floor: 车层
    :return:
    """
    if item.segment == 2:
        # 只要剩余宽度比待放货物的高和宽任意一个大，就算是能放得下
        if item_height < left_width or item_width < left_width:
            # 该层剩余空间可放该item的件数 和 每件的宽度,  比较横着放和竖着放，选取放入数量多的那一种
            can_put_quantity, width, height_new = (
                math.floor(left_width / item_width), item_width, item_height) if math.floor(
                left_width / item_width) > math.floor(left_width / item_height) else (
                math.floor(left_width / item_height), item_height, item_width)
            # 复制一个货物信息，用来添加到该层所装的货物信息中
            put_item = copy.deepcopy(item)
            # 如果可放件数小于货物总数，则放入件数为可放件数
            if can_put_quantity < item.quantity:
                # 修改在该层中放的件数
                put_item.quantity = can_put_quantity
                # 扣去摆放的件数， 得到剩余的件数
                item.quantity -= can_put_quantity
            # 否则为 货物总数，且修改货物总数为0
            else:
                can_put_quantity = item.quantity
                put_item.quantity = item.quantity
                item.quantity = 0
            # 更新该层的剩余宽度，此处对单层进行分析，所以摆放数量不必除以二
            box_list[floor][left_width_io] = float(left_width) - float(width) * float(can_put_quantity)
            # 区分内外层添加所装货物
            if left_width_io == "left_width_in":
                goods_io = "goods_in"
                height_io = "height_in"
            else:
                goods_io = "goods_out"
                height_io = "height_out"
                # 将货物直接添加到goods列表末尾
            box_list[floor][goods_io].append(put_item)
            # 更新层高
            if height < height_new:
                box_list[floor][height_io] = height_new
    elif item.segment == 1:
        # 得到当前内外层剩余宽度较小的宽度
        left_width = box_list[floor]["left_width_in"] if box_list[floor]["left_width_in"] < box_list[floor][
            "left_width_out"] else \
            box_list[floor][
                "left_width_out"]
        # 只要剩余宽度比待放货物的高和宽任意一个大，就算是能放得下
        if item_height < left_width or item_width < left_width:
            # 该层可放的件数
            can_put_quantity, width, height_new = (left_width // float(item_width), item_width, item_height) if float(
                item_width) < float(item_height) else (left_width // float(item_height), item_height, item_width)
            put_item = copy.deepcopy(item)
            # 如果可放件数小于货物总数，则放入件数为可放件数
            if can_put_quantity < item.quantity:
                # 修改在该层中放的件数
                put_item.quantity = can_put_quantity
                # 扣去摆放的件数， 得到剩余的件数
                item.quantity -= can_put_quantity
            # 否则为 货物总数，且修改货物总数为0
            else:
                can_put_quantity = item.quantity
                put_item.quantity = item.quantity
                item.quantity = 0
            box_list[floor]["left_width_in"] -= float(width) * float(can_put_quantity)
            box_list[floor]["left_width_out"] -= float(width) * float(can_put_quantity)
            # 添加该层的货物
            box_list[floor]["goods_in"].append(put_item)
            box_list[floor]["goods_out"].append(put_item)
            # 更新层高
            if box_list[floor]["height_in"] < height_new:
                box_list[floor]["height_in"] = height_new
            # 更新层高
            if box_list[floor]["height_out"] < height_new:
                box_list[floor]["height_out"] = height_new

def free_pcs_overspread(item_height, item_width, height, left_width, item, box_list, left_width_io, floor):
    """
    将传入的item摆放在剩余的的宽度(整件)
    :param item_height: 待摆放货物的高
    :param item_width: 待摆放货物的宽
    :param height: 该层的货物高度
    :param left_width: 该层剩余宽度
    :param item: 待放货物 type: list
    :param box_list: 车层清单 type: dict
    :param left_width_io: left_width_in/left_width_out , 用来区别内外两层
    :param floor: 车层
    :return:
    """
    if item.segment == 2:
        # 只要剩余宽度比待放货物的高和宽任意一个大，就算是能放得下
        if item_height < left_width or item_width < left_width:
            # 该层剩余空间可放该item的件数 和 每件的宽度,  比较横着放和竖着放，选取放入数量多的那一种
            can_put_pcs, width, height_new = (
                math.floor(left_width / item_width), item_width, item_height) if math.floor(
                left_width / item_width) > math.floor(left_width / item_height) else (
                math.floor(left_width / item_height), item_height, item_width)
            # 复制一个货物信息，用来添加到该层所装的货物信息中
            put_item = copy.deepcopy(item)
            # 如果可放件数小于货物总数，则放入件数为可放件数
            if can_put_pcs < item.free_pcs:
                # 修改在该层中放的散根数
                put_item.can_put_pcs = can_put_pcs
                # 扣去摆放的散根数， 得到剩余的散根数
                item.free_pcs -= can_put_pcs
            # 否则为 散根总数，且修改散根总数为0
            else:
                can_put_pcs = item.free_pcs
                put_item.free_pcs = item.free_pcs
                item.free_pcs = 0
            # 更新该层的剩余宽度，此处对单层进行分析，所以摆放数量不必除以二
            box_list[floor][left_width_io] = float(left_width) - float(width) * float(can_put_pcs)
            # 区分内外层添加所装货物
            if left_width_io == "left_width_in":
                goods_io = "goods_in"
                height_io = "height_in"
            else:
                goods_io = "goods_out"
                height_io = "height_out"
            # 判断当前层货物是否存在,则直接修改散根
            is_exist = False
            for item in box_list[floor][goods_io]:
                if item.item_id==put_item.item_id:
                    is_exist=True
                    item.free_pcs=put_item.free_pcs
                    break
            if not is_exist:
                box_list[floor][goods_io].append(put_item)
            # 更新层高
            if height < height_new:
                box_list[floor][height_io] = height_new
    elif item.segment == 1:
        # 得到当前内外层剩余宽度较小的宽度
        left_width = box_list[floor]["left_width_in"] if box_list[floor]["left_width_in"] < box_list[floor][
            "left_width_out"] else \
            box_list[floor][
                "left_width_out"]
        # 只要剩余宽度比待放货物的高和宽任意一个大，就算是能放得下
        if item_height < left_width or item_width < left_width:
            # 该层可放的件数
            can_put_pcs, width, height_new = (left_width // float(item_width), item_width, item_height) if float(
                item_width) < float(item_height) else (left_width // float(item_height), item_height, item_width)
            put_item = copy.deepcopy(item)
            # 如果可放件根数小于货物散根数，则放入件数为可放散根数
            if can_put_pcs < item.free_pcs:
                # 修改在该层中放的件数
                put_item.free_pcs = can_put_pcs
                # 扣去摆放的件数， 得到剩余的件数
                item.free_pcs -= can_put_pcs
            # 否则为 货物散根数，且修改货物散根数为0
            else:
                can_put_pcs = item.free_pcs
                put_item.free_pcs = item.free_pcs
                item.free_pcs = 0
            box_list[floor]["left_width_in"] -= float(width) * float(can_put_pcs)
            box_list[floor]["left_width_out"] -= float(width) * float(can_put_pcs)
            # 添加该层的货物
            box_list[floor]["goods_in"].append(put_item)
            box_list[floor]["goods_out"].append(put_item)
            # 更新层高
            if box_list[floor]["height_in"] < height_new:
                box_list[floor]["height_in"] = height_new
            # 更新层高
            if box_list[floor]["height_out"] < height_new:
                box_list[floor]["height_out"] = height_new

def new_floor_quantity(box_list, truck_width, item_width, item_height, item, new_floor):
    """
    添加新的一层，摆放货物(整件)
    :param box_list: 车层清单 type:dict
    :param truck_width:卡车宽度
    :param item_width:货物宽度
    :param item_height:货物高度
    :param item:货物信息 type:list
    :param new_floor:新的一层  层数
    :param segment: 在车长维度上放几段货物
    :return:
    """
    # 构建新一层
    box_list[new_floor] = {"left_width_in": truck_width, "left_width_out": truck_width, "height_in": 0, "height_out": 0,
                           "goods_in": [],
                           "goods_out": []}
    # 得到该层可摆放的件数
    next_floor_can_put_quantity = math.floor(box_list[new_floor]["left_width_in"] / item_width) * 2
    # 拷贝货物信息
    next_floor_put_item1 = copy.deepcopy(item)
    next_floor_put_item2 = copy.deepcopy(item)
    if item.segment == 2:
        if next_floor_can_put_quantity < item.quantity:
            # 扣去摆放的件数，得到剩余的件数
            item.quantity -= next_floor_can_put_quantity
        # 否则为货物总数，且修改货物总数为0
        else:
            next_floor_can_put_quantity = item.quantity
            item.quantity = 0
        # 判断摆放的货物件数是否为偶数， 为偶数则内外层摆放数量一致，否则内层多摆
        if next_floor_can_put_quantity % 2 != 0:
            outer_layer_num = next_floor_can_put_quantity // 2
            inner_layer_num = outer_layer_num + 1
        else:
            outer_layer_num = inner_layer_num = next_floor_can_put_quantity / 2
            # 修改放在该层的货物信息
        # 如果外层为0则不添加信息
        if outer_layer_num != 0:
            next_floor_put_item1.quantity = outer_layer_num
            next_floor_put_item1.free_pcs = 0
            next_floor_put_item1.total_pcs = 0
            box_list[new_floor]["left_width_out"] -= outer_layer_num * item_width
            box_list[new_floor]["height_out"] = item_height
            box_list[new_floor]["goods_out"].append(next_floor_put_item1)
        next_floor_put_item2.quantity = inner_layer_num
        next_floor_put_item2.free_pcs = 0
        next_floor_put_item2.total_pcs = 0
        box_list[new_floor]["left_width_in"] -= inner_layer_num * item_width
        box_list[new_floor]["height_in"] = item_height
        box_list[new_floor]["goods_in"].append(next_floor_put_item2)

    elif item.segment == 1:
        if next_floor_can_put_quantity / 2 < item.quantity:
            # 扣去摆放的件数，得到剩余的件数
            item.quantity -= next_floor_can_put_quantity / 2
            next_floor_can_put_quantity = next_floor_can_put_quantity / 2
        # 否则为货物总数，且修改货物总数为0
        else:
            next_floor_can_put_quantity = item.quantity
            item.quantity = 0
        next_floor_put_item1.quantity = next_floor_can_put_quantity
        next_floor_put_item1.free_pcs = 0
        next_floor_put_item1.total_pcs = 0
        box_list[new_floor]["left_width_out"] -= next_floor_can_put_quantity * item_width
        box_list[new_floor]["height_out"] = item_height
        box_list[new_floor]["goods_out"].append(next_floor_put_item1)
        box_list[new_floor]["left_width_in"] -= next_floor_can_put_quantity * item_width
        box_list[new_floor]["height_in"] = item_height
        box_list[new_floor]["goods_in"].append(next_floor_put_item1)

def new_floor_free_pcs(box_list, truck_width, item_width, item_height, item, new_floor):
    """
    添加新的一层，摆放货物(散根，在整件摆放的基础上)
    :param box_list: 车层清单 type:dict
    :param truck_width:卡车宽度
    :param item_width:货物宽度
    :param item_height:货物高度
    :param item:货物信息 type:list
    :param new_floor:新的一层  层数
    :param segment: 在车长维度上放几段货物
    :return:
    """
    # 构建新一层
    box_list[new_floor] = {"left_width_in": truck_width, "left_width_out": truck_width, "height_in": 0, "height_out": 0,
                           "goods_in": [],
                           "goods_out": []}
    # 得到该层可摆放的件数
    next_floor_can_put_pcs = math.floor(box_list[new_floor]["left_width_in"] / item_width) * 2
    # 拷贝货物信息
    next_floor_put_item1 = copy.deepcopy(item)
    next_floor_put_item2 = copy.deepcopy(item)
    if item.segment == 2:
        if next_floor_can_put_pcs < item.free_pcs:
            # 扣去摆放的件数，得到剩余的件数
            item.free_pcs -= next_floor_can_put_pcs
        # 否则为货物总数，且修改货物总数为0
        else:
            next_floor_can_put_pcs = item.free_pcs
            item.free_pcs = 0
        # 判断摆放的货物件数是否为偶数， 为偶数则内外层摆放数量一致，否则内层多摆
        if next_floor_can_put_pcs % 2 != 0:
            outer_layer_num = next_floor_can_put_pcs // 2
            inner_layer_num = outer_layer_num + 1
        else:
            outer_layer_num = inner_layer_num = next_floor_can_put_pcs / 2
            # 修改放在该层的货物信息
        # 如果外层为0则不添加信息
        if outer_layer_num != 0:
            next_floor_put_item1.free_pcs = outer_layer_num
            box_list[new_floor]["left_width_out"] -= outer_layer_num * item_width
            box_list[new_floor]["height_out"] = item_height
            box_list[new_floor]["goods_out"].append(next_floor_put_item1)
        next_floor_put_item2.free_pcs = inner_layer_num
        box_list[new_floor]["left_width_in"] -= inner_layer_num * item_width
        box_list[new_floor]["height_in"] = item_height
        box_list[new_floor]["goods_in"].append(next_floor_put_item2)

    elif item.segment == 1:
        if next_floor_can_put_pcs / 2 < item.free_pcs:
            # 扣去摆放的根数，得到剩余的根数
            item.free_pcs -= next_floor_can_put_pcs / 2
            next_floor_can_put_pcs = next_floor_can_put_pcs / 2
        # 否则为货物总根数，且修改货物总根数为0
        else:
            next_floor_can_put_pcs = item.free_pcs
            item.free_pcs = 0
        next_floor_put_item1.free_pcs = next_floor_can_put_pcs
        box_list[new_floor]["left_width_out"] -= next_floor_can_put_pcs * item_width
        box_list[new_floor]["height_out"] = item_height
        box_list[new_floor]["goods_out"].append(next_floor_put_item1)
        box_list[new_floor]["left_width_in"] -= next_floor_can_put_pcs * item_width
        box_list[new_floor]["height_in"] = item_height
        box_list[new_floor]["goods_in"].append(next_floor_put_item1)


def calculate_quantity_size(item_id, product_type):
    """
    计算成件品种的尺寸size
    :param item_id: 物资代码
    :return:
    """
    # 该货物每件的根数
    root_quantity = g.ITEM_A_DICT.get(item_id)["GS_PER"]
    if product_type == "方矩管":
        row, col = get_row_and_col(root_quantity)
        size = item_id[3:10]
        width = str(int(size.split("*")[0].lstrip("0")) * row)
        height = str(int(size.split("*")[1].lstrip("0")) * col)

    else:
        od_id = float(item_id.split("*")[0][3:6].lstrip("0"))
        # 一边上的根数
        root_side = 0.5 + math.sqrt(12 * root_quantity - 3) / 6
        # 计算一件的宽度
        width = str((root_side - 1) * od_id * 2 + 100)
        # 计算一件的高度
        height = str((root_side - 1) * od_id * math.sqrt(3) + 100)

    return height + "*" + width

def calculate_free_pcs_size(item_id, product_type):
    """
    计算散根品种的尺寸size
    :param item_id: 物资代码
    :return:
    """
    if product_type == "方矩管":
        size = item_id[3:10]
        width = str(int(size.split("*")[0].lstrip("0")))
        height = str(int(size.split("*")[1].lstrip("0")))

    else:
        od_id = float(item_id.split("*")[0][3:6].lstrip("0"))
        # 计算一件的宽度
        width = str(od_id)
        # 计算一件的高度
        height = str(od_id)

    return height + "*" + width


def caculate_total_quantity_height(load_list_backup,box_list):
    """
        计算车中货物的总高度,总重量，总数量,总根数

        :param boxlist: 货物列表
        :return: total_height_in,total_height_out,total_weight,total_quantity,total_pcs
        """
    # 装车内外总高度
    total_height_out = 0
    total_height_in = 0
    # 计算装车内外的总高度
    for k in box_list:
        total_height_out += box_list[k]["height_out"]
        total_height_in += box_list[k]["height_in"]
    total_weight=sum(load_list_backup[i].weight for i in range(len(load_list_backup)-1))
    total_quantity=sum(load_list_backup[i].quantity for i in range(len(load_list_backup)-1))
    total_pcs=sum(load_list_backup[i].total_pcs for i in range(len(load_list_backup)-1))
    return total_height_in, total_height_out,total_weight,total_quantity,total_pcs


def get_row_and_col(total_count: int):
    """
    方矩管根据一件的总支数，得到打包支数矩阵的行和列
    :param total_count:总支数
    :return: 行,列
    """
    integral_part = int(math.sqrt(total_count))
    if total_count % integral_part == 0:
        return integral_part, int(total_count / integral_part)
    else:
        flag = 0
        low = integral_part ** 2
        high = (integral_part + 1) ** 2
        if (total_count - low) >= (high - total_count):
            flag = integral_part + 1
        else:
            flag = integral_part
        for i in range(flag - 1, 0, -1):
            if total_count % i == 0:
                return i, int(total_count / i)


def sheets_to_load_list(sheets):
    """
       将发货通知单转为货物列表
       :param sheets:发货通知单
       :return: load_list
       """
    # 存放每一个发货单所装货物的列表
    load_list = []
    # 将货物按照load_task_id进行分类
    load_dict = {}
    # 下面将每个发货单上的货整理成一个列表添加到load_list中
    for sheet in sheets:
        if sheet.load_task_id not in load_dict:
            load_dict[sheet.load_task_id] = []

        # 遍历发货单中的每一个子单，整理子单信息添加到load_list中
        for item in sheet.items:
            # 车次号
            item.load_task_id=sheet.load_task_id
            # 分割 物资代码 得到外径：od_id
            item.od_id = item.item_id.split("*")[0][3:].lstrip("0")
            # 管长
            item.pipe_length = item.item_id.split("*")[-1]
            # 为焊管 则查成件的高和宽
            if item.product_type == "焊管":
                # 通过外径查询焊管成捆后和散根的高和宽和：quantity_size ，example：360*370
                item.quantity_size = calculate_quantity_size(item.item_id, item.product_type)
                item.free_pcs_size = calculate_free_pcs_size(item.item_id, item.product_type)
            # 为方矩管
            elif item.product_type == "方矩管":
                # 方矩管的规格
                item.quantity_size = calculate_quantity_size(item.item_id, item.product_type)
                item.free_pcs_size = calculate_free_pcs_size(item.item_id, item.product_type)
            # 为热镀
            elif item.product_type == "热镀":
                # 通过外径查询热镀成捆后的高和宽：quantity_size ，example：360*370
                item.quantity_size = calculate_quantity_size(item.item_id, item.product_type)
                item.free_pcs_size = calculate_free_pcs_size(item.item_id, item.product_type)
            elif item.product_type == "螺旋焊管":
                # 螺旋焊管按根数算，散根数等同于根数
                item.quantity_size = item.od_id + "*" + item.od_id
                item.free_pcs_size = item.od_id + "*" + item.od_id
                item.quantity=item.quantity+item.free_pcs
                item.free_pcs=0
            # 判断所画图形的形状
            if item.product_type in ["焊管", "热镀"]:
                item.shape = "六边形"
            elif item.product_type == "方矩管":
                item.shape = "矩形"
            elif item.product_type == "螺旋焊管":
                item.shape = "圆形"
            # 将子单信息按【车次号，品名，件尺寸，规格，件数，散根数，总根数, 外径，形状】的格式添加到load_list中
            load_dict[sheet.load_task_id].append(item)

    # 将每个订单的所有子单按照已录信息的外径从小到大排序
    for key in load_dict:
        load_dict[key].sort(key=lambda x: x.od_id)
        # 添加load_task_id
        load_dict[key].append(key)
        # 将该发货单中所装货物添加到load_list中
        load_list.append(load_dict[key])
    return load_list


def box_list_to_goods(box_list):
    """将数据结构打包为内层货物和外层货物"""
    goods_in_list = []
    goods_out_list = []
    for key in box_list:
        goods_in_floor = LoadingFloor()
        goods_in_floor.floor = key
        goods_in_floor.load_task_id = box_list[key]["goods_in"][0].load_task_id
        goods_in_floor.left_width_in = box_list[key]["left_width_in"]
        goods_in_floor.left_width_out = box_list[key]["left_width_out"]
        goods_in_floor.height_in = box_list[key]["height_in"]
        goods_in_floor.height_out = box_list[key]["height_out"]
        goods_out_floor = copy.deepcopy(goods_in_floor)
        goods_in_floor.goods_list = box_list[key]["goods_in"]
        goods_out_floor.goods_list = box_list[key]["goods_out"]
        # 动态去除goods_in,goods_out属性
        delattr(goods_in_floor, "goods_in")
        delattr(goods_in_floor, "goods_out")
        delattr(goods_out_floor, "goods_in")
        delattr(goods_out_floor, "goods_out")
        goods_in_list.append(goods_in_floor)
        goods_out_list.append(goods_out_floor)

    return goods_in_list, goods_out_list


def truck_list_to_object(loading_trucks):
    new_loading_trucks_list = []
    for truck in loading_trucks:
        good_object = LoadingTruck(truck)
        delattr(good_object,"loading_floors")
        new_loading_trucks_list.append(good_object)
    return new_loading_trucks_list
