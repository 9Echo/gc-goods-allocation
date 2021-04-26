from app.util.aspect.method_before import get_item_a
from flask import g
import math
import turtle as t
import requests
from app.main.entity.loading_truck import LoadingTruck
from app.main.entity.loading_floor import LoadingFloor
from app.main.entity.loading_item import LoadingItem

"""
车型：13米，车宽2.4米，车厢高1.5米
1.确定摆放顺序，（将大、小管分类）
2.获取打包参数，确定摆放位置：[第几层，放什么，放几件，放几根，层高，本层剩余宽度]
"""


@get_item_a
def loading(sheets, car_info):
    # 车长
    car_length = car_info[0]
    # 车宽
    car_width = car_info[1]
    # 车侧板高
    car_height = car_info[2]

    # 发货通知单转配载列表
    load_list = sheets_to_load_list(sheets)
    # 装配货物
    loading_trucks = load_goods(car_length, load_list)
    # 对装配数据处理，转为对象
    trucks_list = truck_list_to_object(loading_trucks)

    return trucks_list


def load_goods(car_length, load_list):
    """
        根据传入的list装配货物
        :param load_list: 车辆的装载情况字典 {"1":{left_width_in:??,left_width_out:??,height_in:??,height_out:??,goods_in:[??]，goods_out:[??]}, "2":...}
        :param car_length: 车长
        :return:loading_trucks(字典)"""
    loading_trucks = []
    for items in load_list:
        # 存放所装的货物
        box_list = {}
        # item为发货单中的每一种货物
        for i in range(len(items) - 1):
            item = items[i]
            # 货物长
            product_length = float(item[7])
            # 表示该货物在车长面能够放几段
            part = car_length // product_length
            # 摆放货物
            box_list = put_goods(box_list, item, car_length, part)
        # 计算车内货物的总高度

        total_height_in, total_height_out = caculate_total_hight(box_list)
        # 将每个一辆车上的货物清单和车内外车高都添加到box中
        loading_trucks.append(
            {"load_task_id": items[-1], "loading_floors": box_list, "total_height_in": total_height_in,
             "total_height_out": total_height_out})

    return loading_trucks


def put_goods(box_list, item, car_length, part):
    """
    根据传入的item摆放货物
    :param box_list: 车辆的装载情况字典 {"1":{left_width_in:??,left_width_out:??,height_in:??,height_out:??,goods_in:[??]，goods_out:[??]}, "2":...}
    :param item: 货物  [品名，尺寸，物资代码，件数，散根，总根数，外径，成件的截面形状]
    :param car_length: 车长
    :param part: 在车长维度上放几段货物
    :return:
    """
    # 货物的高
    item_height = float(item[1].split("*")[0])
    # 货物的宽
    item_width = float(item[1].split("*")[1])
    # 得到当前的层数
    n = len(box_list)
    # 判断box_list是否为空
    if n != 0:
        # 遍历每一层
        for i in range(1, n + 1):
            # 得到这一层里面的剩余宽度
            left_width_in = float(box_list[i]["left_width_in"])
            # 得到这一层外面的剩余宽度
            left_width_out = float(box_list[i]["left_width_out"])
            # 得到这一层里面的高度
            height_in = float(box_list[i]["height_in"])
            # 得到这一层外面的高度
            height_out = float(box_list[i]["height_out"])
            if part == 2:
                # 判断空间是否足够放下该item
                overspread(item_height, item_width, height_in, left_width_in, item, box_list, "left_width_in", i, part)
                overspread(item_height, item_width, height_out, left_width_out, item, box_list, "left_width_out", i,
                           part)
            else:
                overspread(item_height, item_width, height_out, left_width_out, item, box_list, "left_width_out", i,
                           part)
        # 已有层都已经摆放过后 ， 继续向新一层摆放
        while item[3]:
            n += 1
            new_floor(box_list, item_width, item_height, item, n, part)
    else:
        while item[3]:
            n += 1
            new_floor(box_list, item_width, item_height, item, n, part)
    return box_list


def overspread(item_height, item_width, height, left_width, item, box_list, left_width_io, floor, part):
    """
    将传入的item摆放在剩余的的宽度
    :param item_height: 待摆放货物的高
    :param item_width: 待摆放货物的宽
    :param height: 该层的货物高度
    :param left_width: 该层剩余宽度
    :param item: 待放货物 type: list
    :param box_list: 车层清单 type: dict
    :param left_width_io: left_width_in/left_width_out , 用来区别内外两层
    :param floor: 车层
    :param part: 在车长维度上放几段货物
    :return:
    """
    if part == 2:
        # 只要剩余宽度比待放货物的高和宽任意一个大，就算是能放得下
        if item_height < left_width or item_width < left_width:
            # 该层剩余空间可放该item的件数 和 每件的宽度,  比较横着放和竖着放，选取放入数量多的那一种
            can_put_quantity, width, height_new = (
                math.floor(left_width / item_width), item_width, item_height) if math.floor(
                left_width / item_width) > math.floor(left_width / item_height) else (
                math.floor(left_width / item_height), item_height, item_width)
            # 复制一个货物信息，用来添加到该层所装的货物信息中
            put_item = item.copy()
            # 将散根数和总根数都置0
            put_item[4] = 0
            put_item[5] = 0
            # 如果可放件数小于货物总数，则放入件数为可放件数
            if can_put_quantity < item[3]:
                # 修改在该层中放的件数
                put_item[3] = can_put_quantity
                # 扣去摆放的件数， 得到剩余的件数
                item[3] -= can_put_quantity
            # 否则为 货物总数，且修改货物总数为0
            else:
                can_put_quantity = item[3]
                put_item[3] = item[3]
                item[3] = 0
            # 更新该层的剩余宽度，此处对单层进行分析，所以摆放数量不必除以二
            box_list[floor][left_width_io] = float(left_width) - float(width) * float(can_put_quantity)
            # 区分内外层添加所装货物
            if left_width_io == "left_width_in":
                goods_io = "goods_in"
                height_io = "height_in"
            else:
                goods_io = "goods_out"
                height_io = "height_out"
            # 存放虚拟货物的下标
            list_invented = []
            # 找出该层货物中为虚的货物并记录下标
            for product in box_list[floor][goods_io]:
                if product[9] == "F":
                    list_invented.append(box_list[floor][goods_io].index(product))
            # 有虚拟货物的情况
            if list_invented:
                # 一共有几件虚拟货物
                times = len(list_invented)
                for time in range(can_put_quantity):
                    # 如果list_invented中有这个下标表示还有虚拟货物可以替换，则先替换虚拟的货物
                    if time <= (times - 1):
                        # 单件替换
                        put_item[3] = 1
                        # 将虚拟货物取出
                        box_list[floor][goods_io].pop(list_invented[time])
                        # 替换box_list中虚拟的货物
                        box_list[floor][goods_io].insert(list_invented[time], put_item)
                    # 如果没有虚拟货物了，就直接将剩余的货物添加到末尾
                    else:
                        # 添加剩余的件数
                        put_item[3] = can_put_quantity - times
                        # 添加到goods列表末尾
                        box_list[floor][goods_io].append(put_item)
                        break
            # 没有虚拟货物的情况
            else:
                # 将货物直接添加到goods列表末尾
                box_list[floor][goods_io].append(put_item)
            # 更新层高
            if height < height_new:
                box_list[floor][height_io] = height_new
    elif part == 1:
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
            put_item = item.copy()
            # 将散根数和总根数都置0
            put_item[4] = 0
            put_item[5] = 0
            # 如果可放件数小于货物总数，则放入件数为可放件数
            if can_put_quantity < item[3]:
                # 修改在该层中放的件数
                put_item[3] = can_put_quantity
                # 扣去摆放的件数， 得到剩余的件数
                item[3] -= can_put_quantity
            # 否则为 货物总数，且修改货物总数为0
            else:
                can_put_quantity = item[3]
                put_item[3] = item[3]
                item[3] = 0
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


def new_floor(box_list, truck_width, item_width, item_height, item, new_floor, segment):
    """
    添加新的一层，摆放货物
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
    next_floor_put_item1 = item.copy()
    next_floor_put_item2 = item.copy()
    next_floor_put_item3 = item.copy()
    if segment == 2:
        if next_floor_can_put_quantity < item[3]:
            # 扣去摆放的件数，得到剩余的件数
            item[3] -= next_floor_can_put_quantity
        # 否则为货物总数，且修改货物总数为0
        else:
            next_floor_can_put_quantity = item[3]
            item[3] = 0
        # 判断摆放的货物件数是否为偶数， 为偶数则内外层摆放数量一致，否则内层多摆
        if next_floor_can_put_quantity % 2 != 0:
            outer_layer_num = next_floor_can_put_quantity // 2
            inner_layer_num = outer_layer_num + 1
        else:
            outer_layer_num = inner_layer_num = next_floor_can_put_quantity / 2
            # 修改放在该层的货物信息
        # 如果外层为0则不添加信息
        if outer_layer_num != 0:
            next_floor_put_item1[3] = outer_layer_num
            next_floor_put_item1[4] = 0
            next_floor_put_item1[5] = 0
            box_list[new_floor]["left_width_out"] -= outer_layer_num * item_width
            box_list[new_floor]["height_out"] = item_height
            box_list[new_floor]["goods_out"].append(next_floor_put_item1)
        next_floor_put_item2[3] = inner_layer_num
        next_floor_put_item2[4] = 0
        next_floor_put_item2[5] = 0
        box_list[new_floor]["left_width_in"] -= inner_layer_num * item_width
        box_list[new_floor]["height_in"] = item_height
        box_list[new_floor]["goods_in"].append(next_floor_put_item2)
        if inner_layer_num != outer_layer_num:
            next_floor_put_item3[3] = 1
            next_floor_put_item3[4] = 0
            next_floor_put_item3[5] = 0
            next_floor_put_item3[9] = "F"
            box_list[new_floor]["goods_out"].append(next_floor_put_item3)

    elif segment == 1:
        if next_floor_can_put_quantity / 2 < item[3]:
            # 扣去摆放的件数，得到剩余的件数
            item[3] -= next_floor_can_put_quantity / 2
            next_floor_can_put_quantity = next_floor_can_put_quantity / 2
        # 否则为货物总数，且修改货物总数为0
        else:
            next_floor_can_put_quantity = item[3]
            item[3] = 0
        next_floor_put_item1[3] = next_floor_can_put_quantity
        next_floor_put_item1[4] = 0
        next_floor_put_item1[5] = 0
        box_list[new_floor]["left_width_out"] -= next_floor_can_put_quantity * item_width
        box_list[new_floor]["height_out"] = item_height
        box_list[new_floor]["goods_out"].append(next_floor_put_item1)
        box_list[new_floor]["left_width_in"] -= next_floor_can_put_quantity * item_width
        box_list[new_floor]["height_in"] = item_height
        box_list[new_floor]["goods_in"].append(next_floor_put_item1)


def draw_hexagon(side, turtle, product_type, item_id, T):
    """
    画一个六边形

    :param side: 边长
    :param turtle: 海龟绘图对象
    :param product_type: 品种
    :param item_id: 物资代码
    :param T: 判断是否为实体
    :return:
    """
    turtle.right(60)
    if T == "T":
        for n in range(6):
            turtle.fd(side)
            turtle.left(60)
    else:
        for n in range(6):
            for k in range(5):
                turtle.fd(side / 10)
                turtle.pu()
                turtle.fd(side / 10)
                turtle.pd()
            turtle.left(60)
    turtle.seth(0)
    turtle.pu()
    turtle.fd(side / 3)
    turtle.write(product_type)
    turtle.write(" " * 10 + item_id)
    turtle.bk(side / 3)
    turtle.seth(0)


def draw_circle(r, turtle, product_type, item_id, T):
    """
    画一个圆形

    :param r: 圆的半径
    :param turtle: 海龟绘图对象
    :param product_type: 品种
    :param item_id: 物资代码
    :param T: 是否为实体
    :return:
    """
    turtle.seth(-90)
    if T == "T":
        turtle.circle(r)
    else:
        for i in range(36):
            turtle.circle(r, 5)
            turtle.pu()
            turtle.circle(r, 5)
            turtle.pd()
    turtle.seth(0)
    turtle.pu()
    turtle.fd(r / 5)
    turtle.write(product_type)
    turtle.write(" " * 15 + item_id)
    turtle.bk(r / 5)
    turtle.seth(0)


def draw_rectangle(side, turtle, product_type, item_id, T):
    """
    画一个矩形
    :param side: 边长
    :param turtle: 海龟绘图对象
    :param product_type: 品种
    :param item_id: 物资代码
    :param T: 是否为实体
    :return:
    """
    turtle.seth(-90)
    if T == "T":
        turtle.fd(side / 2)
        for i in range(3):
            turtle.left(90)
            turtle.fd(side)
        turtle.left(90)
        turtle.fd(side / 2)
    else:
        for i in range(2):
            turtle.fd(side / 8)
            turtle.pu()
            turtle.fd(side / 8)
            turtle.pd()
        for j in range(3):
            turtle.left(90)
            for q in range(4):
                turtle.fd(side / 8)
                turtle.pu()
                turtle.fd(side / 8)
                turtle.pd()
        turtle.left(90)
        for i in range(2):
            turtle.fd(side / 8)
            turtle.pu()
            turtle.fd(side / 8)
            turtle.pd()
    turtle.seth(0)
    turtle.pu()
    turtle.fd(side / 7)
    turtle.write(product_type)
    turtle.write(" " * 15 + item_id)
    turtle.bk(side / 7)
    turtle.seth(0)


def distance(dis, turtle):
    """
    显示剩余距离的
    :param dis: 剩余距离
    :param turtle: 海龟绘图对象
    :return:
    """
    if dis > 45:
        dis2 = dis / 2
    else:
        dis2 = dis
    turtle.left(60)
    turtle.fd(10)
    turtle.bk(10)
    turtle.seth(-60)
    turtle.fd(10)
    turtle.bk(10)
    turtle.seth(0)
    turtle.fd(dis2 / 5)
    turtle.write(dis * 2)
    turtle.fd(dis2 * 4 / 5)
    turtle.seth(120)
    turtle.fd(10)
    turtle.bk(10)
    turtle.seth(-120)
    turtle.fd(10)
    turtle.seth(0)


def draw_car(car_width, car_height, turtle):
    """
    画一个车厢
    :param car_width: 车宽
    :param car_height: 车侧栏高
    :param turtle: 海龟绘图对象
    :return:
    """
    turtle.setup(1000, 1000, 200, 200)
    turtle.speed(10000)
    turtle.penup()
    turtle.goto(-car_width / 2, car_height / 2)
    turtle.pendown()
    turtle.pensize(2)
    turtle.right(90)
    turtle.fd(car_height)
    turtle.left(90)
    turtle.fd(car_width)
    turtle.left(90)
    turtle.fd(car_height)


def draw_product(car_dict, turtle, io):
    """
    根据传入的车层信息绘制截面图
    :param car_dict: 车层信息 type：dict
    :param turtle: 海龟绘图对象
    :param io: 判断绘制内/外 部分
    :return:
    """
    # 按比例缩小画图原为2400 缩小一倍
    car_width = 1200
    car_height = 750
    draw_car(car_width, car_height, turtle)
    height_now = 0
    for i in car_dict:
        # 得到每一层的信息的字典
        info = car_dict[i]
        # 层高
        floor_height = info["height_" + io]
        # 层宽
        floor_left_width = info["left_width_" + io]
        # 该层的货物
        floor_goods = info["goods_" + io]
        # 记录图中画的高度
        draw_height = 0
        # 遍历这一层的所有货物
        for j in floor_goods:
            # 获得截面形状
            shape = j[8]
            # 件数
            quantity = int(j[3])
            # 宽
            width = j[1].split("*")[1] if j[1].split("*")[0] > j[1].split("*")[1] else j[1].split("*")[0]
            if shape == "六边形":
                draw = math.sqrt(3) / 2 * float(width)
            elif shape in ["圆形", "矩形"]:
                draw = float(width)
            if draw_height == 0:
                draw_height = draw
                # 起始位置的坐标
                start_x = -car_width / 2
                start_y = (-car_height / 2 + float(draw_height) / 4 + float(height_now) / 2)
                # 将画笔移到初始点
                turtle.penup()
                turtle.pensize(1)
                turtle.goto(start_x, start_y)
                turtle.speed(100)
                turtle.pendown()
                turtle.seth(0)
            else:
                now_height = draw
                diff = now_height - draw_height
                draw_height = now_height
                turtle.pu()
                turtle.seth(90)
                turtle.fd(diff / 4)
                turtle.seth(0)
                turtle.pd()
            if shape == "六边形":
                side = float(width) / 4
                for k in range(quantity):
                    draw_hexagon(side, turtle, j[0], j[2], j[9])
                    turtle.pu()
                    turtle.fd(2 * side)
                    turtle.pd()
            elif shape == "圆形":
                r = float(width) / 4
                for k in range(quantity):
                    draw_circle(r, turtle, j[0], j[2], j[9])
                    turtle.pu()
                    turtle.fd(2 * r)
                    turtle.pd()
            elif shape == "矩形":
                side = float(width) / 2
                for k in range(quantity):
                    draw_rectangle(side, turtle, j[0], j[2], j[9])
                    turtle.pu()
                    turtle.fd(side)
                    turtle.pd()
        distance(floor_left_width / 2, turtle)
        height_now += floor_height
    t.done()
    # ts = t.getscreen()
    # ts.getcanvas().postscript(file='abc.eps')
    # turtle.done()


def calculate_size(item_id, product_type):
    """
    计算品种的尺寸size
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


def caculate_total_hight(box_list: list):
    """
        计算车中货物的总高度

        :param boxlist: 货物列表
        :return: total_height_in,total_height_out
        """
    # 装车内外总高度
    total_height_out = 0
    total_height_in = 0
    # 计算装车内外的总高度
    for k in box_list:
        total_height_out += box_list[k]["height_out"]
        total_height_in += box_list[k]["height_in"]
    return total_height_in, total_height_out


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
            # 分割 物资代码 得到外径：od_id
            od_id = item.item_id.split("*")[0][3:].lstrip("0")
            # 管长
            pipe_length = item.item_id.split("*")[-1]
            # 为焊管 则查成件的高和宽
            if item.product_type == "焊管":
                # 通过外径查询焊管成捆后的高和宽：size ，example：360*370
                size = calculate_size(item.item_id, item.product_type)
            # 为方矩管
            elif item.product_type == "方矩管":
                # 方矩管的外径取长的一个，所以去第二个位置的数据
                # od_id = item.item_id.split("*")[1].lstrip("0")
                # temp_size = item.item_id[3:10].lstrip("0")
                # size = ModelConfig.FANGJU_PACK_SIZE[temp_size]
                size = calculate_size(item.item_id, item.product_type)
            # 为热镀
            elif item.product_type == "热镀":
                # 通过外径查询热镀成捆后的高和宽：size ，example：360*370
                size = calculate_size(item.item_id, item.product_type)
            elif item.product_type == "螺旋焊管":
                size = od_id + "*" + od_id
            # 判断所画图形的形状
            if item.product_type in ["焊管", "热镀"]:
                shape = "六边形"
            elif item.product_type == "方矩管":
                shape = "矩形"
            elif item.product_type == "螺旋焊管":
                shape = "圆形"
            # 将子单信息按【品名，件尺寸，规格，件数，散根数，总根数, 外径，形状, 是否为实体】的格式添加到load_list中
            load_dict[sheet.load_task_id].append([item.product_type,
                                                  size,
                                                  item.item_id,
                                                  item.quantity,
                                                  item.free_pcs,
                                                  item.total_pcs,
                                                  float(od_id),
                                                  pipe_length,
                                                  shape,
                                                  "T"])

    # 将每个订单的所有子单按照已录信息的外径从小到大排序
    for key in load_dict:
        load_dict[key].sort(key=lambda x: x[6])
        # 添加load_task_id
        load_dict[key].append(key)
        # 将该发货单中所装货物添加到load_list中
        load_list.append(load_dict[key])
    return load_list


def truck_list_to_object(loading_trucks):
    new_loading_trucks_list = []
    for truck in loading_trucks:
        floor_list = []
        for key in truck["loading_floors"]:
            goods_in = []
            goods_out = []
            for item in truck["loading_floors"][key]["goods_in"]:
                loading_item = LoadingItem()
                loading_item.product_type = item[0]
                loading_item.size = item[1]
                loading_item.item_id = item[2]
                loading_item.quantity = item[3]
                loading_item.free_pcs = item[4]
                loading_item.total_pcs = item[5]
                loading_item.od_id = item[6]
                loading_item.pipe_length = item[7]
                loading_item.is_entity = item[9]
                goods_in.append(loading_item)

            for item in truck["loading_floors"][key]["goods_out"]:
                loading_item = LoadingItem()
                loading_item.product_type = item[0]
                loading_item.size = item[1]
                loading_item.item_id = item[2]
                loading_item.quantity = item[3]
                loading_item.free_pcs = item[4]
                loading_item.total_pcs = item[5]
                loading_item.od_id = item[6]
                loading_item.pipe_length = item[7]
                loading_item.is_entity = item[9]
                goods_out.append(loading_item)

            loading_floor = LoadingFloor(truck["loading_floors"][key])
            loading_floor.floor = key
            loading_floor.goods_in = goods_in
            loading_floor.goods_out = goods_out
            floor_list.append(loading_floor)
        # 重新填充
        truck["loading_floors"] = floor_list
        good_object = LoadingTruck(truck)
        new_loading_trucks_list.append(good_object)
    return new_loading_trucks_list


if __name__ == '__main__':
    # with open("result.json", "rt", encoding='utf-8') as f:
    #     temp = json.loads(f.read())
    #     print(temp)
    #     result = loading(temp, [12000, 2400, 1500])
    #     print(result[0][0])
    result = requests.post("http://localhost:9238/order", headers={"Content-Type": "application/json"}, data={
        "data": {
            "customer_id": "scymymygxgs",
            "salesman_id": "4",
            "company_id": "00",
            "weight": 0,
            "items": [{
                "product_type": "方矩管",
                "spec": "02A165*4.25*6000",
                "item_id": "058040*040*2.75*6000",
                "f_whs": "sth",
                "f_loc": "sth",
                "material": "sth",
                "quantity": "30",
                "free_pcs": "0"
            }, {"product_type": "热镀",
                "spec": "016075.5*2.3*6000",
                "item_id": "020020.5*1.6*6000",
                "f_whs": "sth",
                "f_loc": "sth",
                "material": "sth",
                "quantity": "30",
                "free_pcs": "0"
                }, {"product_type": "螺旋焊管",
                    "spec": "016075.5*2.3*6000",
                    "item_id": "0C0219*8.0*12000",
                    "f_whs": "sth",
                    "f_loc": "sth",
                    "material": "sth",
                    "quantity": "20",
                    "free_pcs": "0"
                    }]
        }

    })
    # for i in result:
    #     print(i)
    # draw_product(result[0][0], t, "out")
    a = result.json()
    box_list = loading(result.json(), [12000, 2400, 1500])
    print(box_list[0][0])
