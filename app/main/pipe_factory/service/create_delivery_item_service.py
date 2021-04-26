import math
import copy
from app.main.pipe_factory.entity.delivery_item import DeliveryItem
from app.util import weight_calculator
from app.util.bean_convert_utils import BeanConvertUtils
from app.util.code import ResponseCode
from app.util.my_exception import MyException
from model_config import ModelConfig


class CreateDeliveryItem:
    def __init__(self, order):
        """
        将订单数据转为订单子项的list
        :param order:订单数据
        :return delivery_item:订单子项的list
        """
        self.delivery_item_list = []
        for item in order.items:
            delivery_item = BeanConvertUtils.copy_properties(item, DeliveryItem)
            delivery_item.max_quantity = ModelConfig.ITEM_ID_DICT.get(delivery_item.item_id[:3])
            delivery_item.volume = (delivery_item.quantity / delivery_item.max_quantity
                                    if delivery_item.max_quantity else ModelConfig.DEFAULT_VOLUME)
            delivery_item.weight = weight_calculator.calculate_weight(delivery_item.product_type, delivery_item.item_id,
                                                                      delivery_item.quantity, delivery_item.free_pcs)
            delivery_item.total_pcs = weight_calculator.calculate_pcs(delivery_item.product_type, delivery_item.item_id,
                                                                      delivery_item.quantity, delivery_item.free_pcs)
            # 如果遇到计算不出来的明细，计算异常
            if delivery_item.weight <= 0:
                raise MyException('物资代码' + delivery_item.item_id + '未维护理论重量或维护错误！', ResponseCode.Error)

            self.delivery_item_list.append(delivery_item)

    def spec(self):
        """
        根据规格优先再对订单子项的list再进行拆分
        :return:
        """

        for delivery_item in self.delivery_item_list:
            # 如果该明细有件数上限并且单规格件数超出，进行切单
            if delivery_item.max_quantity and delivery_item.quantity > delivery_item.max_quantity:
                # copy次数
                count = math.floor(delivery_item.quantity / delivery_item.max_quantity)
                # 最后一个件数余量
                surplus = delivery_item.quantity % delivery_item.max_quantity
                # 标准件数的重量和总根数
                new_weight = weight_calculator.calculate_weight(delivery_item.product_type, delivery_item.item_id,
                                                                delivery_item.max_quantity, 0)
                new_total_pcs = weight_calculator.calculate_pcs(delivery_item.product_type, delivery_item.item_id,
                                                                delivery_item.max_quantity, 0)
                # 创建出count个拷贝的新明细，散根数为0，件数为标准件数，总根数为标准总根数，体积占比近似为1
                for _ in range(0, count):
                    copy_dilivery_item = copy.deepcopy(delivery_item)
                    copy_dilivery_item.free_pcs = 0
                    copy_dilivery_item.quantity = delivery_item.max_quantity
                    copy_dilivery_item.volume = 1
                    copy_dilivery_item.weight = new_weight
                    copy_dilivery_item.total_pcs = new_total_pcs
                    # 将新明细放入明细列表
                    self.delivery_item_list.append(copy_dilivery_item)
                # 原明细更新件数为剩余件数，体积占比通过件数/标准件数计算
                delivery_item.quantity = surplus
                delivery_item.volume = (delivery_item.quantity / delivery_item.max_quantity
                                        if delivery_item.max_quantity else ModelConfig.DEFAULT_VOLUME)
                delivery_item.weight = weight_calculator.calculate_weight(delivery_item.product_type,
                                                                          delivery_item.item_id, delivery_item.quantity,
                                                                          delivery_item.free_pcs)
                delivery_item.total_pcs = weight_calculator.calculate_pcs(delivery_item.product_type,
                                                                          delivery_item.item_id, delivery_item.quantity,
                                                                          delivery_item.free_pcs)

        return self.delivery_item_list

    def weight(self):
        """
        创建提货单子项
        :return:
        """
        # product_type = None
        item_list = []  # 用于装拆散后的订单子项，单个元素要么是单捆，要么是散件
        # new_max_weight = 0

        for delivery_item in self.delivery_item_list:  # 遍历每个订单子项
            # 单捆重量
            weight1 = weight_calculator.calculate_weight(delivery_item.product_type, delivery_item.item_id, pack_num=1,
                                                         free_num=0)
            total_pcs1 = weight_calculator.calculate_pcs(delivery_item.product_type, delivery_item.item_id, pack_num=1,
                                                         free_num=0)
            # 单散根重量
            weight2 = weight_calculator.calculate_weight(delivery_item.product_type, delivery_item.item_id, pack_num=0,
                                                         free_num=1)
            total_pcs2 = 1

            # 单捆的子项
            for _ in range(delivery_item.quantity):
                item = BeanConvertUtils.copy_properties(delivery_item, DeliveryItem)
                item.quantity = 1
                item.free_pcs = 0
                item.max_quantity = ModelConfig.ITEM_ID_DICT.get(delivery_item.item_id[:3])
                item.volume = 1 / item.max_quantity if item.max_quantity else ModelConfig.DEFAULT_VOLUME
                item.weight = weight1
                item.total_pcs = total_pcs1
                item_list.append(item)

            # 单散根的子项
            for _ in range(delivery_item.free_pcs):
                item = BeanConvertUtils.copy_properties(delivery_item, DeliveryItem)
                item.quantity = 0
                item.free_pcs = 1
                item.volume = 0
                item.weight = weight2
                item.total_pcs = total_pcs2
                item_list.append(item)
        return item_list

    def optimize(self):
        max_delivery_items = []
        min_delivery_items = []

        for delivery_item in self.delivery_item_list:

            # 搜集小管
            if delivery_item.volume == ModelConfig.DEFAULT_VOLUME:
                min_delivery_items.append(delivery_item)
                continue
            # 搜集大管
            # 如果该明细有件数上限并且单规格件数超出，进行切单
            if delivery_item.max_quantity and delivery_item.quantity > delivery_item.max_quantity:
                # copy次数
                count = math.floor(delivery_item.quantity / delivery_item.max_quantity)
                # 最后一个件数余量
                surplus = delivery_item.quantity % delivery_item.max_quantity
                # 标准件数的重量和总根数
                new_weight = weight_calculator.calculate_weight(delivery_item.product_type, delivery_item.item_id,
                                                                delivery_item.max_quantity, 0)
                new_total_pcs = weight_calculator.calculate_pcs(delivery_item.product_type, delivery_item.item_id,
                                                                delivery_item.max_quantity, 0)
                # 创建出count个拷贝的新明细，散根数为0，件数为标准件数，总根数为标准总根数，体积占比近似为1
                for _ in range(0, count):
                    copy_di = copy.deepcopy(delivery_item)
                    copy_di.free_pcs = 0
                    copy_di.quantity = delivery_item.max_quantity
                    copy_di.volume = 1
                    copy_di.weight = new_weight
                    copy_di.total_pcs = new_total_pcs
                    # 将新明细放入明细列表
                    max_delivery_items.append(copy_di)
                # 原明细更新件数为剩余件数，体积占比通过件数/标准件数计算
                delivery_item.quantity = surplus
                delivery_item.volume = (delivery_item.quantity / delivery_item.max_quantity
                                        if delivery_item.max_quantity else ModelConfig.DEFAULT_VOLUME)
                delivery_item.weight = weight_calculator.calculate_weight(delivery_item.product_type,
                                                                          delivery_item.item_id, delivery_item.quantity,
                                                                          delivery_item.free_pcs)
                delivery_item.total_pcs = weight_calculator.calculate_pcs(delivery_item.product_type,
                                                                          delivery_item.item_id, delivery_item.quantity,
                                                                          delivery_item.free_pcs)

            max_delivery_items.append(delivery_item)

        return max_delivery_items, min_delivery_items
