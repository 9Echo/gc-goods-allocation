# -*- coding: utf-8 -*-
# @Time    : 2019/11/11 15:32
# @Author  : Zihao.Liu
import datetime


class BaseEntity:
    """实体类的基类"""

    def as_dict(self):
        """将对象转为dict返回"""
        return {attr: self.get_attr_str(attr) for attr in self.__dict__.keys()}

    def get_attr_str(self, attr):
        """读取类成员变量，将其统一转为string"""
        value = getattr(self, attr)
        if isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, list) and value and isinstance(value[0], BaseEntity):
            # 如果list内部对象为BaseEntity也将其转为dict
            return [i.as_dict() for i in value]
        else:
            return value

    def set_attr(self, value: dict):
        """根据传入的对象设置成员变量"""
        for attr in self.__dict__.keys():
            if value.__contains__(attr):
                setattr(self, attr, value.get(attr))

