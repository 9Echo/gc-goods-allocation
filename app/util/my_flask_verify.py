# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/9/9 13:45


class MyFlaskVerify:
    @staticmethod
    def str_type(value, name):
        if not value:
            raise ValueError(name + '不能为空！')
        elif not isinstance(value, str):
            raise TypeError(name + '类型错误！')
        else:
            return value

    @staticmethod
    def number_type(value, name):
        if not value:
            raise ValueError(name + '不能为空！')
        elif not (isinstance(value, float) or isinstance(value, int)):
            raise TypeError(name + '类型错误！')
        else:
            return value

    @staticmethod
    def number_value(value, name):
        if value <= 0:
            raise ValueError(name + '必须是大于0的数！')
        else:
            return value

    @staticmethod
    def truck_submit_count(value, name):
        pass
