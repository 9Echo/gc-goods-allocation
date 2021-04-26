# -*- coding: utf-8 -*-
# Description: 自定义异常类
# Created: shaoluyu 2019/12/14


class MyException(RuntimeError):
    """
    自定义异常类
    """
    def __init__(self, message, status):
        super().__init__(message, status)
        self.message = message
        self.status = status
