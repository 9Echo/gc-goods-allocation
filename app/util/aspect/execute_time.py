# -*- coding: utf-8 -*-
# Description: 
# Created: shaoluyu 2020/7/13 17:48


def print_execute_time(func):
    from time import time

    # 定义嵌套函数，用来打印出装饰的函数的执行时间
    def wrapper(*args, **kwargs):
        # 定义开始时间和结束时间，将func夹在中间执行，取得其返回值
        start = time()
        func_return = func(*args, **kwargs)
        end = time()
        # 打印方法名称和其执行时间
        print(f'{func.__name__}() execute time: {end - start}s')
        # 返回func的返回值
        return func_return

    # 返回嵌套的函数
    return wrapper
