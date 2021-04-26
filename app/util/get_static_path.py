# -*- coding: utf-8 -*-
# Description:
# Created: shaoluyu 2020/05/06
import os


def get_path(static_name):
    cur_path = os.path.abspath(os.path.dirname(__file__))
    #print("cur",cur_path)
    root_path = cur_path[:cur_path.find("app") + len("app")]
    #print("root",root_path)
    #print(os.path.abspath(root_path + 'static/' + static_name))
    return os.path.abspath(root_path + '/test/static/' + static_name)
