# -*- coding: utf-8 -*-
# Description: 异常捕捉
# Created: shaoluyu 2019/03/05
from pymysql import MySQLError
from flask import current_app
from app.main import blueprint
from app.util.my_exception import MyException
from app.util.result import Result


@blueprint.app_errorhandler(MySQLError)
def handle_mysql_exception(e):
    """封装数据库错误信息"""
    current_app.logger.exception(e)
    return Result.error_response("数据库错误")


@blueprint.app_errorhandler(KeyError)
def handle_key_exception(e):
    """缺少输入参数错误信息"""
    current_app.logger.exception(e)
    return Result.error_response("缺少输入参数" + str(e))


@blueprint.app_errorhandler(ValueError)
def handle_value_exception(e):
    """传入参数的值错误信息"""
    current_app.logger.exception(e)
    return Result.error_response("传入参数的值错误" + str(e))


@blueprint.app_errorhandler(TypeError)
def handle_type_exception(e):
    """传入参数的类型错误信息"""
    current_app.logger.exception(e)
    return Result.error_response("传入参数的类型错误" + str(e))


@blueprint.app_errorhandler(Exception)
def handle_type_exception(e):
    """系统错误"""
    current_app.logger.exception(e)
    return Result.error_response("系统错误")


@blueprint.app_errorhandler(MyException)
def handle_type_exception(me):
    """自定义异常"""
    current_app.logger.error(me.message)
    current_app.logger.exception(me)
    return Result.error_response(me.message)
