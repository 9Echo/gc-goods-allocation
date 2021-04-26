# -*- coding: utf-8 -*-
# Description: 数据库连接池
# Created: shaoluyu 2019/09/20
# Modified: shaoluyu 2019/10/10;

import pymysql
from DBUtils.PooledDB import PooledDB


import config

active_config = config.get_active_config()

db_pool_ods = None
if hasattr(active_config, 'ODS_MYSQL_HOST'):
    # 创建数据库连接池。
    # 后续数据库操作应从连接池获取连接，并在操作完成后关闭归还连接。
    db_pool_ods = PooledDB(
        pymysql,
        2,
        host=active_config.ODS_MYSQL_HOST,
        port=active_config.ODS_MYSQL_PORT,
        user=active_config.ODS_MYSQL_USER,
        passwd=active_config.ODS_MYSQL_PASSWD,
        db=active_config.ODS_MYSQL_DB,
        charset=active_config.ODS_MYSQL_CHARSET)


'''连接第二个数据库'''
db_pool_ods_2 = None
if hasattr(active_config, 'ODS_MYSQL_HOST_2'):
    # 创建数据库连接池。
    # 后续数据库操作应从连接池获取连接，并在操作完成后关闭归还连接。
    db_pool_ods_2 = PooledDB(
        pymysql,
        2,
        host=active_config.ODS_MYSQL_HOST_2,
        port=active_config.ODS_MYSQL_PORT_2,
        user=active_config.ODS_MYSQL_USER_2,
        passwd=active_config.ODS_MYSQL_PASSWD_2,
        db=active_config.ODS_MYSQL_DB_2,
        charset=active_config.ODS_MYSQL_CHARSET_2)
