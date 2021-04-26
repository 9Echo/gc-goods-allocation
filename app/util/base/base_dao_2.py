# -*- coding: utf-8 -*-
# Description:
# Created: jjunf 2020/12/03
import traceback
import pymysql
from pymysql import MySQLError

from app.util.db_pool import db_pool_ods_2


class BaseDao2:
    """封装数据库操作基础类"""

    def select_one(self, sql, values=None):
        _ = self
        conn = None
        cursor = None
        try:
            conn = db_pool_ods_2.connection()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            traceback.print_exc()
            raise MySQLError
        finally:
            cursor.close()
            conn.close()

    def select_all(self, sql, values=None):
        _ = self
        conn = None
        cursor = None
        try:
            conn = db_pool_ods_2.connection()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            traceback.print_exc()
            raise MySQLError
        finally:
            cursor.close()
            conn.close()

    def execute(self, sql, values=None):
        _ = self
        conn = None
        cursor = None
        try:
            conn = db_pool_ods_2.connection()
            cursor = conn.cursor()
            if values:
                cursor.execute(sql, values)
            else:
                cursor.execute(sql)
            conn.commit()
        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            raise MySQLError
        finally:
            cursor.close()
            conn.close()

    def executemany(self, sql, values=None):
        _ = self
        conn = None
        cursor = None
        try:
            conn = db_pool_ods_2.connection()
            cursor = conn.cursor()
            if values:
                cursor.executemany(sql, values)
            else:
                cursor.executemany(sql)
            conn.commit()
        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            raise MySQLError
        finally:
            cursor.close()
            conn.close()

    def execute_many_sql(self, sql_list, values):
        _ = self
        conn = None
        cursor = None
        try:
            conn = db_pool_ods_2.connection()
            cursor = conn.cursor()
            cursor.execute(sql_list[0])
            cursor.executemany(sql_list[1], values)
            conn.commit()
        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            raise MySQLError
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
