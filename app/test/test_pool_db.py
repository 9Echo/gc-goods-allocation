import json
import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB


# 创建数据库连接池。
# 后续数据库操作应从连接池获取数据库连接，并在操作完成后关闭归还数据库连接。
db_pool_trans_plan = PooledDB(pymysql, 2, host="47.99.118.183", port=3306, user='v3dev_user',
                              passwd='V3dev!56', db='db_trans_plan', charset="utf8")


def get_student_list():
    """使用数据库连接池，进行数据查询"""
    try:
        conn = db_pool_trans_plan.connection()
        cursor = conn.cursor()
        sql = "SELECT * FROM t_student LIMIT 3"
        cursor.execute(sql)
        results = cursor.fetchall()
        print(results)
        print(results[0][6])
    except Exception as e:
        print(e)
    finally:
        print('\nfinally')
        print('conn: ', conn.__class__, conn)
        print('cursor: ', cursor.__class__, cursor)
        print(results.__class__, 'results len = ', len(results))
        cursor.close()
        conn.close()


def get_student_list_by_df():
    """使用数据库连接池，进行数据查询"""
    try:
        conn = db_pool_trans_plan.connection()
        sql = "SELECT * FROM t_student LIMIT 3"
        student_pd = pd.read_sql(sql, conn)
        student_pd.create_date = student_pd.apply(lambda df: df.create_date.strftime('%Y-%m-%d %H:%M:%S'), axis=1)
        student_pd.update_date = student_pd.apply(lambda df: df.update_date.strftime('%Y-%m-%d %H:%M:%S'), axis=1)
        dict_data = student_pd.to_dict(orient='records')

        result = {"code": 100, "msg": "成功", "data": dict_data}
        print(result)
    except Exception as e:
        print(e)
    finally:
        print('\nfinally')
        print('conn: ', conn.__class__, conn)
        print(dict_data.__class__, 'results len = ', len(dict_data))
        conn.close()


if __name__ == "__main__":
    get_student_list()
    # get_student_list_by_df()
