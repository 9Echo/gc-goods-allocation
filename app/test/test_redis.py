import json

import pandas as pd
import pymysql
import redis
from DBUtils.PooledDB import PooledDB

try:
    # 数据库连接池
    db_pool_trans_plan = PooledDB(pymysql, 2, host="47.99.118.183", port=3306, user='v3dev_user',
                                  passwd='V3dev!56', db='db_trans_plan', charset="utf8")
    # redis连接池
    redis_pool = redis.ConnectionPool(
        host='47.99.118.183', port=6379, password="wobugaoxing", max_connections=2,
        encoding='utf8', decode_responses=True)
    print('info: redis connected successfully.\n')
except Exception as e:
    print(e)


def test_redis_string():
    """测试redis的字符串缓存功能"""
    redis_conn = redis.Redis(connection_pool=redis_pool)

    redis_conn.set('user:1:name', 'zhangsan1张三')
    ret = redis_conn.get('user:1:name')
    print(ret, type(ret))

    redis_conn.set('user:1:age', 18)
    ret = redis_conn.get('user:1:age')
    print(ret, type(ret))
    redis_conn.incr('user:1:age', 5)
    ret = redis_conn.get('user:1:age')
    print(ret, type(ret))
    ret = int(ret)
    print(ret, type(ret))

    redis_conn.close()


def get_student_list():
    """使用数据库连接池，进行数据查询，结果缓存到redis"""
    try:
        conn = db_pool_trans_plan.connection()
        sql = "SELECT * FROM t_student LIMIT 5"
        df_data = pd.read_sql(sql, conn)
        df_data.create_date = df_data.apply(lambda df: df.create_date.strftime('%Y-%m-%d %H:%M:%S'), axis=1)
        df_data.update_date = df_data.apply(lambda df: df.update_date.strftime('%Y-%m-%d %H:%M:%S'), axis=1)

        print('\nget_student_list start')

        dict_data = df_data.to_dict(orient='records')
        print(dict_data)
        json_data = json.dumps(dict_data)
        print(json_data)

        print('get_student_list end\n')

        # redis cache
        redis_conn = redis.Redis(connection_pool=redis_pool)
        redis_conn.set('student_list', json_data)
        redis_conn.close()
        #
        result = {"code": 100, "msg": "成功", "data": dict_data}

    except Exception as e:
        result = {"code": -1, "msg": "系统错误"}
        print(e)
    finally:
        conn.close()
    #
    return result


if __name__ == "__main__":
    # redis字符串缓存
    test_redis_string()

    # 数据库查询，结果缓存到redis
    result = get_student_list()
    redis_conn = redis.Redis(connection_pool=redis_pool)
    json_student_list = redis_conn.get('student_list')
    redis_conn.close()

    print(json_student_list)

    dict_student_list = json.loads(json_student_list)
    print(dict_student_list)

    df_student = pd.io.json.json_normalize(dict_student_list)
    print(df_student)

