from app.util.base.base_dao import BaseDao
import pandas as pd


class noInterestCountDao(BaseDao):
    # --查询前一天的短信发送情况
    def log_info_select(self):
        read_sql = '''
                    SELECT
                        pl.pickup_no,
                        pl.prod_name,
                        city_name,
                        district_name,
                        driver_id,
                        label_type,
                        create_date 
                    FROM
                        `t_propelling_log` pl 
                    WHERE
                        pl.prod_name IS NOT NULL 
                        AND pl.create_date > date_sub(NOW( ), INTERVAL 24 HOUR)'''
        data = self.select_all(read_sql)

        return pd.DataFrame(data)

    def driver_records_select(self):
        read_sql = '''
        SELECT
        *
        FROM
        db_ads.zd_pickup_order_driver_behavior 
        WHERE
        TO_DAYS(now())-TO_DAYS(insert_time) < 1'''
        data = self.select_all(read_sql)

        return pd.DataFrame(data)

    def no_interest_count_select(self):
        read_sql = '''SELECT * FROM t_driver_no_interest_count'''
        data = self.select_all(read_sql)

        return pd.DataFrame(data)

    def no_interest_count_insert(self, values):
        insert_sql = '''INSERT INTO t_driver_no_interest_count(
                            driver_id,
                            district,
                            product_name,
                            `count`,
                            update_time
                        )
                        VALUE(%s,%s,%s,%s,%s)'''
        self.execute(insert_sql, values=values)

    def no_interest_count_update(self, values):
        sql_list = []
        # 先清空表
        delete_sql = '''DELETE
                        FROM t_driver_no_interest_count'''
        sql_list.append(delete_sql)

        # 插入更新数据
        insert_sql = '''INSERT INTO t_driver_no_interest_count(
                            driver_id,
                            district,
                            product_name,
                            `count`,
                            update_time
                        )
                        VALUE(%s,%s,%s,%s,%s)'''
        sql_list.append(insert_sql)
        self.execute_many_sql(sql_list, values=values)


no_interest_count_dao = noInterestCountDao()
