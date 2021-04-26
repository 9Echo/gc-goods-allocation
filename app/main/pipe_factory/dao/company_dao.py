import pandas as pd

from app.util.base.base_dao import BaseDao
from app.util.db_pool import db_pool_trans_plan, db_pool_ods
import traceback
from datetime import datetime


class GetCompanyData(BaseDao):

    @staticmethod
    def get_company_data(last_time):
        try:
            conn = db_pool_ods.connection()
            sql = """
            SELECT
            tw.waybill_no,
            tw.create_date as create_date,
            tw.travel_no as travel_no,
            tf.f_docuno,
            tf.b_docuno,
            tf.order_cal,
            tf.SJGBSL,
            tf.org_unit_name,
            tf.f_crted_date,
            tf.j_crted_date
            FROM (
            select
            f_docuno,
            b_docuno,
            order_cal,
            SJGBSL,
            org_unit_name,
            f_crted_date,
            j_crted_date
            from (
            select
            tab_f.docuno as f_docuno, -- 发货通知单号
            tab_j.docuno as b_docuno, -- 结算单号
            order_cal, -- 理重
            SJGBSL,-- 实重
            tab_f.org_unit_name as org_unit_name,-- 客户名
            tab_f.crted_date as f_crted_date,
            tab_j.crted_date as j_crted_date
            from (
            SELECT
            docuno, -- 发货通知单号
            org_unit, -- 客户id
            org_unit_name, -- 客户名称
            crted_date, -- 创建时间
            order_cal, -- 理重合计
            bath_no, -- 批次号
            data_address -- 数据来源  0010：衡水  0020：唐山
            FROM db_dw.ods_db_inter_t_keeperhd
            where data_address = '0030'
            and crted_date between '{0}' and '{1}'
            ) as tab_f
            left join
            (
            SELECT
            doctype, -- 单子类型
            docuno, -- 结算单号
            HXDH,  -- 发货通知单号
            org_unit,  -- 客户id
            org_unit_name, -- 客户名称
            crted_date, -- 创建时间
            SJGBSL, -- 结算重量
            bath_no, -- 批次号
            data_address -- 业务来源（0010：衡水，0020：唐山，0030：成都，0040：吉林，0050：郑州，0060：日照，0070：广州）
            FROM db_dw.ods_db_inter_t_orderhd
            where data_address = '0030' and SJGBSL < 30
            ) as tab_j
            on tab_f.docuno = tab_j.HXDH
            where tab_j.docuno is not NULL
            order by tab_f.docuno
    
            ) as tab
            ) as tf
            left join
            (SELECT
            waybill_no,
            main_product_list_no,
            create_date,
            travel_no
            FROM db_dw.ods_db_trans_t_waybill
            where company_id = 'C000000888')as tw
            on tw.main_product_list_no = tf.f_docuno
            where travel_no is not null
                """
            now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sql = sql.format(last_time, now_time)
            data = pd.read_sql(sql, conn)
            return data, now_time
        except Exception as e:
            print("get_company_data error")
            traceback.print_exc()
        finally:
            conn.close()

    def write_database(self, re, now_time):
        """
        :return:
        """
        try:
            sql = "insert into t_compose_company(company_name1, company_name2, match_size, update_time) values('{}','{}','{}','{}')"
            # for re in result:
            # print(re[0][0])
            # print(re[1])
            self.execute(sql.format(re[0][0], re[0][1], re[1], now_time))
            print("finish!")
        except Exception as e:
            print("write_database error!")
            traceback.print_exc()

    @staticmethod
    def add_match_size(company_list, result_dic):
        for li in company_list:
            company_con1 = (li[0], li[1])
            company_con2 = (li[1], li[0])
            if company_con1 not in result_dic and company_con2 not in result_dic:
                result_dic[company_con1] = 1
            else:
                if company_con1 in result_dic:
                    result_dic[company_con1] = result_dic[company_con1] + 1
                elif company_con2 in result_dic:
                    result_dic[company_con2] = result_dic[company_con2] + 1

    def update_database_size(self, re):
        try:
            sql = """
                update db_trans_plan.t_compose_company 
                set match_size = '{0}'
                where company_name1 = '{1}' and company_name2 = '{2}'
            """
            self.execute(sql.format(re[1], re[0][0], re[0][1]))
        except Exception as e:
            print("company_dao.update_database_size error")
            traceback.print_exc()

    def update_database_time(self, data):
        """更新数据库时间
            data:要写入的数据  形如：update_time
        """
        try:
            sql = """
                update db_trans_plan.t_compose_company 
                set  update_time = '{}'
            """

            self.execute(sql.format(data))
        except Exception as e:
            print("company_dao.update_database_time is error")
            traceback.print_exc()


if __name__ == '__main__':
    last_time = {'2019-11-06 15:37:14'}
    las = list(last_time)
    g = GetCompanyData()
    data, now_time = g.get_company_data(las[0])
    print(data, now_time)
    # data = get_data_from_table()
    # print(data)
    # existing_data = get_data_from_table()
    # print(existing_data)
    # last_time = set(existing_data['update_time'].values)
    # print(last_time)
