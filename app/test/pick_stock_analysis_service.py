# -*- coding: utf-8 -*-
# Description: 库存查询
# Created: jjunf 2020/09/28
import pandas as pd

from app.test.pick_stock_analysis import Stock
from app.test.pick_stock_analysis_dao import stock_analysis_dao


def stock_analysis():
    """
    库存分析
    :return:
    """
    df_plan_stock = pd.DataFrame(stock_analysis_dao.select_plan_stock())
    df_lms_loading_main = pd.DataFrame(stock_analysis_dao.select_lms_loading_main_stock())
    df_lms_loading_detail = pd.DataFrame(stock_analysis_dao.select_lms_loading_detail_stock())
    df_out_stock = pd.DataFrame(stock_analysis_dao.select_out_stock_weight())
    df = pd.merge(df_plan_stock,df_lms_loading_main,how='inner',on='schedule_no')
    df = pd.merge(df,df_lms_loading_detail,how='inner',on='main_no')
    df = pd.merge(df,df_out_stock,how='inner',on='main_no')
    df['plan_weight'] = df['plan_weight'].astype('float64')

    L_df = len(df)
    # df.to_excel('df.xls')

    # 运营开单重量-实际出库重量
    df['plan_real_'] = df['plan_weight'] - df['real_weight']
    df['plan_real'] = abs(df['plan_weight'] - df['real_weight'])
    df_plan_real = [df.loc[df['plan_real'] < 0.2],
                    df.loc[(0.2 < df['plan_real']) & (df['plan_real'] < 0.5)],
                    df.loc[(0.5 < df['plan_real']) & (df['plan_real'] < 1)],
                    df.loc[(1 < df['plan_real']) & (df['plan_real'] < 2)],
                    df.loc[(2 < df['plan_real']) & (df['plan_real'] < 5)],
                    df.loc[df['plan_real'] > 5]]
    # df_plan_real中每一部分的个数
    L_df_plan_real = []
    for i in range(len(df_plan_real)):
        L_df_plan_real.append(len(df_plan_real[i]))

    # 运营开单重量-西门开单重量
    df['plan_xmkd_'] = df['plan_weight'] - df['xmkd_weight']
    df['plan_xmkd'] = abs(df['plan_weight'] - df['xmkd_weight'])
    df_plan_xmkd = [df.loc[df['plan_xmkd'] < 0.2],
                    df.loc[(0.2 < df['plan_xmkd']) & (df['plan_xmkd'] < 0.5)],
                    df.loc[(0.5 < df['plan_xmkd']) & (df['plan_xmkd'] < 1)],
                    df.loc[(1 < df['plan_xmkd']) & (df['plan_xmkd'] < 2)],
                    df.loc[(2 < df['plan_xmkd']) & (df['plan_xmkd'] < 5)],
                    df.loc[df['plan_xmkd'] > 5]]
    # df_plan_xmkd中每一部分的个数
    L_df_plan_xmkd = []
    for i in range(len(df_plan_xmkd)):
        L_df_plan_xmkd.append(len(df_plan_xmkd[i]))


    dic = df.to_dict(orient='record')
    stock_list = []
    for record in dic:
        stock = Stock(record)
        stock_list.append(stock)

    return stock_list


if __name__=='__main__':
    stock_analysis()
