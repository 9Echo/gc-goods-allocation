import pandas as pd
from typing import List, Tuple, Dict
from app.main.steel_factory.entity.load_task import LoadTask
from app.util.get_static_path import get_path
import operator


def generate_excel(load_task_list: List[LoadTask]):
    df = pd.DataFrame([item.as_dict() for item in load_task_list])
    writer = pd.ExcelWriter(get_path("分货结果.xlsx"))
    # 去除甩货
    raw_df = df
    df = df[df['load_task_id'] >= 1]

    df1 = df.groupby(['city', 'end_point', 'big_commodity']).agg(
        {'weight': 'sum'}).reset_index()
    df2 = df.groupby(['city', 'end_point'])['load_task_id'].nunique().reset_index()
    load_list = []
    load_advice_list = []
    # 统计区县配载建议以及车次数
    for index, row in df1.iterrows():
        joint_str_list = []
        temp_df_num = df2[(df2['city'] == row['city']) & (df2['end_point'] == row['end_point'])]
        load_list.append(list(temp_df_num['load_task_id'])[0])
        temp_df = df[(df['city'] == row['city']) & (df['end_point'] == row['end_point'])]
        for load_id in temp_df['load_task_id'].unique().tolist():
            unique_big_commodity = temp_df[temp_df['load_task_id'] == load_id]['big_commodity'].unique().tolist()
            if len(unique_big_commodity) > 1:
                joint_str_list.append(unique_big_commodity)
        # 去除重复的配载情况
        news_str_list = drop_duplicate_list(joint_str_list)
        # 拼接混载字符
        joint_str = concat_string(news_str_list)
        load_advice_list.append(joint_str)

    # 增加新的一列车次数
    df1['load_num'] = load_list
    df1['load_advice'] = load_advice_list
    # 更改字段名
    df = rename_load_result(raw_df)
    df1 = rename_collect_result(df1)

    df.to_excel(writer, sheet_name="分货车次明细")
    df1.to_excel(writer, sheet_name="分货车次汇总")
    writer.save()


def rename_load_result(dataframe):
    """
      分货结果字段重命名
      :param dataframe:
      :return:dataframe
      """
    dataframe = dataframe.rename(columns={
        "load_task_id": "车次号",
        "priority": "优先级",
        "load_task_type": "装卸类型",
        "total_weight": "总重量",
        "weight": "重量",
        "count": "件数",
        "city": "城市",
        "end_point": "区县",
        "big_commodity": "大品种",
        "commodity": "小品种",
        "notice_num": "发货通知单号",
        "oritem_num": "订单号",
        "standard": "规格",
        "priority_grade": "车次急发等级",
        "sgsign": "材质",
        "outstock_code": "出库仓库",
        "instock_code": "入库仓库",
        "receive_address": "卸货地址",
        "price_per_ton": "吨单价",
        "total_price": "总价格",
        "remark": "备注(配件)",
    })
    return dataframe


def rename_collect_result(dataframe):
    """
      分货汇总结果重命名
      :param dataframe:
      :return:dataframe
      """
    dataframe = dataframe.rename(columns={
        "city": "城市",
        "end_point": "区县",
        "big_commodity": "大品种",
        "weight": "总重量",
        "load_advice": "混装配载建议",
        "load_num": "区县总车次数"
    })
    return dataframe


def drop_duplicate_list(joint_str_list: List):
    """
      二位数组内部去除重复数组
      :param joint_str_list:
      :return:news_str_list
      """
    news_str_list = []
    if len(joint_str_list) != 0:
        news_str_list.append(joint_str_list[0])
        for str in joint_str_list:
            flag = False
            for new_str_item in news_str_list:
                if operator.eq(str, new_str_item):
                    flag = True
            if flag == False:
                news_str_list.append(str)
    return news_str_list


def concat_string(news_str_list):
    """
      拼接数组内品种字符
      :param news_str_list:
      :return:joint_str
      """
    joint_str = ""
    if len(news_str_list) != 0:
        for joint_item in news_str_list:
            temp_str = joint_item[0]
            for index in range(1, len(joint_item)):
                temp_str = temp_str + "和" + joint_item[index]
            joint_str = joint_str + temp_str + "混装,"
    return joint_str


if __name__ == '__main__':
    generate_excel()
