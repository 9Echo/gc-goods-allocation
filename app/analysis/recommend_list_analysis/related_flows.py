import pandas as pd
import pymysql as pym


def related_flows():
    """
    1、取得所有区县
    2、计算每个流向与其他流向度，取度排前10对应的流向，度为0的不要
    3、返回
    :return:
    """
    inpath = "docs/flowname_matrix.csv"
    flowname_matrix_df = pd.read_csv(inpath)
    related_flows_dict = {}
    # print(flowname_matrix_df.head(10))
    flow_list = flowname_matrix_df['Unnamed: 0'].tolist()
    print(len(flow_list))
    print(flow_list)
    for flow in flow_list:
        print(flow)
        for other_flow in flow_list:
            row = flowname_matrix_df[flowname_matrix_df['Unnamed: 0'] == flow]
            # print(row.index)
            out_weight = row[other_flow]
            out_weight = out_weight[row.index.tolist()[0]]
            row2 = flowname_matrix_df[flowname_matrix_df['Unnamed: 0'] == other_flow]
            in_weight = row2[flow]
            in_weight = in_weight[row2.index.tolist()[0]]
            # total_weight = out_weight+in_weight

            total_weight = out_weight + in_weight
            if total_weight == 0:
                continue
            if related_flows_dict.__contains__(flow) is False:
                related_flows_dict[flow] = {}
                related_flows_dict[flow][other_flow] = total_weight
            else:
                related_flows_dict[flow][other_flow] = total_weight

    # inpath2 = "docs/pretreated_data.csv"
    # flow_count = {}
    # pretreated_data = pd.read_csv(inpath2)
    # for index,row in pretreated_data.iterrows():
    #     flow = row['end_district_name']
    #     if flow_count.__contains__(flow):
    #         flow_count[flow] += 1
    #     else:
    #         flow_count[flow] = 1
    # #除各流向运单总数
    # for main_flow in related_flows_dict.keys():
    #     for related_flow in related_flows_dict[main_flow].keys():
    #         if flow_count[flow]<10:
    #
    #         weight = related_flows_dict[main_flow][related_flow]
    #         related_flows_dict[main_flow][related_flow] = float(weight)/(flow_count[related_flow]*)

    print(len(list(related_flows_dict.keys())))
    # 排序,筛选前10个
    new_relation = {}
    for flow in related_flows_dict.keys():
        ascending_sort = sorted(related_flows_dict[flow].items(), key=lambda x: x[1], reverse=True)  # 按value升序排序
        # if len(ascending_sort)>10:
        #     ascending_sort = ascending_sort[:10]
        new_relation[flow] = ascending_sort
        print(flow, ":", new_relation[flow])

    return new_relation


def relation_to_table(flow_relation, table):
    # 建立连接
    conn = pym.connect(
        host='am-bp16yam2m9jqm2tyk90650.ads.aliyuncs.com',
        database='db_model',
        user='bigdata_user4',
        password='user4!0525',
        port=3306,
    )
    # 建立游标
    cursor = conn.cursor()
    # 得到sql
    insert_sql = "insert into " + str(table) + "(main_flow,related_flow,correlation) values (%s,%s,%s)"
    for main_flow in flow_relation.keys():
        for flow_tuple in flow_relation[main_flow]:
            related_flow = flow_tuple[0]
            correlation = flow_tuple[1]
            correlation = float(str(correlation))
            new_tuple = (main_flow, related_flow, correlation)
            cursor.execute(insert_sql, new_tuple)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    flow_relation = related_flows()
    relation_to_table(flow_relation, "flows_transfer")
