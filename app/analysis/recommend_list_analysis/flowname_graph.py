import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
# 以下两句是显示中文的方法
from pylab import *
import numpy as np


def flowname_graph_build(flowname_df):
    flowname_graph = nx.DiGraph()
    for index, row in flowname_df.iterrows():
        pre_flowname = -1
        for flowname in row:
            if pd.isnull(flowname):  # 空值忽略
                break
            if flowname_graph.__contains__(flowname) is False:  # 如果图中没有此流向，则加入
                flowname_graph.add_node(flowname)
            if pre_flowname == -1:  # 是当前行的第一个值
                pre_flowname = flowname
                continue
            if pre_flowname != flowname:  # 两个流向不一样
                if flowname_graph.has_edge(pre_flowname, flowname):  # 如果已经存在边，权重+1
                    flowname_graph[pre_flowname][flowname]['weight'] += 1
                else:  # 否则，加入边，权重初始为1
                    flowname_graph.add_edge(pre_flowname, flowname, weight=1)
            pre_flowname = flowname
    return flowname_graph


def out_to_csv(outpath, df, index):
    try:
        df.to_csv(outpath, index=index)
    except:
        print("out error")


if __name__ == "__main__":
    mpl.rcParams['font.sans-serif'] = ['SimHei']
    inpath = "docs/driver_flownames3.csv"
    flowname_df = pd.read_csv(inpath, low_memory=False)
    flowname_graph = flowname_graph_build(flowname_df)
    graph_flows = flowname_graph.nodes()  # 流向索引
    flow_index = list(flowname_graph.nodes)
    # print(flow_index[2])
    # print(flowname_graph.nodes)
    # nx.draw(flowname_graph,with_labels=True)
    # plt.show()

    # flowname_graph.nodes = []
    A = nx.adjacency_matrix(flowname_graph).todense()  # 将图转为邻接矩阵
    A[A < 5] = 0
    new_graph = nx.DiGraph()
    new_graph.add_nodes_from(flow_index)
    [rows, cols] = A.shape

    count_diff = {}
    diff_list = []
    max_diff = 0
    max_pos = [None] * 2
    min_diff = 0
    min_pos = [None] * 2
    for i in range(rows):
        for j in range(cols):
            # print(A[i,j])
            if i >= j:
                continue
            # new_graph.add_edge(flow_index[i],flow_index[j],weight=A[i,j])
            count_diff[str(i) + "-" + str(j)] = A[i, j] - A[j, i]
            diff_list.append(A[i, j] - A[j, i])
            print(str(i) + "-" + str(j) + ":", A[i, j] - A[j, i])
            if (A[i, j] - A[j, i]) > max_diff:
                max_diff = A[i, j] - A[j, i]
                max_pos = [i, j]
            if (A[i, j] - A[j, i]) < min_diff:
                min_diff = A[i, j] - A[j, i]
                min_pos = [i, j]
    print("max:", max_diff)
    print(max_pos[0], " ", max_pos[1])
    print(flow_index[114], " ", flow_index[115])
    print("min:", min_diff)
    print(min_pos[0], " ", min_pos[1])
    print(flow_index[128], " ", flow_index[129])

    # diff_avg = 0
    # for i in diff_list:
    #     diff_avg += i
    # print(diff_avg / (len(diff_list)))
    # print(type(new_graph))
    #
    #
    # nx.draw(new_graph,with_labels=True)
    # plt.show()

    # a_np = np.array(A)
    # a_np[a_np<5] = 0
    # outpath = "docs/flownames_matrixv2.csv"
    # out_to_csv(outpath=outpath,df=a_np,index=None)

    # a_df = pd.DataFrame(a_np,index=graph_flows,columns=graph_flows)
    # outpath = "docs/flowname_matrix.csv"
    # a_df.to_csv(outpath)

    # inpath = "docs/flowname_matrix.csv"
    # flowmx_df = pd.read_csv(inpath)
    # graph = nx.from_numpy_matrix(flowmx_df)
