import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

from app.analysis.luchengkai.rule import swap_rule, free_time_rule


def free_time_statistic(df, str_type):
    train_data = np.array(df)  # 先将数据框转换为数组
    train_data_list = swap_rule.swap_df_to_list(train_data.tolist(), str_type)  # 其次转换为列表
    train_data_dic = swap_rule.split_trans_group(train_data_list, 'waybill_no')
    free_start_count_dic, free_stop_count_dic = free_time_rule.calculate_free_time(train_data_dic)

    # 绘图
    plt.title('free_time_quantum')
    list_start_count = [i for i in free_start_count_dic.values()]
    list_stop_count = [i for i in free_stop_count_dic.values()]
    list_date = [i for i in free_stop_count_dic.keys()]
    plt.plot(list_date, list_start_count, color='red', label='start_time_count')
    plt.plot(list_date, list_stop_count, color='green', label='stop_time_count')
    plt.legend()  # 显示图例
    plt.xlabel('date')
    plt.ylabel('count')
    plt.xticks(list_date, rotation=45, fontsize=10)
    if str_type == 'free_time_quantum':
        x_major_locator = MultipleLocator(5)
        # 把x轴的刻度间隔设置为1，并存在变量里
        ax = plt.gca()
        # ax为两条坐标轴的实例
        ax.xaxis.set_major_locator(x_major_locator)
        # 把x轴的主刻度设置为1的倍数
    # plt.show()
    plt.savefig("C:/Users/13908/Desktop/" + str_type + ".png")

