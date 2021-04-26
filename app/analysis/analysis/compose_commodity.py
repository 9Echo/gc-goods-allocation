from app.main.dao.commodity_dao import get_commodity, write_database, update_database_size, update_database_time, pd_read_table
import time
import traceback
import pandas as pd


def get_commodity_collocation():
    """品种搭配规则

    Args:

    Returns:
        data2: 品种搭配的字典  形如：{'焊管 1,热镀': 62,...}

    Raise:

    """
    try:
        # 读取的表名
        table = "t_compose_commodity"
        # 读取原品名搭配表
        base_data = pd_read_table(table)
        # 获得上次更新的时间
        update_time = set(base_data["update_time"].values)
        # 判断是不是第一次更新
        if not update_time:
            # 得到从发货单、运单、结算单个别信息   形如：（发货通知单号，发货通知单创建时间，车牌，品种，发运单创建时间，结算单创建时间）
            results, now_time = get_commodity()
        else:
            results, now_time = get_commodity(start_date=str(update_time.pop()))
        # 将results整理成字典   形如：{车牌：[[发货单，品种，发货单创建时间，运单创建时间，结算单创建时间],...],...}其中时间为时间戳
        data = {}
        # 拼命搭配字典    形如： {'主品名,搭配品名':次数,...}
        data2 = {}
        if results:
            # 循环 results
            for res in results:
                # 如果车牌不在data的键中，则创建
                if res[2] not in data.keys():
                    data[res[2]] = []
                # 给该车牌 添加数据
                data[res[2]].append([res[0],
                                     res[3],
                                     int(time.mktime(res[1].timetuple())),
                                     int(time.mktime(res[4].timetuple())),
                                     int(time.mktime(res[5].timetuple()))])
            # 循环 data
            for i in data.keys():
                count = 0
                # 比较同一个车牌下的结算单创建时间
                for j in data[i]:
                    count += 1
                    for q in range(len(data[i])-count):
                        p = q + count
                        # 将运单创建时间 相近的且发货单不同的 视为拼货单
                        if abs(j[3] - data[i][p][3]) < 600 and j[0] != data[i][p][0] and j[1] != data[i][p][1]:
                            # 合并品种搭配数据
                            if j[1] > data[i][p][1]:
                                temp = j[1]
                                j[1] = data[i][p][1]
                                data[i][p][1] = temp
                            # 将拼货的品种 用逗号连接 并记录次数
                            name = j[1] + ',' + data[i][p][1]
                            data2[name] = data2.get(name, 0) + 1
            # 排序 按data2的keys排序
            data2 = sorted(data2.items(), key=lambda x: x[0])
            # 写库
            for i in data2:
                # 将date2 的数据整理成data3形式   形如：[主品名，搭配品名，搭配次数，更新时间]
                list1 = i[0].split(',')
                data3 = ([list1[0], list1[1], i[1], now_time])
                # 在t_compose_commodity表中找 主品名、搭配品名相同的列
                df = base_data.loc[(base_data["main_commodity"] == list1[0]) & (base_data["match_commodity"] == list1[1])]
                # 如果df不为空则累加
                if len(df) != 0:
                    data3[2] += int(df["match_size"])
                    update_database_size(data3)
                # 如果df为空就添加信息
                else:
                    write_database(data3)
            # 更新时间
            update_database_time(now_time)
        # 如果新库存为空 则更新时间
        else:
            update_database_time(now_time)
        return data2
    except Exception as e:
        print("compose_commodity.get_commodity_collocation is error")
        traceback.print_exc()


if __name__ == "__main__":
    get_commodity_collocation()