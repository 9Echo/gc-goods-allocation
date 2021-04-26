import pandas as pd
import datetime


def is_vaild_date(date_str):
    try:
        if ":" in date_str:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        else:
            date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            print(date_str)
        return date
    except:
        print(date_str + " -1")
        return False


def driver_flowname_timelist_format(data_df):
    driver_flowname_timelist = {}
    unvaild_sum = 0
    for index, row in data_df.iterrows():
        driver_id = row["driver_id"]
        flow_name = row["end_district_name"]
        load_date = is_vaild_date(row["load_date"])
        if load_date is False:
            print(index)
            unvaild_sum += 1
            continue
        if driver_flowname_timelist.__contains__(driver_id):
            if driver_flowname_timelist[driver_id].__contains__(flow_name):
                driver_flowname_timelist[driver_id][flow_name].append(load_date)
            else:
                driver_flowname_timelist[driver_id][flow_name] = [load_date]
        else:
            driver_flowname_timelist[driver_id] = {}
            driver_flowname_timelist[driver_id][flow_name] = [load_date]
    print(unvaild_sum)
    # 按时间排序
    for driver_id in driver_flowname_timelist.keys():
        for flow_name in driver_flowname_timelist[driver_id].keys():
            driver_flowname_timelist[driver_id][flow_name].sort()

    return driver_flowname_timelist


def features_form(search_t, time_list):
    features = []
    last_3_days = 0
    last_7_days = 0
    last_15_days = 0
    last_30_days = 0
    total_times = 0
    next_3_days = 0
    next_7_days = 0
    next_10_days = 0
    next_15_days = 0
    for time in time_list:
        days_diff = (time - search_t).days  # 时间列表的时间-查询时间的天数差
        if days_diff < 0:  # 表示在查询时间之前的时间
            if days_diff >= -3:
                last_3_days += 1
                last_7_days += 1
                last_15_days += 1
                last_30_days += 1
                total_times += 1
            elif days_diff >= -7:
                last_7_days += 1
                last_15_days += 1
                last_30_days += 1
                total_times += 1
            elif days_diff >= -15:
                last_15_days += 1
                last_30_days += 1
                total_times += 1
            elif days_diff >= -30:
                last_30_days += 1
                total_times += 1
            else:
                total_times += 1
        else:  # 在查询时间之后
            if search_t == time:
                continue
            if days_diff <= 3:
                next_3_days += 1
                next_7_days += 1
                next_10_days += 1
                next_15_days += 1
            elif days_diff <= 7:
                next_7_days += 1
                next_10_days += 1
                next_15_days += 1
            elif days_diff <= 10:
                next_10_days += 1
                next_15_days += 1
            elif days_diff <= 15:
                next_15_days += 1
    features = [last_3_days, last_7_days, last_15_days, last_30_days,
                total_times, next_3_days, next_7_days, next_10_days, next_15_days]
    return features


def features_formation(data_df):
    data_prepared = []

    driver_flowname_timelist = driver_flowname_timelist_format(data_df)  # 得到6-9月各个司机每个流向的历史运输时间列表
    # i = 0
    # for driver_id in driver_flowname_timelist.keys():
    #     for flow_name in driver_flowname_timelist[driver_id].keys():
    #         if i>10:
    #             return 0
    #         print(driver_flowname_timelist[driver_id][flow_name])
    #         i += 1

    for index, row in data_df.iterrows():  # 对7-8月数据进行计算，生成特征
        load_date = is_vaild_date(row["load_date"])
        if load_date is False:
            continue
        mark_time = datetime.datetime.strptime("2020-07-01 00:00:00", "%Y-%m-%d %H:%M:%S")
        if 0 <= (load_date - mark_time).days <= 61:  # 选择7-8月的记录
            driver_id = row["driver_id"]
            flow_name = row["end_district_name"]
            time_list = driver_flowname_timelist[driver_id][flow_name]
            features = features_form(load_date, time_list)
            new_list = []
            new_list.extend(row)
            new_list.extend(features)
            data_prepared.append(new_list)
    return data_prepared


if __name__ == "__main__":
    filepath = "docs/pretreated_data.csv"
    data_df = pd.read_csv(filepath, low_memory=False)

    data_prepared = features_formation(data_df)

    col_list = ["main_product_list_no", "vehicle_no", "driver_id",
                "driver_name", "load_date", "end_district_name", "repeated",
                "last_3_days", "last_7_days", "last_15_days", "last_30_days",
                "total_times", "next_3_days", "next_7_days", "next_10_days", "next_15_days"]
    prepared_data_df = pd.DataFrame(data_prepared, columns=col_list)
    print(prepared_data_df.head(10))

    new_path = "docs/prepared_data.csv"
    prepared_data_df.to_csv(new_path)
