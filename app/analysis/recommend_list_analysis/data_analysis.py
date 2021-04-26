import pandas as pd
import datetime
from model2.features_format import is_vaild_date


def driver_flowname_form(trans_df):
    driver_flowname = {}
    for index, row in trans_df.iterrows():
        driver_id = row["driver_id"]
        flow_name = row["end_district_name"]
        load_date = is_vaild_date(row["load_date"])
        if load_date is False:
            continue
        new_list = [driver_id, flow_name, load_date]
        if driver_flowname.__contains__(driver_id):
            driver_flowname[driver_id].append(new_list)
        else:
            driver_flowname[driver_id] = []
            driver_flowname[driver_id].append(new_list)

    for driver_id in driver_flowname.keys():
        driver_flowname[driver_id].sort(key=lambda x: x[2])
    i = 0
    for driver_id in driver_flowname.keys():
        if i > 5:
            break
        print(driver_flowname[driver_id])

    return driver_flowname


# if __name__=="__main__":
#     filepath = "docs/prepared_data.csv"
#     trans_df = pd.read_csv(filepath,low_memory=False)
#     driver_flowname = driver_flowname_form(trans_df)
#     data = []
#     for driver_id in driver_flowname.keys():
#         new_list = []
#         for trans in driver_flowname[driver_id]:
#             new_list.append(trans[1])
#         data.append(new_list)
#     out_df = pd.DataFrame(data)
#     out_path = "docs/driver_flownames3.csv"
#     out_df.to_csv(out_path,index=None)

if __name__=="__main__":
    inpath = "docs/driver_flownames3.csv"
    data_df = pd.read_csv(inpath, low_memory=False)
    new_list = []
    for index, row in data_df.iterrows():
        for i in row.keys():
            if row[i] == "黄岛区":
                new_list.append(row)
                break
            # print(row[i])

    outpath = "docs/contain_huangdao.csv"
    out_df = pd.DataFrame(new_list)
    out_df.to_csv(outpath)

