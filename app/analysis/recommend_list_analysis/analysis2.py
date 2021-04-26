import pandas as pd
import pymysql as pym


def flows_count():
    flows_count = {}

    inpath = "docs/pretreated_data.csv"
    inpath2 = "docs/流向关联_utf8.csv"
    waybill_df = pd.read_csv(inpath)
    flows_df = pd.read_csv(inpath2)
    for index, row in waybill_df.iterrows():
        flows = row["end_district_name"]
        if flows_count.__contains__(flows):
            flows_count[flows] += 1
        else:
            flows_count[flows] = 1
    flows_count_df = pd.DataFrame([flows_count]).T
    print(flows_count_df)

    flows_df['count'] = flows_df['Id'].apply(lambda x: flows_count[x])

    print(flows_df.head(10))

    outpath = "docs/flows_relation_count.csv"
    flows_df.to_csv(outpath, index=None)


def flows_group_count():
    inpath = "docs/flows_group.csv"
    inpath2 = "docs/pretreated_data.csv"
    inpath3 = "docs/流向关联_utf8.csv"
    flows_df = pd.read_csv(inpath)
    waybill_df = pd.read_csv(inpath2)
    base_df = pd.read_csv(inpath3)

    flows_count = {}
    for index, row in waybill_df.iterrows():
        flows = row["end_district_name"]
        if flows_count.__contains__(flows):
            flows_count[flows] += 1
        else:
            flows_count[flows] = 1
    flows_df['count'] = flows_df['flows'].apply(lambda x: flows_count[x])
    flows_df['pagerank'] = flows_df['']

    outpath = "docs/group_flows_count.csv"
    flows_df.to_csv(outpath, index=None)


if __name__ == "__main__":
    # flows_count()

    flows_group_count()
