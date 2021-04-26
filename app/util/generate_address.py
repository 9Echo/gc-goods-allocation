import requests
import json
from app.util.get_static_path import get_path
import pandas as pd


def read_address():
    """

    Args:

    Returns:

    Raise:

    """
    dic = dict()
    data_path = get_path("不存在的地址.xlsx")
    df = pd.read_excel(data_path)
    df.rename(index=str,
              columns={"不存在的地址": "address"},
              inplace=True)
    df = df.drop(columns=["Unnamed: 0"])
    address_list = df.to_dict(orient="record")
    dic["tPointModels"] = address_list
    dic["requestCompanyId"] = "C000000882"
    return dic


def get_point_list(input_dict):
    """
    调用java接口   获取经纬度信息
    """
    try:
        # 批量新增地址接口  返回地址信息
        point_url = 'https://uat.jczh56.com/api/system/point/addAddresList'
        # 设置请求头
        headers = {
            "Content-Type": "application/json;charset-UTF-8"
        }

        res = requests.post(point_url, data=json.dumps(input_dict), headers=headers, timeout=5)
        if 'data' not in res.json():
            return None
        result = res.json()['data']
        print(res.json())
        return result
    except Exception as e:
        print(e)
        return None


if __name__ == "__main__":
    address = read_address()
    # print(address)
    print(get_point_list(address))
    # get_point_list(address)