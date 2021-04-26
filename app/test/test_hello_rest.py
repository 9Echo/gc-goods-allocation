# -*- coding: utf-8 -*-
# Description: rest服务调用测试
# Created: shaoluyu 2019/10/29
# Modified: shaoluyu 2019/10/29; shaoluyu 2019/07/18

import requests

# rest api的url
url = 'http://localhost:9200/hello'

params = {
    'main': 'abc',
    'pwd': '4567'
}

headers = {
    'Content-Type': 'application/json;charset=UTF-8'
}

if __name__ == "__main__":
    # get请求rest服务
    res = requests.get(url, params={'username': '张三'}, timeout=2)
    print('get response type: {}'.format(type(res)))
    print('get response json object type: {}'.format(type(res.json())))
    print('get response json object: {}'.format(res.json()))
    print('')

    res = requests.get(url, timeout=2)
    print('get response json object: {}'.format(res.json()))
    print('')

    # post请求rest服务
    res = requests.post(url, json=params, headers=headers, timeout=2)
    print('post response type: {}'.format(type(res)))
    print('posr response json object type: {}'.format(type(res.json())))
    print('posr response json object: {}'.format(res.json()))
