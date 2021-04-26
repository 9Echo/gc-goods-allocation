# -*- coding: utf-8 -*-
# Description: RestTemplate
# Created: shaoluyu 2020/06/06
import json
import requests
from requests import RequestException
from flask import current_app
from app.util.code import ResponseCode
from app.util.my_exception import MyException


class RestTemplate(object):

    @staticmethod
    def do_post(url, data):
        """
        发送post请求
        :param url:请求地址
        :param data:字典或字典列表
        :return:
        """

        headers = {
            'Content-Type': 'application/json;charset=UTF-8'
        }
        # current_app.logger.info('request_body is：' + json.dumps(data, ensure_ascii=False))
        response = requests.post(url=url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            raise RequestException(str(response.json()))
        if response.json().get('code') != 100:
            raise MyException('目标服务错误:'+response.json().get('msg'), ResponseCode.Error)
        return response.json()

    @staticmethod
    def do_post_for_jcos(url, data):
        """
        发送post请求
        :param url:请求地址
        :param data:字典或字典列表
        :return:
        """

        headers = {
            'Content-Type': 'application/json;charset=UTF-8'
        }
        # current_app.logger.info('request_body is：' + json.dumps(data, ensure_ascii=False))
        response = requests.post(url=url, headers=headers, data=json.dumps(data))
        if response.status_code != 200:
            raise RequestException(str(response.json()))
        return response.json()
