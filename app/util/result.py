import json


from flask import jsonify, Response

from app.util.base.base_entity import BaseEntity
from app.util.code import ResponseCode, ResponseMessage


class Result(BaseEntity):
    """将结果封装为定义好的json"""

    def __init__(self):
        self.code = ""
        self.msg = ""
        self.data = None

    @staticmethod
    def from_entity(obj):
        """封装成功返回的实体类"""

        if isinstance(obj, Result):
            return obj
        result = Result()
        result.code = ResponseCode.Success
        result.msg = "成功!"
        # 为BaseEntity提供json封装
        if isinstance(obj, list):
            data = [item.as_dict() for item in obj]
        elif isinstance(obj, BaseEntity):
            data = obj.as_dict()
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, list):
                    obj[k] = [item.as_dict() for item in v]
            data = obj
        else:
            data = obj
        result.data = data
        return result

    @staticmethod
    def error(msg=None):
        result = Result()
        result.code = ResponseCode.Error
        result.msg = ResponseMessage.Error if not msg else msg
        return result

    @staticmethod
    def warn(msg):
        result = Result()
        result.code = ResponseCode.Warn
        result.msg = msg
        return result

    @staticmethod
    def info(msg="", data=None):
        result = Result()
        result.code = ResponseCode.Info
        result.msg = msg
        result.data = data
        return result

    @staticmethod
    def success(msg="", data=None):
        result = Result()
        result.code = ResponseCode.Success
        result.msg = ResponseMessage.Success if not msg else msg
        result.data = data
        return result

    @staticmethod
    def success_response(obj=None):
        """返回成功信息"""
        result = Result.from_entity(obj)
        return Response(json.dumps({"code": result.code, "msg": result.msg, "data": result.data}),
                        mimetype='application/json')
        # return {"code": result.code, "msg": result.msg, "data": result.data}

    @staticmethod
    def error_response(msg=None):
        """返回错误信息"""
        result = Result.error(msg)
        return jsonify({"code": result.code, "msg": result.msg})
