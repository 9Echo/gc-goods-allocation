# -*- coding: utf-8 -*-
# Description: 代码定义
# Created: shaoluyu 2019/06/27
# Modified: shaoluyu 2019/06/27; shaoluyu 2019/06/27


class ResponseCode(object):
    Fail = -1  # 失败，未知错误
    Success = 100  # 成功
    Error = 101  # 错误
    Warn = 102   # 警告
    Info = 200   # 提示
    NoResourceFound = 40001  # 未找到资源
    InvalidParameter = 40002  # 参数无效
    AccountOrPassWordErr = 40003  # 账户或密码错误
    VerificationCodeError = 40004  # 验证码错误
    PleaseSignIn = 40005  # 请登陆
    WeChatAuthorizationFailure = 40006  # 微信授权失败
    InvalidOrExpired = 40007  # 验证码过期
    MobileNumberError = 40008  # 手机号错误
    FrequentOperation = 40009  # 操作频繁,请稍后再试


class ResponseMessage(object):
    Fail = "失败"
    Success = "成功"
    Error = "错误"
    Warn = "库存剩余不足"
    NoResourceFound = "未找到资源"
    InvalidParameter = "参数无效"
    AccountOrPassWordErr = "账户或密码错误"
    VerificationCodeError = "验证码错误"
    PleaseSignIn = "请登陆"

