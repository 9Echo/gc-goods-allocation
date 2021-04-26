# -*- coding: utf-8 -*-
# Description: 应用配置文件
# Created: shaoluyu 2019/10/29
# Modified: shaoluyu 2019/10/29

import datetime
import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    """默认配置
    """
    # 应用参数
    APP_NAME = 'models-gc-goods-allocation'
    SERVER_PORT = 9238
    #
    FLATPAGES_AUTO_RELOAD = True
    FLATPAGES_EXTENSION = '.md'
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'can you guess it'
    DEBUG = True

    # sqlalchemy两个主要配置
    # 关闭数据库时是否自动提交事务
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True
    # 是否追踪修改
    SQLALCHEMY_TRACK_MODIFICATIONS = True

    # 是否开启任务调度器,默认不开启
    SCHEDULER_OPEN = False
    SCHEDULER_API_ENABLED = False
    # 任务调度器lock文件名称
    SCHEDULER_LOCK_FILE_NAME = 'scheduler-{}.lock'.format(APP_NAME)

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    """开发环境配置
    """

    # 数仓连接
    # ODS_MYSQL_HOST = 'am-bp16yam2m9jqm2tyk90650.ads.aliyuncs.com'
    # ODS_MYSQL_PORT = 3306
    # ODS_MYSQL_USER = 'modeluser'
    # ODS_MYSQL_PASSWD = 'eSod!0615'
    # ODS_MYSQL_DB = 'db_model'
    # ODS_MYSQL_CHARSET = 'utf8'
    # # 数仓生产
    # ODS_MYSQL_HOST_2 = 'am-bp117g8ua37t2f4vh90650.ads.aliyuncs.com'
    # ODS_MYSQL_PORT_2 = 3306
    # ODS_MYSQL_USER_2 = 'apiuser'
    # ODS_MYSQL_PASSWD_2 = 'reUa!0610'
    # ODS_MYSQL_DB_2 = 'db_model'
    # ODS_MYSQL_CHARSET_2 = 'utf8'


    # 本地数据库连接
    ODS_MYSQL_HOST = 'localhost'
    ODS_MYSQL_PORT = 3306
    ODS_MYSQL_USER = 'root'
    ODS_MYSQL_PASSWD = '9527'
    ODS_MYSQL_DB = 'db_model'
    ODS_MYSQL_CHARSET = 'utf8'

    # sales数据库连接
    # ODS_MYSQL_HOST = '192.168.1.50'
    # ODS_MYSQL_PORT = 8484
    # ODS_MYSQL_USER = 'data_group'
    # ODS_MYSQL_PASSWD = 'safdsfewf'
    # ODS_MYSQL_DB = 'db_model'
    # ODS_MYSQL_CHARSET = 'utf8'

    # Redis配置，可选（不使用时可删除）
    REDIS_HOST = '192.168.1.22'
    REDIS_PORT = '6379'
    REDIS_PASSWD = 'xiexieni56'
    REDIS_MAX_CONNECTIONS = 5
    # EUREKA
    EUREKA_SERVER = 'http://127.0.0.1:8761/eureka/'
    # 微服务url
    DISPATCH_SERVICE_URL = 'http://192.168.1.70:9078'
    # 招投标url
    TENDER_SERVICE_URL = 'https://uat.jczh56.com/api/tender'
    # 数仓url
    DATA_WAREHOUSE_URL = 'https://uat.jczh56.com/api/manage'

    # APScheduler定时任务配置，可选（不使用时可删除）
    SCHEDULER_OPEN = False
    SCHEDULER_API_ENABLED = True

    # Celery配置，可选（不使用时可删除）
    # REDIS_URL = 'redis://:xiexieni56@192.168.1.22:6379/0'
    BROKER_URL = 'amqp://admin:jczh@56Haishang@192.168.1.77:5672/model_test'
    default_timeout = 60 * 60
    CELERY_BROKER_URL = BROKER_URL
    # CELERY_RESULT_BACKEND = BROKER_URL
    # 导入任务所在的模块
    CELERY_IMPORTS = ('app.task.celery_task')
    CELERY_DEFAULT_QUEUE = "gc_goods_allocation"
    # 设置定时任务
    from datetime import timedelta
    from celery.schedules import crontab
    CELERY_TIMEZONE = 'Asia/Shanghai'  # 指定时区，不指定默认为 'UTC'
    CELERYBEAT_SCHEDULE = {
        'push_message': {
            'task': 'app.task.celery_task.push_message',
            'schedule': crontab(),
            'args': None
        }

    }


class TestConfig(Config):
    """测试环境配置
    """
    # 数仓连接
    ODS_MYSQL_HOST = 'am-bp16yam2m9jqm2tyk90650.ads.aliyuncs.com'
    ODS_MYSQL_PORT = 3306
    ODS_MYSQL_USER = 'modeluser'
    ODS_MYSQL_PASSWD = 'eSod!0615'
    ODS_MYSQL_DB = 'db_model'
    ODS_MYSQL_CHARSET = 'utf8'

    # Redis配置，可选（不使用时可删除）
    REDIS_HOST = '172.16.110.156'
    REDIS_PORT = '6379'
    REDIS_PASSWD = 'JCdev@56zh'
    REDIS_MAX_CONNECTIONS = 2
    # 微服务url
    DISPATCH_SERVICE_URL = 'http://192.168.1.70:9078'
    # 招投标url
    # TENDER_SERVICE_URL = 'http://192.168.1.20:9108'
    # # 数仓url
    # DATA_WAREHOUSE_URL = 'http://192.168.1.26:9016'
    # tenderUrl
    TENDER_SERVICE_URL = 'https://www.uat.jczh56.com/api/tender'
    # 数仓url
    DATA_WAREHOUSE_URL = 'https://uat.jczh56.com/api/manage'

    # APScheduler定时任务配置，可选（不使用时可删除）
    SCHEDULER_OPEN = False
    SCHEDULER_API_ENABLED = True

    # Celery配置，可选（不使用时可删除）
    REDIS_URL = 'redis://:xiexieni56@192.168.1.22:6379/0'
    default_timeout = 60 * 60
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    # 导入任务所在的模块
    CELERY_IMPORTS = ('app.task.celery_task')
    CELERY_DEFAULT_QUEUE = "gc_goods_allocation"
    # 设置定时任务
    from datetime import timedelta
    from celery.schedules import crontab
    CELERY_TIMEZONE = 'Asia/Shanghai'  # 指定时区，不指定默认为 'UTC'
    CELERYBEAT_SCHEDULE = {
        'save_hour_stock_': {
            'task': 'app.task.celery_task.save_hour_stock_',
            'schedule': crontab(minute=0, hour='*/1'),
            'args': None
        },
        'push_message': {
            'task': 'app.task.celery_task.push_message',
            'schedule': crontab(minute='*/5'),
            'args': None
        },
        'check_driver_behavior': {
            'task': 'app.task.celery_task.check_driver_behavior',
            'schedule': crontab(hour=0, minute=30),
            'args': None
        }
    }


class UatConfig(Config):
    """测试环境配置
    """
    # 数仓连接
    ODS_MYSQL_HOST = 'am-bp16yam2m9jqm2tyk90650.ads.aliyuncs.com'
    ODS_MYSQL_PORT = 3306
    ODS_MYSQL_USER = 'modeluser'
    ODS_MYSQL_PASSWD = 'eSod!0615'
    ODS_MYSQL_DB = 'db_model'
    ODS_MYSQL_CHARSET = 'utf8'

    # Redis配置，可选（不使用时可删除）
    REDIS_HOST = '172.16.110.156'
    REDIS_PORT = '6379'
    REDIS_PASSWD = 'JCdev@56zh'
    REDIS_MAX_CONNECTIONS = 2

    # 微服务url
    DISPATCH_SERVICE_URL = 'https://uat.jczh56.com/api/dispatch-center'
    # Celery配置，可选（不使用时可删除）
    # CELERY_BROKER_URL = 'redis://:wobugaoxing@47.99.118.183:6379/0'
    # CELERY_RESULT_BACKEND = 'redis://:wobugaoxing@47.99.118.183:6379/0'
    # # 导入任务所在的模块
    # CELERY_IMPORTS = ('app.task.celery_task', 'app.task.celery_task2')
    # # 设置定时任务
    # from datetime import timedelta
    # from celery.schedules import crontab
    # CELERY_TIMEZONE = 'Asia/Shanghai'  # 指定时区，不指定默认为 'UTC'
    # CELERYBEAT_SCHEDULE = {
    #     'add-every-30-seconds': {
    #         'task': 'app.task.celery_task.add_together',
    #         'schedule': timedelta(seconds=30),  # 每30秒执行一次
    #         'args': (5, 8)  # 任务函数参数
    #     },
    #     'print-at-some-time': {
    #         'task': 'app.task.celery_task.print_hello',
    #         'schedule': crontab(minute='0-59/2'),
    #         'args': None
    #     },
    #     'multiply-every-60-seconds': {
    #         'task': 'app.task.celery_task2.multiply_together',
    #         'schedule': timedelta(seconds=60),  # 每30秒执行一次
    #         'args': (3, 4)  # 任务函数参数
    #     },
    # }


class ProductionConfig(Config):
    """生产环境配置
    """
    # Mysql配置，可选（不使用时可删除）
    MYSQL_HOST = '47.99.118.183'
    MYSQL_PORT = 3306
    MYSQL_USER = 'v3dev_user'
    MYSQL_PASSWD = 'V3dev!56'
    MYSQL_DB = 'db_trans_plan'
    MYSQL_CHARSET = 'utf8'

    # sqlalchemy ORM底层所访问数据库URI，可选（不使用时可删除）
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://{}:{}@{}:{}/{}?charset={}'.format(
        MYSQL_USER, MYSQL_PASSWD, MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_CHARSET)

    # Redis配置，可选（不使用时可删除）
    REDIS_HOST = '47.99.118.183'
    REDIS_PORT = '6379'
    REDIS_PASSWD = 'wobugaoxing'
    REDIS_MAX_CONNECTIONS = 2

    # APScheduler定时任务配置，可选（不使用时可删除）
    SCHEDULER_OPEN = False
    SCHEDULER_API_ENABLED = True

    # tenderUrl
    TENDER_SERVICE_URL = 'https://www.jczh56.com/api/tender'
    # 数仓url
    DATA_WAREHOUSE_URL = 'https://jczh56.com/api/manage'
    # Celery配置，可选（不使用时可删除）
    REDIS_URL = 'redis://:xiexieni56@192.168.1.22:6379/0'
    default_timeout = 60 * 60
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    # 导入任务所在的模块
    CELERY_IMPORTS = ('app.task.celery_task')
    CELERY_DEFAULT_QUEUE = "gc_goods_allocation"
    # 设置定时任务
    from datetime import timedelta
    from celery.schedules import crontab
    CELERY_TIMEZONE = 'Asia/Shanghai'  # 指定时区，不指定默认为 'UTC'
    CELERYBEAT_SCHEDULE = {
        'push_message': {
            'task': 'app.task.celery_task.push_message',
            'schedule': crontab(hour=13, minute=30),
            'args': None
        }
    }


# 设置环境配置映射
config = {
    'development': DevelopmentConfig,
    'test': TestConfig,
    'uat': UatConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}


def get_active_config():
    """获取当前生效的环境配置类

    :return: 当前生效的环境配置类
    """
    config_name = os.getenv('FLASK_CONFIG') or 'default'
    return config[config_name]


def get_active_config_name():
    """获取当前生效的环境配置名称

    :return: 当前生效的环境配置名称
    """
    config_name = os.getenv('FLASK_CONFIG') or 'default'
    return config_name
