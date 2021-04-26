# 启动脚本最后一个参数是附加的，用于在进程中标志应用名称，gunicorn不使用该参数
python3 /usr/local/bin/gunicorn -c gunicorn_config.py manage:app models-gc-goods-allocation