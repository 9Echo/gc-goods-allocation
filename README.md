# gc-goods-allocation

## 智能配载项目结构

- app 应用包
    - analysis 离线分析包
    - util 工具类包
    - main 业务程序包
        - routes 路由包
        - steel_factory 钢厂业务包
            - entity 业务实体包w
            - dao 数据库访问对象包
            - service 业务逻辑包
            - task 定时任务包
            - rule 钢厂分货规则
        - pipe_factory 管厂业务包
            - entity 业务实体包
            - dao 数据库访问对象包
            - service 业务逻辑包
            - task 定时任务包
            - rule 管厂分货规则
- config.py 项目配置信息
- model_config.py 模型配置信息
- manage.py 项目入口，启动文件
- test 测试包
    - static 测试数据包
- document 文档包
