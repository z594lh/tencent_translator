"""Listing归属与人员绩效服务包，导入不启动线程或执行任务。

归属入口：ownership_service，SQL在ownership_repository；配置和共用验证在configuration/common。
人员查询只有三个模块：performance_service读取/汇总，breakdown_service负责SKU/每日排序分页，
order_counts补充真实已发货订单去重；没有生成事件、任务、旧订单利润重算或影子预览模块。
人员数据按当日历史负责人汇总原sku_profit，利润只求和，退款不再另扣。
店铺/SKU报表继续由原report_generator及既有cron生成，绩效不参与生成链路。
"""
