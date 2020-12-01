# coding:utf8

from batch_spider.db import DB

# 建立连接
db_uri = "mysql://root:123456@localhost:3306/test"
db = DB().create(db_uri)

# 插入数据
data = {"name": "A", "sex": "man"}
# 要求 data中字段名和mysql字段名一样
db.add(data, table_name="test")

# 批量插入
data_list = [data, data]
# 要求列表中每个数据都有同样数据的
db.add_many(data_list, table_name="test")

# 更新
condition = {"name": "A"}
# 默认condition中各个字段是and关系 如果需要更复杂的自行调用执行sql
db.update(data, condition=condition)
# 稍微简单点的用法
db.update(data, where_sql="name='A'")

# 查询
result = db.query_all("show tables")
print(result)
