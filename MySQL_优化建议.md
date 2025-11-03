# MySQL数据库卡死问题解决方案

## 问题分析

根据诊断结果，您的MySQL数据库卡死主要由以下原因造成：

### 1. 表级锁等待 (Metadata Lock)
- `movie_agent_tasks` 表被锁定
- 长时间运行的查询阻塞了其他操作
- 事务处理不当导致锁等待

### 2. 连接数过多
- 连接池配置了100个连接，实际只需要5-10个
- 大量空闲连接占用资源
- 连接超时设置不当

### 3. 事务处理问题
- 长时间运行的事务
- 缺乏适当的超时机制
- 锁等待时间过长

## 解决方案

### 1. 立即修复 ✅
已运行 `mysql_fix.py` 脚本：
- 终止了1个长时间运行的查询
- 清理了40个长时间空闲连接
- 解除了表锁

### 2. 代码优化建议

#### 2.1 连接池优化
```python
# 原配置
max_connections = 100  # 过多

# 优化后
max_connections = 5    # 根据实际需求调整
```

#### 2.2 添加连接超时
```python
conn = pymysql.connect(
    host=host, port=port, user=user, password=password, db=db,
    cursorclass=DictCursor, autocommit=False,
    connect_timeout=10,  # 连接超时
    read_timeout=30,     # 读取超时
    write_timeout=30     # 写入超时
)
```

#### 2.3 使用FOR UPDATE SKIP LOCKED
```sql
SELECT * FROM movie_agent_tasks 
WHERE state = 0 
ORDER BY id 
LIMIT %s 
FOR UPDATE SKIP LOCKED  -- 跳过被锁定的行
```

#### 2.4 设置锁等待超时
```sql
SET SESSION innodb_lock_wait_timeout = 10;
SET SESSION lock_wait_timeout = 10;
```

### 3. 监控和预防

#### 3.1 定期检查脚本
```python
# 运行 mysql_diagnosis.py 定期检查
python mysql_diagnosis.py
```

#### 3.2 监控指标
- 连接数：保持在10个以下
- 锁等待时间：不超过10秒
- 查询执行时间：不超过30秒

#### 3.3 日志监控
```python
# 在代码中添加详细日志
simple_log.log(f'数据库操作: {operation}, 耗时: {duration}秒', log_path=logging_path)
```

## 使用优化后的代码

### 1. 替换原有代码
将 `main_thread.py` 替换为 `optimized_main_thread.py`

### 2. 配置调整
```python
# 在配置文件中调整
{
    "max_connections": 5,      # 减少连接数
    "max_workers": 3,          # 减少工作线程
    "slice_size": 5,           # 减少批处理大小
    "max_retry_times": 3       # 减少重试次数
}
```

### 3. 监控脚本
定期运行诊断脚本：
```bash
# 每天运行一次
python mysql_fix.py
```

## 预防措施

### 1. 代码层面
- 使用连接池管理连接
- 设置合理的超时时间
- 避免长时间运行的事务
- 使用适当的锁机制

### 2. 数据库层面
- 定期清理长时间空闲连接
- 监控锁等待情况
- 优化查询性能
- 设置合理的连接限制

### 3. 运维层面
- 定期监控数据库状态
- 设置告警机制
- 定期备份数据
- 监控系统资源使用情况

## 总结

通过以上优化措施，可以有效解决MySQL数据库卡死问题：

1. ✅ **立即修复**：已清理连接和锁
2. 🔧 **代码优化**：使用优化后的代码
3. 📊 **监控预防**：定期检查和监控
4. ⚙️ **配置调整**：合理设置连接数和超时

建议立即使用 `optimized_main_thread.py` 替换原有代码，并定期运行诊断脚本进行监控。

