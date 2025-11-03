#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试并发插入和查询的脚本
"""

import pymysql
from pymysql.cursors import DictCursor
import time
import threading
import random
import string
from datetime import datetime

def generate_random_string(length: int = 20):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_date() -> str:
    return datetime.now().strftime('%Y-%m-%d')

def insert_test_data(connection_params, num_records=10, delay=1.0):
    """在另一个线程中插入测试数据"""
    print(f"开始插入 {num_records} 条测试数据...")
    
    conn = pymysql.connect(
        host=connection_params['host'],
        port=connection_params['port'],
        user=connection_params['user'],
        password=connection_params['password'],
        database=connection_params['db'],
        cursorclass=DictCursor
    )
    
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * 11)
            for i in range(num_records):
                time.sleep(delay)  # 模拟插入间隔
                
                cursor.execute(f'INSERT INTO movie_agent_tasks VALUES ({placeholders})', (
                    None,
                    generate_random_date(),
                    generate_random_date(),
                    generate_random_date(),
                    generate_random_string(100),
                    generate_random_string(20),
                    1024,
                    1024,
                    0,  # state = 0
                    0,
                    generate_random_string(20),
                ))
                
                print(f"插入第 {i+1} 条记录")
                
                # 每插入几条就提交一次
                if (i + 1) % 3 == 0:
                    conn.commit()
                    print(f"提交了 {i+1} 条记录")
            
            # 提交剩余记录
            conn.commit()
            print(f"所有 {num_records} 条记录插入完成")
            
    except Exception as e:
        print(f"插入数据时出错: {e}")
        conn.rollback()
    finally:
        conn.close()

def query_state0_records(connection_params, query_interval=2.0, max_queries=10):
    """查询state=0的记录"""
    print("开始查询state=0的记录...")
    
    conn = pymysql.connect(
        host=connection_params['host'],
        port=connection_params['port'],
        user=connection_params['user'],
        password=connection_params['password'],
        database=connection_params['db'],
        cursorclass=DictCursor
    )
    
    try:
        with conn.cursor() as cursor:
            # 设置事务隔离级别
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            
            for i in range(max_queries):
                time.sleep(query_interval)
                
                # 查询state=0的记录数量
                cursor.execute("SELECT COUNT(*) as count FROM movie_agent_tasks WHERE state = 0")
                result = cursor.fetchone()
                count = result['count']
                
                print(f"查询 {i+1}: 找到 {count} 条state=0的记录")
                
                # 如果找到记录，显示前几条
                if count > 0:
                    cursor.execute("SELECT id, task_uuid FROM movie_agent_tasks WHERE state = 0 LIMIT 5")
                    records = cursor.fetchall()
                    for record in records:
                        print(f"  - ID: {record['id']}, UUID: {record['task_uuid']}")
                        
    except Exception as e:
        print(f"查询数据时出错: {e}")
    finally:
        conn.close()

def test_concurrent_operations():
    """测试并发操作"""
    connection_params = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': 'tkp040629',
        'db': 'esports'
    }
    
    print("开始并发测试...")
    print("=" * 50)
    
    # 创建两个线程：一个插入数据，一个查询数据
    insert_thread = threading.Thread(
        target=insert_test_data,
        args=(connection_params, 20, 0.5)  # 插入20条记录，每0.5秒一条
    )
    
    query_thread = threading.Thread(
        target=query_state0_records,
        args=(connection_params, 1.0, 15)  # 查询15次，每1秒一次
    )
    
    # 启动线程
    insert_thread.start()
    time.sleep(1)  # 让插入线程先开始
    query_thread.start()
    
    # 等待线程完成
    insert_thread.join()
    query_thread.join()
    
    print("=" * 50)
    print("并发测试完成")

if __name__ == "__main__":
    test_concurrent_operations()
