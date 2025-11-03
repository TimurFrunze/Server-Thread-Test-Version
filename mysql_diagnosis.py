#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据库卡死问题诊断和修复工具
"""

import pymysql
import time
import sys
from typing import List, Tuple

class MySQLDiagnosis:
    def __init__(self, host: str, port: int, user: str, password: str, db: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.conn = None
    
    def connect(self):
        """建立数据库连接"""
        try:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db,
                autocommit=True
            )
            print("数据库连接成功")
            return True
        except Exception as e:
            print(f"数据库连接失败: {e}")
            return False
    
    def check_processes(self) -> List[Tuple]:
        """检查当前进程列表"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SHOW PROCESSLIST")
            processes = cursor.fetchall()
            cursor.close()
            return processes
        except Exception as e:
            print(f"❌ 检查进程列表失败: {e}")
            return []
    
    def check_locked_tables(self) -> List[Tuple]:
        """检查被锁定的表"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SHOW OPEN TABLES WHERE In_use > 0")
            locked_tables = cursor.fetchall()
            cursor.close()
            return locked_tables
        except Exception as e:
            print(f"❌ 检查锁定表失败: {e}")
            return []
    
    def kill_long_running_queries(self, timeout_seconds: int = 60):
        """终止长时间运行的查询"""
        processes = self.check_processes()
        killed_count = 0
        
        for process in processes:
            process_id, user, host, db, command, time_seconds, state, info = process
            
            # 跳过当前连接和系统进程
            if process_id == self.conn.thread_id() or user == 'system user':
                continue
            
            # 终止长时间运行的查询
            if time_seconds > timeout_seconds and command != 'Sleep':
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(f"KILL {process_id}")
                    cursor.close()
                    print(f"🔪 终止进程 {process_id} (运行时间: {time_seconds}秒, 命令: {command})")
                    killed_count += 1
                except Exception as e:
                    print(f"❌ 终止进程 {process_id} 失败: {e}")
        
        return killed_count
    
    def check_table_locks(self):
        """检查表锁情况"""
        locked_tables = self.check_locked_tables()
        if locked_tables:
            print("🔒 被锁定的表:")
            for table in locked_tables:
                print(f"  - 数据库: {table[0]}, 表: {table[1]}, 使用中: {table[2]}, 名称锁定: {table[3]}")
        else:
            print("✅ 没有表被锁定")
    
    def check_connection_count(self):
        """检查连接数"""
        processes = self.check_processes()
        total_connections = len(processes)
        sleep_connections = len([p for p in processes if p[4] == 'Sleep'])
        active_connections = total_connections - sleep_connections
        
        print(f"📊 连接统计:")
        print(f"  - 总连接数: {total_connections}")
        print(f"  - 活跃连接: {active_connections}")
        print(f"  - 空闲连接: {sleep_connections}")
        
        if total_connections > 50:
            print("⚠️  连接数过多，建议优化连接池配置")
    
    def check_long_running_queries(self):
        """检查长时间运行的查询"""
        processes = self.check_processes()
        long_queries = []
        
        for process in processes:
            process_id, user, host, db, command, time_seconds, state, info = process
            if time_seconds > 30 and command != 'Sleep':
                long_queries.append(process)
        
        if long_queries:
            print("⏰ 长时间运行的查询:")
            for query in long_queries:
                print(f"  - ID: {query[0]}, 用户: {query[1]}, 时间: {query[5]}秒, 状态: {query[6]}")
                if query[7]:
                    print(f"    查询: {query[7][:100]}...")
        else:
            print("✅ 没有长时间运行的查询")
    
    def optimize_connections(self):
        """优化连接数"""
        processes = self.check_processes()
        sleep_connections = [p for p in processes if p[4] == 'Sleep' and p[5] > 300]  # 5分钟以上的空闲连接
        
        if sleep_connections:
            print(f"🧹 清理 {len(sleep_connections)} 个长时间空闲连接...")
            for conn in sleep_connections:
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(f"KILL {conn[0]}")
                    cursor.close()
                    print(f"  - 清理连接 {conn[0]}")
                except Exception as e:
                    print(f"  - 清理连接 {conn[0]} 失败: {e}")
    
    def run_diagnosis(self):
        """运行完整诊断"""
        print("开始MySQL数据库诊断...")
        print("=" * 50)
        
        if not self.connect():
            return False
        
        # 检查连接数
        self.check_connection_count()
        print()
        
        # 检查表锁
        self.check_table_locks()
        print()
        
        # 检查长时间运行的查询
        self.check_long_running_queries()
        print()
        
        # 优化连接
        self.optimize_connections()
        print()
        
        # 终止长时间运行的查询
        killed = self.kill_long_running_queries(60)
        if killed > 0:
            print(f"🔪 已终止 {killed} 个长时间运行的查询")
        
        print("=" * 50)
        print("✅ 诊断完成")
        
        if self.conn:
            self.conn.close()
        
        return True

def main():
    # 数据库连接配置
    HOST = 'testapi.fuhu.tech'
    PORT = 3306
    USER = 'ai_creator'
    PASSWORD = 'ai_creator123456'
    DB = 'esports'
    
    print("MySQL数据库卡死问题诊断工具")
    print("=" * 50)
    
    diagnosis = MySQLDiagnosis(HOST, PORT, USER, PASSWORD, DB)
    diagnosis.run_diagnosis()

if __name__ == "__main__":
    main()
