#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据库卡死问题快速修复工具
"""

import pymysql
import time

def fix_mysql_deadlock():
    """修复MySQL数据库卡死问题"""
    print("开始修复MySQL数据库卡死问题...")
    print("=" * 50)
    
    try:
        # 连接数据库
        conn = pymysql.connect(
            host='testapi.fuhu.tech',
            port=3306,
            user='ai_creator',
            password='ai_creator123456',
            db='esports',
            autocommit=True
        )
        print("数据库连接成功")
        
        cursor = conn.cursor()
        
        # 1. 检查当前进程
        print("\n1. 检查当前进程...")
        cursor.execute("SHOW PROCESSLIST")
        processes = cursor.fetchall()
        print(f"总连接数: {len(processes)}")
        
        # 2. 检查被锁定的表
        print("\n2. 检查被锁定的表...")
        cursor.execute("SHOW OPEN TABLES WHERE In_use > 0")
        locked_tables = cursor.fetchall()
        if locked_tables:
            print("被锁定的表:")
            for table in locked_tables:
                print(f"  - 数据库: {table[0]}, 表: {table[1]}, 使用中: {table[2]}")
        else:
            print("没有表被锁定")
        
        # 3. 终止长时间运行的查询
        print("\n3. 终止长时间运行的查询...")
        killed_count = 0
        for process in processes:
            process_id, user, host, db, command, time_seconds, state, info = process
            
            # 跳过当前连接和系统进程
            if process_id == conn.thread_id() or user == 'system user':
                continue
            
            # 终止长时间运行的查询
            if time_seconds > 60 and command != 'Sleep':
                try:
                    cursor.execute(f"KILL {process_id}")
                    print(f"终止进程 {process_id} (运行时间: {time_seconds}秒, 命令: {command})")
                    killed_count += 1
                except Exception as e:
                    print(f"终止进程 {process_id} 失败: {e}")
        
        print(f"已终止 {killed_count} 个长时间运行的查询")
        
        # 4. 清理长时间空闲的连接
        print("\n4. 清理长时间空闲的连接...")
        sleep_connections = [p for p in processes if p[4] == 'Sleep' and p[5] > 300]
        if sleep_connections:
            print(f"清理 {len(sleep_connections)} 个长时间空闲连接...")
            for conn_info in sleep_connections:
                try:
                    cursor.execute(f"KILL {conn_info[0]}")
                    print(f"清理连接 {conn_info[0]}")
                except Exception as e:
                    print(f"清理连接 {conn_info[0]} 失败: {e}")
        else:
            print("没有需要清理的空闲连接")
        
        # 5. 检查修复后的状态
        print("\n5. 检查修复后的状态...")
        cursor.execute("SHOW PROCESSLIST")
        processes_after = cursor.fetchall()
        print(f"修复后总连接数: {len(processes_after)}")
        
        cursor.execute("SHOW OPEN TABLES WHERE In_use > 0")
        locked_tables_after = cursor.fetchall()
        if locked_tables_after:
            print("仍有被锁定的表:")
            for table in locked_tables_after:
                print(f"  - 数据库: {table[0]}, 表: {table[1]}, 使用中: {table[2]}")
        else:
            print("所有表都已解锁")
        
        cursor.close()
        conn.close()
        
        print("\n" + "=" * 50)
        print("修复完成！")
        
        return True
        
    except Exception as e:
        print(f"修复过程中出现错误: {e}")
        return False

if __name__ == "__main__":
    fix_mysql_deadlock()
