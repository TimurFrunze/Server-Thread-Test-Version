#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试回调函数是否正常工作的脚本
"""

from concurrent.futures import ThreadPoolExecutor, Future
import time
import threading

def test_task(task_id: int, delay: float = 1.0):
    """测试任务函数"""
    print(f"Task {task_id} started")
    time.sleep(delay)
    print(f"Task {task_id} completed")
    return task_id, None  # 返回 (id, error_message) 格式

class TestCallback:
    def __init__(self, task_id: int):
        self.task_id = task_id
        self.called = False
    
    def __call__(self, future: Future):
        print(f"Callback for task {self.task_id} is being called!")
        self.called = True
        try:
            result = future.result()
            print(f"Task {self.task_id} result: {result}")
        except Exception as e:
            print(f"Task {self.task_id} failed with exception: {e}")

def test_callback_execution():
    """测试回调函数执行"""
    print("开始测试回调函数执行...")
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        callbacks = []
        
        # 提交5个测试任务
        for i in range(5):
            future = executor.submit(test_task, i, delay=0.5)
            callback = TestCallback(i)
            
            future.add_done_callback(callback)
            
            futures.append(future)
            callbacks.append(callback)
        
        print("所有任务已提交到线程池")
        
        # 等待所有任务完成
        for i, future in enumerate(futures):
            try:
                result = future.result(timeout=10)
                print(f"Future {i} result: {result}")
            except Exception as e:
                print(f"Future {i} failed: {e}")
    
    # 检查回调是否被调用
    print("\n回调函数执行情况:")
    for i, callback in enumerate(callbacks):
        print(f"Task {i} callback called: {callback.called}")
    
    # 统计
    called_count = sum(1 for callback in callbacks if callback.called)
    print(f"\n总结: {called_count}/{len(callbacks)} 个回调函数被执行")

def test_callback_with_exception():
    """测试异常情况下的回调函数"""
    print("\n开始测试异常情况下的回调函数...")
    
    def failing_task(task_id: int):
        """会失败的任务"""
        print(f"Failing task {task_id} started")
        time.sleep(0.5)
        raise Exception(f"Task {task_id} intentionally failed")
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(failing_task, 999)
        callback = TestCallback(999)
        future.add_done_callback(callback)
        
        try:
            result = future.result(timeout=10)
        except Exception as e:
            print(f"Expected exception: {e}")
    
    print(f"异常任务的回调是否被调用: {callback.called}")

if __name__ == "__main__":
    test_callback_execution()
    test_callback_with_exception()
    print("\n测试完成！")
