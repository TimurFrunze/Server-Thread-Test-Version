from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable
import pymysql
from pymysql.cursors import DictCursor
from DBpool import DBpool, DBpoolRenew
import DBpool
from queue import Queue
import simple_log
import time
import read_config

# 在使用游标进行操作时, 添加重试机制, 自定义重试次数(3~5次)
# 弹出特定异常, 即触发重试, 其他异常不触发重试

def retry(conn:pymysql.Connection,max_retry_times:int=5,logging_path:str='./log.txt'):
  status = False
  for i in range(max_retry_times):
    try:
      conn.ping(reconnect=True)
    except Exception as e:
      print(f'Ping exception {e}! Retry failed, retrying times: {i}\nIn main_thread.retry')
      simple_log.log(f'Ping exception {e}! Retry failed, retrying times: {i}\nIn main_thread.retry',log_path=logging_path)
      time.sleep(1)
      continue
    else:
        status = True
        break
  return status

def retry_execute(cursor:DictCursor,logging_path:str,sql:str,args:tuple=None,max_retry_times:int=5):
  status = False
  for i in range(max_retry_times):
    try:
      cursor.execute(sql,args)
    except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
      if e.args[0] in (0, 2003, 2006, 2013):
        retry(cursor.connection,max_retry_times,logging_path)
        continue
      else:
        raise e
    except Exception as e:
        print(f'Execute exception {e}! Retry failed, retrying times: {i}\nIn main_thread.retry_execute')
        simple_log.log(f'Execute exception {e}! Retry failed, retrying times: {i}\nIn main_thread.retry_execute',log_path=logging_path)
        raise e
    else:
        status = True
        break
  return status

class main_thread:
  # 连接测试成功
  def __init__(self,func:Callable,host:str='testapi.fuhu.tech',port:int=3306,user:str='ai_creator',password:str='ai_creator123456',db:str='vimaxai',max_connections:int=10, logging_path:str='./logging_dir', max_retry_times:int=5,generate_retry_times:int=3,heart_beat_interval:int=100,dbpool_get:Callable=DBpool.get_DBpool,time_overflow_seconds:int=1800):
    '''
    func: 接收字典和线程池引用作为参数, 返回tuple[int,None|str]
    host: 数据库主机
    port: 数据库端口
    user: 数据库用户
    password: 数据库密码
    db: 数据库名称
    max_connections: 数据库连接池最大连接数(近似认为是数据库访问的并行程度)
    '''
    self.func = func
    self.host = host
    self.port = port
    self.user = user
    self.password = password
    self.db = db
    self.queue = Queue()
    self.status=True
    self.max_connections = max_connections
    self.logging_path = logging_path
    self.max_retry_times=max_retry_times
    self.generate_retry_times=generate_retry_times
    self.heart_beat_interval=heart_beat_interval
    self.time_overflow_seconds=time_overflow_seconds
    self.dbpool:DBpool.DBpool|DBpool.DBpoolRenew = dbpool_get(self.max_connections,self.host,self.port,self.user,self.password,self.db,DictCursor,self.logging_path,self.heart_beat_interval)
    try:
      self.conn = self.dbpool.create_connection()
      self.dbpool.put_connection(self.conn)
    except pymysql.Error as e:
      self.dbpool.close()
      simple_log.log(str(e),log_path=self.logging_path)
      raise e
  
  def init_process(self,max_workers:int=10):
    pass

  def fetch_status0(self,ub:int=10):
    '''
    从数据库中找到特定数量的state=0的记录并添加到queue中, 并更新state为1
    '''
    rows = []
    conn = None
    cursor = None
    try:
      # 每次获取新的连接，避免事务状态问题
      conn = self.dbpool.get_connection()
      conn.begin()
      cursor = conn.cursor()

      # 设置事务隔离级别为READ COMMITTED，确保能看到其他事务已提交的数据
      cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

      #弱点: 不熟悉数据库的事务和锁机制

      # 使用原子操作：先更新再查询，避免竞态条件
      # 使用FOR UPDATE SKIP LOCKED来避免锁定冲突
      sql = '''
      SELECT * FROM text_to_video_tasks
      WHERE state = 0 
      ORDER BY id 
      LIMIT %s 
      '''
      args = (ub,)

      status = retry_execute(cursor, self.logging_path, sql, args, self.max_retry_times)
      if status == False:
        raise Exception("""
                        Error in fetch_status0:retry_execute(sql,args) 
                        for finding rows with state=0
                        """)

      rows = list(cursor.fetchall())
      print('--------------------------------------------------------rows size:',len(rows))

      if len(rows) > 0:
        # 立即更新这些记录的状态为1
        placeholders = ','.join(['%s'] * len(rows))
        sql = f'UPDATE text_to_video_tasks SET state = 1 WHERE id IN ({placeholders})'
        args = tuple(row['id'] for row in rows)

        res = retry_execute(cursor, self.logging_path, sql, args, self.max_retry_times)
        if res == False:
          conn.rollback()
          raise Exception("Retry failed, error in fetch_status0:retry_execute(sql,args), update state to 1")
        else:
          conn.commit()
          simple_log.log(f'Successfully updated {len(rows)} tasks from state=0 to state=1', log_path=self.logging_path)

      else: #测试语句, 正式调试时删除
        print('no rows to update')
        time.sleep(5)

      idlist = []
      if len(rows) > 0:
        for row in rows:
          self.queue.put(row)
          idlist.append(row['id'])

      return idlist

    except Exception as e:
      simple_log.log(f'Error in fetch_status0: {str(e)}', log_path=self.logging_path)
      if conn:
        try:
          conn.rollback()
        except:
          pass
      raise e
    finally:
      if cursor:
        try:
          cursor.close()
        except:
          pass
      if conn:
        try:
          self.dbpool.put_connection(conn)
        except:
          pass

  #测试成功
  def close(self):
    self.dbpool.close()

  class callback:
    def __init__(self,package:dict[str,any],dbpool:DBpool,logging_path:str,max_retry_times:int = 5,generate_retry_times:int = 3):
      self.package=package
      self.dbpool=dbpool
      self.logging_path=logging_path
      self.max_retry_times=max_retry_times
      self.generate_retry_times=generate_retry_times
    def __call__(self,future:Future[tuple[int,None|str]]):
      # 添加调试日志，确认回调函数被调用
      simple_log.log(f'Callback started for task {self.package["id"]}', log_path=self.logging_path)
      if future.cancelled():
        simple_log.log(f'Task {self.package["id"]} was cancelled', log_path=self.logging_path)
        return
      else:
        try:
          result = future.result()
        except Exception as e:
          simple_log.log(f'Exception in callback for task {self.package["id"]}: {str(e)}', log_path=self.logging_path)
          return
        index = result[0]
        msg = result[1]
        #没有错误信息直接将任务标识为成功结束
        if msg is None:
          state = 2
          sql = 'update text_to_video_tasks set state = %s, progress = 100 , description = %s where id = %s'
          args = (state, "success", index)
          try:
            conn = self.dbpool.get_connection()
            with conn.cursor() as cursor:
              res = retry_execute(cursor, self.logging_path, sql, args, self.max_retry_times)
              if res == False:
                if conn: 
                  conn.rollback()
                raise Exception('Error in callback:retry_execute(sql,args) for updating state to 2')
              else:
                conn.commit()
                simple_log.log(f'Successfully updated task {self.package["id"]} state to {state}', log_path=self.logging_path)
          finally:
            self.dbpool.put_connection(conn)
        else:
          #任务有错误信息, 检查重试次数
          sql1 = 'select retry_times from text_to_video_tasks where id = %s'
          args1 = (index,)
          try:
            conn = self.dbpool.get_connection()
            #查询重试次数
            with conn.cursor() as cursor:
              res = retry_execute(cursor, self.logging_path, sql1, args1, self.max_retry_times)
              if res == False:
                simple_log.log(f'Error in task{index} for querying retry_times',log_path=self.logging_path)
                raise Exception('Error in task{index} for querying retry_times')
              times = cursor.fetchone()['retry_times']
              #重试次数小于最大重试次数
              if times < self.generate_retry_times:
                #将状态重置为0并重试次数加1
                sql2 = 'update text_to_video_tasks set state = 0, retry_times = %s where id = %s'
                args2 =(times+1,index)
                state2 = retry_execute(cursor, self.logging_path,sql2,args2,self.max_retry_times)
                if state2 == False:
                  if conn:
                    conn.rollback()
                  raise Exception('Error in task{index} for updating state to 0 and retry_times + 1')
                else:
                  conn.commit()
                  simple_log.log(f'Successfully updated task {self.package["id"]} state to 0 and retry_times + 1', log_path=self.logging_path)
              else:
                #重试次数大于等于最大重试次数, 将状态设置为错误
                sql3 = 'update text_to_video_tasks set state = 3, progress=100, description = %s where id = %s'
                args3 = (msg+" -> task failed, retry times >= max_retry_times",index)
                state3 = retry_execute(cursor=cursor,logging_path=self.logging_path,sql=sql3,args=args3,max_retry_times=self.max_retry_times)
                if state3 == False:
                  if conn:
                    conn.rollback()
                  raise Exception('Error in task{index} for updating state to 3')
                else:
                  conn.commit()
                  simple_log.log(f'Successfully updated task {self.package["id"]} state to 3, description: {msg+" -> task failed, retry times >= max_retry_times"}', log_path=self.logging_path)
          finally:
            self.dbpool.put_connection(conn)
            

  def add_output_path(self,args:dict[str,any]):
    pass

  def run(self, slice_size:int=10,max_workers:int=10):
    '''
    查找数据库中status为0的记录, 每一条记录都开一个线程处理, 线程数不够则等待
    '''
    try:
      with ThreadPoolExecutor(max_workers=max_workers) as executor:
        times = 0 #测试语句, 正式调试时删除
        while True:
          self.init_process(max_workers=max_workers) #初始化进程, 在最新版本main_thread_cfg_init中, 函数依照is_init值决定是否执行, 并保证在服务器开启后只执行一次
          print('times:',times) #测试语句, 正式调试时删除
          times += 1 #测试语句, 正式调试时删除
          if self.status == False:
            break
          print('before fetch_status0, times:',times) #测试语句, 正式调试时删除
          idlist = self.fetch_status0(slice_size) #每次获取10条数据, 进行测试, 正式调试传入1024

          #捕获数据后, 返回全部行数据的id, 用于更新进度条
          print('idlist:',idlist) #测试语句, 正式调试时删除
          if self.queue.empty():
            print('queue is empty, times:',times) #测试语句, 正式调试时删除

          print('after fetch_status0, times:',times) #测试语句, 正式调试时删除
          futures = []  # 存储所有的Future对象
          while not self.queue.empty():
            print('into cycle, times:',times) #测试语句, 正式调试时删除
            '''
            对queue中的每一行, 开一个线程处理
            在处理结束后将queue中的行状态改为2
            '''
            '''
            self.func接收参数为字典, 字典内容为{'id','task_uuid','prompt','width','height','text_to_video_pack_id'}
            '''
            row = self.queue.get()
            args = {'id':row['id'],'task_uuid':row['task_uuid'],'prompt':row['prompt'],'width':row['width'],'height':row['height'],'text_to_video_pack_id':row['text_to_video_pack_id']}
            self.add_output_path(args)

            # 提交任务到线程池
            future = executor.submit(self.func,args,self.dbpool)
            futures.append(future)  # 保存Future对象

            # 添加回调函数
            callback_obj = main_thread.callback(args,self.dbpool,self.logging_path,self.max_retry_times,self.generate_retry_times)
            future.add_done_callback(callback_obj)

            simple_log.log(f'Submitted task {args["id"]} to thread pool', log_path=self.logging_path)

          # 等待所有任务完成（可选，用于调试）
          if futures:
            simple_log.log(f'Waiting for {len(futures)} tasks to complete', log_path=self.logging_path)
            # 注意：这里不等待完成，让任务在后台运行
            # 如果需要等待，可以取消注释下面这行
            # executor.shutdown(wait=True)
          print('queue size:',self.queue.qsize()) #测试语句, 正式调试时删除
    finally:
      self.close()

class main_thread_with_config(main_thread):
  def __init__(self,func:Callable,path_config:str,dbpool_get:Callable=DBpool.get_DBpool):
    '''
    传入config.json文件路径, 读取配置文件, 并初始化main_thread
    '''
    self.path_config = path_config
    self.config = read_config.read_config(path_config)
    if self.config is None:
      raise RuntimeError('Failed to load config')
    super().__init__(func=func,host=self.config['host'],port=self.config['port'],user=self.config['user'],password=self.config['password'],db=self.config['db'],max_connections=self.config['max_connections'],logging_path=self.config['log_path'],max_retry_times=self.config['max_retry_times'],generate_retry_times=self.config['generate_retry_times'],heart_beat_interval=self.config['heart_beat_interval'],dbpool_get=dbpool_get,time_overflow_seconds=self.config['time_overflow_seconds'])
    self.output_path = self.config['output_path']
    # self.generate_retry_times = self.config['generate_retry_times']
    # 测试语句, 正式调试时删除
    print('path_config: ',path_config)
    print('config: ',self.config)
    print('max_retry_times: ',self.config['max_retry_times'])
    print('generate_retry_times: ',self.generate_retry_times)
    print('heart_beat_interval: ',self.heart_beat_interval)
    print('time_overflow_seconds: ',self.time_overflow_seconds)
  def add_output_path(self,args:dict[str,any]):
    args['output_path'] = self.output_path

class main_thread_cfg_init(main_thread_with_config):
  def __init__(self,func:Callable,path_config:str,dbpool_get:Callable=DBpool.get_DBpool):
    self.__is_init = True
    super().__init__(func=func,path_config=path_config,dbpool_get=dbpool_get)

  #确实可以在开始时将全部未完成任务状态转回为0, 但是无法保证在run过程中不会出现新的未完成任务
  def init_process(self,max_workers:int=10):
    '''
    在服务器启动时, 只负责将未完成任务的状态转回为0, 接下来交给run处理
    本函数只在初始状态执行一次
    '''
    if self.__is_init == False:
      return
    else:
      print('start init_process')
      self.__is_init = False
      conn = self.dbpool.get_connection()
      try:
        with conn.cursor() as cursor:
          #测试语句 - 查看更新前的状态
          sql = 'select id from text_to_video_tasks where state = 1'
          res=retry_execute(cursor,self.logging_path,sql,None,self.max_retry_times)
          print('res:',res)
          if res == False:
            raise Exception('Error in init_process:retry_execute(sql,args) for finding rows with state=1')

          state_1_ids = [row['id'] for row in cursor.fetchall()]
          print('\nstate_1_ids size:\n',len(state_1_ids),'\nstate_1_ids:\n',state_1_ids)
          print('**************************************************************************')
          # update语句中最好不要嵌套子查询, 否则会报错
          if len(state_1_ids) > 0:
            # 使用IN子句进行更新
            placeholders = ','.join(['%s'] * len(state_1_ids))
            sql = f'update text_to_video_tasks set state = 0 where id in ({placeholders})'
            args = tuple(state_1_ids)
            res = retry_execute(cursor,self.logging_path,sql,args,self.max_retry_times)
            conn.commit()
            print('res:',res)
            if res == False:
              conn.rollback()
              print('Error in init_process:retry_execute(sql,args) for updating state to 0')
              raise Exception('Error in init_process:retry_execute(sql,args) for updating state to 0')
            affected_rows = cursor.rowcount
            print(f'update success, affected rows: {affected_rows}')
          else:
            print('no rows with state=1 to update')

          #测试语句 - 查看更新后的状态
          test_list = []
          sql = 'select id from text_to_video_tasks where state = 1'
          res = retry_execute(cursor,self.logging_path,sql,None,self.max_retry_times)
          if res == False:
            raise Exception('Error in init_process:retry_execute(sql,args) for finding rows with state=1')
          else:
            test_list = list(cursor.fetchall())
          print('test_list: (after update)\n',test_list)

      except Exception as e:
        print(f'init_process failed: {str(e)}')
        simple_log.log(str(e)+' init_process failed',log_path=self.logging_path)
      finally:
        self.dbpool.put_connection(conn)

class main_thread_TimedRenew(main_thread_cfg_init):
  def __init__(self,func:Callable,path_config:str,dbpool_get:Callable=DBpool.get_DBpoolRenew):
    super().__init__(func=func,path_config=path_config,dbpool_get=dbpool_get)