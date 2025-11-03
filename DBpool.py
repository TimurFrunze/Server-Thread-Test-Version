from concurrent.futures import ThreadPoolExecutor
from typing import Any


from ast import Dict
import queue
import threading
import pymysql
from pymysql.connections import Connection
from pymysql.cursors import Cursor, DictCursor, SSDictCursor, SSCursor
from queue import Queue
import simple_log
import traceback
import time
class DBpool:
  '''
  数据库连接池
  '''  
  
  def create_connection(self):
    try:
      if self.cursorclass == 'Default' or self.cursorclass is None or self.cursorclass == 'Cursor':
        conn = pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.password,db=self.db)
      elif self.cursorclass == 'DictCursor':
        conn = pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.password,db=self.db,cursorclass=DictCursor)
      elif self.cursorclass == 'SSDictCursor':
        conn = pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.password,db=self.db,cursorclass=SSDictCursor)
      elif self.cursorclass == 'SSCursor':
        conn = pymysql.connect(host=self.host,port=self.port,user=self.user,password=self.password,db=self.db,cursorclass=SSCursor)
      else:
        simple_log.log(f"Invalid cursorclass: {self.cursorclass} in creating of DBpool",log_path=self.logging_path)
        raise Exception(f"Invalid cursorclass: {self.cursorclass}")
      return conn
    except Exception as e:
      simple_log.log(traceback.format_exc(),log_path=self.logging_path)
      raise e
  
  def __init__(self,max_connections:int,host:str,port:int,user:str,password:str,db:str,cursorclass:str = 'Default',logging_path:str = './logging_dir/log.txt'):
    self.host = host
    self.port = port
    self.user = user
    self.password = password
    self.db = db
    self.pool = Queue[Connection]()
    self.max_connections = max_connections
    self.cursorclass = cursorclass
    self.logging_path = logging_path
    successful_connections = 0
    for i in range(self.max_connections):
      try:
        conn = self.create_connection()
        self.pool.put(conn)
        successful_connections += 1
        #改为日志记录
        print(f"Successfully created connection {successful_connections}/{max_connections}")
        
      except Exception as e:
        #改为日志记录
        simple_log.log(traceback.format_exc(),log_path = self.logging_path)
        simple_log.log(f"Failed to create connection {i+1}: {str(e)}",log_path=self.logging_path)
        print(f"Failed to create connection {i+1}: {str(e)}")
        break
    actual_connections = self.pool.qsize()
    if actual_connections != max_connections:
      #改为日志记录
      simple_log.log(f"Warning: Only created {actual_connections} out of {max_connections} requested connections",log_path=self.logging_path)
      while self.pool.empty() == False:
        self.pool.get().close()
      raise Exception(f"Failed to create {max_connections} connections. Only {actual_connections} connections were created successfully. This might be due to MySQL max_connections limit or system resource constraints.")

  # 获取连接
  def get_connection(self):
    return self.pool.get(block=True)

  # 获取连接, 超时时间默认10秒, 超时后抛出TimeoutError
  def timed_get_connection(self,timeout:int=10):
    return self.pool.get(block=True,timeout=timeout)
  
  # 放回连接
  def put_connection(self,conn:Connection):
    self.pool.put(conn)
    
  # 关闭连接池
  def close(self):
    while self.pool.empty() == False:
      self.pool.get().close()
  
  # 获取当前连接池大小
  def get_pool_size(self):
    return self.pool.qsize()
  
  # 检查MySQL连接限制
  def check_mysql_limits(self):
    try:
      conn = self.pool.get()
      with conn.cursor() as cursor:
        cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
        result = cursor.fetchone()
        max_conn = result[1] if result else "Unknown"
        
        cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
        result = cursor.fetchone()
        current_conn = result[1] if result else "Unknown"
        
        cursor.execute("SHOW STATUS LIKE 'Max_used_connections'")
        result = cursor.fetchone()
        max_used = result[1] if result else "Unknown"
        
        print(f"MySQL Status:")
        print(f"  Max connections allowed: {max_conn}")
        print(f"  Current connections: {current_conn}")
        print(f"  Max connections used: {max_used}")
        
      self.pool.put(conn)
      return max_conn, current_conn, max_used
    except Exception as e:
      simple_log.log(traceback.format_exc(),log_path=self.logging_path)
      print(f"Error checking MySQL limits: {e}")
      return None, None, None    

class ConnectionRenew(pymysql.Connection):
  def __init__(
        self,
        *,
        user=None,  # The first four arguments is based on DB-API 2.0 recommendation.
        password="",
        host=None,
        database=None,
        unix_socket=None,
        port=0,
        charset="",
        collation=None,
        sql_mode=None,
        read_default_file=None,
        conv=None,
        use_unicode=True,
        client_flag=0,
        cursorclass=DictCursor,
        init_command=None,
        connect_timeout=10,
        read_default_group=None,
        autocommit=False,
        local_infile=False,
        max_allowed_packet=16 * 1024 * 1024,
        defer_connect=False,
        auth_plugin_map=None,
        read_timeout=None,
        write_timeout=None,
        bind_address=None,
        binary_prefix=False,
        program_name=None,
        server_public_key=None,
        ssl=None,
        ssl_ca=None,
        ssl_cert=None,
        ssl_disabled=None,
        ssl_key=None,
        ssl_key_password=None,
        ssl_verify_cert=None,
        ssl_verify_identity=None,
        compress=None,  # not supported
        named_pipe=None,  # not supported
        passwd=None,  # deprecated
        db=None,  # deprecated
        last_renew_time = 0
    ):
    super().__init__(user=user,password=password,host=host,database=database,unix_socket=unix_socket,port=port,charset=charset,collation=collation,sql_mode=sql_mode,read_default_file=read_default_file,conv=conv,use_unicode=use_unicode,client_flag=client_flag,cursorclass=cursorclass,init_command=init_command,connect_timeout=connect_timeout,read_default_group=read_default_group,autocommit=autocommit,local_infile=local_infile,max_allowed_packet=max_allowed_packet,defer_connect=defer_connect,auth_plugin_map=auth_plugin_map,read_timeout=read_timeout,write_timeout=write_timeout,bind_address=bind_address,binary_prefix=binary_prefix,program_name=program_name,server_public_key=server_public_key,ssl=ssl,ssl_ca=ssl_ca,ssl_cert=ssl_cert,ssl_disabled=ssl_disabled,ssl_key=ssl_key,ssl_key_password=ssl_key_password,ssl_verify_cert=ssl_verify_cert,ssl_verify_identity=ssl_verify_identity,compress=compress,named_pipe=named_pipe,passwd=passwd,db=db)
    # 标记该连接上一次进行检查的时间
    self.last_renew_time = last_renew_time
  

class DBpoolRenew(DBpool):
  '''
  每经过一段自定义时间, 就会进入"更新"状态, 在更新状态下, 每次回收到的连接对象会被关闭并重新创建,
  在将全部的连接对象都更新完毕后会退出"更新"状态, 并重置时间
  '''
  #默认使用DictCursor
  def create_connection(self):
    return ConnectionRenew(host=self.host,port=self.port,user=self.user,password=self.password,db=self.db,cursorclass=DictCursor,last_renew_time=time.time())
  
  def __init__(self,max_connections:int,host:str,port:int,user:str,password:str,db:str,cursorclass=DictCursor,logging_path:str = './logging_dir/log.txt',time_interval:int = 100,max_workers:int = 8):
    super().__init__(max_connections,host,port,user,password,db,cursorclass,logging_path)
    self.time_interval = time_interval #单位为秒
    self.max_workers = max_workers
    # self.ThreadPool = ThreadPoolExecutor(max_workers=self.max_workers)
  
  '''
  在每次获取和放回连接时, 检查该连接的上一次检查时间, 若超过时间间隔, 则创建新的连接, 并关闭旧连接
  '''
  
  def put_connection(self,conn:ConnectionRenew):
    last_time = conn.last_renew_time
    conn.last_renew_time = time.time()
    if time.time() > last_time + self.time_interval:
      try:
        with conn.cursor() as cursor:
          cursor.execute("SELECT 1")
      except Exception as e:
        simple_log.log(traceback.format_exc()+"\n-> "+str(e)+"\n-> Fixing connection error...",log_path=self.logging_path)
        if conn:
          conn.close()
        try:
          conn = self.create_connection()
          self.pool.put(conn)
        except Exception as err:
          simple_log.log(traceback.format_exc()+"\n-> "+str(err)+"\n-> Failed to create new connection while checking heartbeat",log_path=self.logging_path)
          raise err
      else:
        self.pool.put(conn)
    else:
      self.pool.put(conn)
    
  def get_connection(self):
    conn:ConnectionRenew = self.pool.get(block=True)
    last_time = conn.last_renew_time
    conn.last_renew_time = time.time()
    if time.time() > last_time + self.time_interval:
      #进行心跳检查
      try:
        with conn.cursor() as cursor:
          cursor.execute("SELECT 1")
      except Exception as e:
        simple_log.log(traceback.format_exc()+"\n-> "+str(e)+"\n-> Fixing connection error...",log_path=self.logging_path)
        if conn:
          conn.close()
        try:
          conn = self.create_connection()
        except Exception as err:
          simple_log.log(traceback.format_exc()+"\n-> "+str(err)+"\n-> Failed to create new connection while checking heartbeat",log_path=self.logging_path)
          raise err
        else:
          return conn
      else:
        return conn
    else:
      return conn
  
  def timed_get_connection(self, timeout: int = 10):
    conn:ConnectionRenew = self.pool.get(block=True,timeout=timeout)
    last_time = conn.last_renew_time
    conn.last_renew_time = time.time()
    if time.time() > last_time + self.time_interval:
      #进行心跳检查
      try:
        with conn.cursor() as cursor:
          cursor.execute("SELECT 1")
      except Exception as e:
        simple_log.log(traceback.format_exc()+"\n-> "+str(e)+"\n-> Fixing connection error...",log_path=self.logging_path)
        if conn:
          conn.close()
        try:
          conn = self.create_connection()
        except Exception as err:
          simple_log.log(traceback.format_exc()+"\n-> "+str(err)+"\n-> Failed to create new connection while checking heartbeat",log_path=self.logging_path)
          raise err
        else:
          return conn
      else:
        return conn
    else:
      return conn
  
  def close(self):
    # self.ThreadPool.shutdown(wait=True)
    super().close()
  
  # def fix_connection(self,conn:ConnectionRenew):
  #   #运行中主动归还有误的连接对象给连接池, 连接池将此连接测试修复后返回新连接
  #   def fix_connection_task(conn:ConnectionRenew):
  #     try:
  #       with conn.cursor() as cursor:
  #         cursor.execute("SELECT 1")
  #     except Exception as e:
  #       simple_log.log(traceback.format_exc()+"\n-> "+str(e)+"\n-> Fixing connection error...",log_path=self.logging_path)
  #       if conn:
  #         conn.close()
  #       try:
  #         new_conn = self.create_connection()
  #       except Exception as err:
  #         simple_log.log(traceback.format_exc()+"\n-> "+str(err)+"\n-> Failed to create new connection while fixing connection error",log_path=self.logging_path)
  #         raise err
  #       else:
  #         return new_conn
  #     else:
  #       return conn
  #   future = self.ThreadPool.submit(fix_connection_task,conn)
  #   result = None
  #   try:
  #     result = future.result()
  #   except Exception as err:
  #     simple_log.log(traceback.format_exc()+"\n-> "+str(err)+"\n-> Failed to fix connection error",log_path=self.logging_path)
  #     raise err
  #   return result
  

def get_DBpool(max_connections,host,port,user,password,db,cursorclass,logging_path,time_interval):
  return DBpool(max_connections=max_connections,host=host,port=port,user=user,password=password,db=db,cursorclass=cursorclass,logging_path=logging_path)

def get_DBpoolRenew(max_connections,host,port,user,password,db,cursorclass,logging_path,time_interval):
  return DBpoolRenew(max_connections=max_connections,host=host,port=port,user=user,password=password,db=db,cursorclass=cursorclass,logging_path=logging_path,time_interval=time_interval)