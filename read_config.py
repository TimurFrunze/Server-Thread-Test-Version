#读取当前工作目录下的config.json文件, 并检查参数是否正确
import json
import os
'''
json文件中的参数为: host,port,user,password,db,max_connections,output_path,log_path,max_retry_times
'''
def read_config(path_json:str)->dict:
  try:
    with open(path_json,'r',encoding='utf-8') as f:
      config = json.load(f)
      if check_config(config) == False:
        raise Exception('config.json args error, please check config.json file')
      return config
  except Exception as e:
    print(f'read config.json file failed: {e}')
    return None
      
def check_config(config:dict)->bool:
  """检查配置参数，允许额外参数"""
  required_args = ['host','port','user','password','db','max_connections','output_path','log_path','max_retry_times']
  
  # 检查是否包含所有必需参数
  for arg in required_args:
    if arg not in config:
      print(f"缺少必需参数: {arg}")
      return False
  
  # 检查必需参数的类型
  type_checks = {
    'host': str,
    'port': int,
    'user': str,
    'password': str,
    'db': str,
    'max_connections': int,
    'output_path': str,
    'log_path': str,
    'max_retry_times': int
  }
  
  for arg, expected_type in type_checks.items():
    if not isinstance(config[arg], expected_type):
      print(f"参数 {arg} 类型错误，期望 {expected_type.__name__}，实际 {type(config[arg]).__name__}")
      return False
  
  return True