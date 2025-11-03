from main_thread import main_thread, main_thread_TimedRenew, main_thread_with_config, main_thread_cfg_init
from DBpool import DBpool
import random

def generate_failure_rate(failure_percent:int) -> bool:
  #返回False表示失败, 返回True表示成功s
  num = random.randint(0,100)
  if num <= failure_percent:
    return False
  else:
    return True

def test_func_with_dbpool(args:dict[str,any],dbpool:DBpool)->tuple[int,None|str]:
  print('test_func_with_dbpool',args)
  print('dbpool',dbpool)
  if 'prompt' not in list(args.keys()) or args['prompt'] is None:
    return args['id'],'error'
  return args['id'],None

def test_func_with_failure(args:dict[str,any],dbpool:DBpool) -> tuple[int,None|str]:
  result = test_func_with_dbpool(args,dbpool)
  if generate_failure_rate(50) == False:
    return result[0],'error'
  else:
    return result[0],None
# if __name__ == '__main__':
#   # 使用配置文件初始化main_thread
#   mth = main_thread_with_config(test_func_with_dbpool,path_config='./movie_agent_config.json')
#   mth.run()

if __name__ == '__main__':
  # mth = main_thread_cfg_init(func=test_func_with_failure,path_config='./movie_agent_config.json')
  # mth.run()
  mth= main_thread_TimedRenew(func=test_func_with_failure,path_config='./movie_agent_config.json')
  mth.run()

# if __name__ == '__main__':
#   mth = main_thread_with_config(test_func_with_dbpool,path_config='./movie_agent_config.json')
#   mth.run()

#传给模型函数的参数字典需要添加字段
# if __name__ == '__main__':
#   mth = main_thread_TimedConnection(func=test_func_with_dbpool,path_config='./movie_agent_config.json')
#   mth.run()