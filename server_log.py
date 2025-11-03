from platform import java_ver
import traceback
import os
import time
def get_time():
  return time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())

def append_log(message:str,log_path:str):
  if os.path.exists(log_path) == False:
    with open(log_path,'a') as file:
      file.write(get_time()+':\n'+message+'\n')
