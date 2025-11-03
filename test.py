import random
import string
import pymysql
from datetime import datetime

def generate_random_string(length:int=20):
  return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_random_date() -> str:
  return datetime.now().strftime('%Y-%m-%d')

def random_num_0_to_3() -> int:
  return random.randint(0,3)

def random_num_0_or_1() -> int:
  return random.randint(0,1)

def connect_by_config(path:str)->pymysql.Connection:
  import json
  with open(path,"r") as f:
    config = json.load(f)
    conn = pymysql.connect(
      host = config['host'],
      port = config['port'],
      user = config['user'],
      password = config['password'],
      db = config['db'],
    )
  return conn

# if __name__ == '__main__':
#   print(generate_random_string(20))

# if __name__ == '__main__':
#   #向数据库中插入1000条数据
#   conn = pymysql.connect(host='127.0.0.1',port=3306,user='root',password='tkp040629',database='esports')
#   with conn.cursor() as cursor:
#     placeholders = ','.join(['%s']*11)
#     for i in range(1000):
#       cursor.execute(f'insert into movie_agent_tasks values ({placeholders})',(
#       None,
#       generate_random_date(),
#       generate_random_date(),
#       generate_random_date(),
#       generate_random_string(100),
#       generate_random_string(20),
#       1024,
#       1024,
#       0,
#       0,
#       generate_random_string(20),
#       ))
#     conn.commit()
#   conn.close()
#   print('insert 1000 rows success')

# if __name__ == '__main__':
#   #向数据库中插入1000条数据
#   conn = pymysql.connect(host='127.0.0.1',port=3306,user='root',password='tkp040629',database='esports')
#   with conn.cursor() as cursor:
#     placeholders = ','.join(['%s']*11)
#     for i in range(1000):
#       cursor.execute(f'insert into movie_agent_tasks values ({placeholders})',(
#       None,
#       generate_random_date(),
#       generate_random_date(),
#       generate_random_date(),
#       generate_random_string(100),
#       generate_random_string(20),
#       1024,
#       1024,
#       random_num_0_or_1(),
#       0,
#       generate_random_string(20),
#       ))
#     conn.commit()
#   conn.close()
#   print('insert 1000 rows success')

if __name__ == '__main__':
    #向数据库中插入1000条数据
    conn = connect_by_config('./movie_agent_config.json')
    with conn.cursor() as cursor:
      for i in range(1000):
        cursor.execute('insert into text_to_video_tasks values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                       (
                        None,
                        None,
                        None,
                        None,
                        generate_random_string(),
                        random.randint(0,1000),
                        generate_random_string(30),
                        None,
                        None,
                        random_num_0_or_1(),
                        0,
                        None,
                        random.randint(0,1000),
                        0
                       )
                       )
        conn.commit()
    conn.close()
    print('insert 1000 rows success')
