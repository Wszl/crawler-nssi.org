#!/usr/bin/python3
# crawle https://www.nssi.org.cn/nssi/services/biaozhundongtai.jsp

import requests
import time
import pymongo
import pymysql
import os
import json

mgdb_con_str = os.getenv("MGDB_CON_STR", "mongodb://localhost:27017/")
mgdb_user = os.getenv("MGDB_USER", "admin")
mgdb_pwd = os.getenv("MGDB_PWD", "admin")

mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_port = os.getenv("MYSQL_PORT", "3306")
mysql_user = os.getenv("MYSQL_USER", "root")
mysql_pwd = os.getenv("MYSQL_PWD", "root")
mysql_db = os.getenv("MYSQL_DB", "db")

db_use = "mysql"
mysql_con = pymysql.connect(host=mysql_host, port=int(mysql_port), user=mysql_user, password=mysql_pwd, database=mysql_db)
cursor = mysql_con.cursor()

def crawler():
  status_ary = ["release", "implement", "invalid", "collect"]
  sub_type_ary = ["ST_N_SBTS", "ST_N_CSIC", "ST_F_NATIONAL", "ST_F_INTER", "ST_F_INSTITUTE"]
  def parse_lines(bs, status, sub_type) -> list:
    lines_list = []
    bs_j = json.loads(bs)
    total_page = bs_j['dynamicInfos']['totalPages']
    total_num = bs_j['dynamicInfos']['totalNumber']
    data = bs_j['dynamicInfos']['list']
    for i in data:
      d = []
      d.append(i['a100'])
      d.append(i.get('a298', i.get('a302', "N/A")))
      repace_stands = i['a461']
      if repace_stands is not None and repace_stands != "":
        repace_stands_j = json.loads(repace_stands)
        rs_a = []
        for rs in repace_stands_j:
          rs_s = rs['ra100_s']
          rs_a.append(rs_s)
        d.append(rs_a)
      else:
        d.append("")
      d.append(i.get('a205', i.get('a101', "N/A")))
      d.append(status)
      d.append(sub_type)
      lines_list.append(d)
    return lines_list, total_page, total_num
  
  def get_page(status, sub_type):
    url_page = "https://www.nssi.org.cn/cssn/front/dynamic/getDynamicInfosByPageCond"
    total_page = 0
    page = 1
    has_next_page = True
    while has_next_page:
      time.sleep(3)
      param = {
        "status": status,
        "sub_type": sub_type,
        "page": page
      }
      print("post url {}, param {}, page {} , total_page {}".format(url_page, param, page, total_page))
      headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cookie": "__jsluid_s=90d17bda6ef3d0414636025372b768d7; token=; uname=; account_id=; user_id=; relogin_name=; user_type=; JSESSIONID=DBE88800828A4E53BE73E95E1C581356",
        "Origin": "https://www.nssi.org.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
      }
      result = requests.post(url_page, param, timeout=15, headers=headers)
      if result.status_code != 200:
        raise Exception("crawle failed in page {} status code {}, "+
                        "result is {} ".format(page, result.status_code, result))
      dec_res = result.content.decode("u8")
      line_list, total_page, total_num = parse_lines(dec_res, status, sub_type)
      if page > total_page:
        has_next_page = False
      page += 1
      for item in line_list:
        if db_use == "mysql":
          save_mysql(item)
        else:
          save(item)
  for status in status_ary:
    for sub_type in sub_type_ary:
      get_page(status, sub_type)


def save(line):
  line_obj = {
    "code": line[0],
    "name": line[1],
    "replace": line[2],
    "public_date": line[3],
    "status": line[4],
    "sub_type": line[5]
  }
  mgcli = pymongo.MongoClient(mgdb_con_str, username=mgdb_user, password=mgdb_pwd)
  nssi_db = mgcli['crawler_nssi']
  nssi_collection = nssi_db['nong_yao']
  insert_res = nssi_collection.insert_one(line_obj)
  print("data {} inserted to mongodb, id is {}".format(line_obj, insert_res.inserted_id))

def save_mysql(line):
    mysql_con.autocommit(True)
    cursor.execute("insert into cralwe_nssi (code, `name`, `replace`, public_date, `status`, sub_type) values (%s, %s, %s, %s, %s, %s)",
                (line[0], line[1], json.dumps(line[2]), line[3], line[4], line[5]))
    print("data {} inserted to mysql".format(line))

        

if __name__ == "__main__":
  try:
    crawler()
  finally:
    mysql_con.close()
