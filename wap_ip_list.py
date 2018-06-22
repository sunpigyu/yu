# /user/bin/env python
# coding:utf-8
import sys,glob,gzip
import time
import os
import cx_Oracle

def db_19():
        con = cx_Oracle.connect('web','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur




path="/data5/iyc_logs/backup/2018-05-27.log"

con,cur = db_19()


for data in glob.glob('/data1/logs/wap_new/2018-06-22/08[3-5]*.sta.gz'):
	for line in gzip.open(data):
		

		tm_1=data.split('/')[-1].split('.')[0]	
		ss  = line.split('\t')
		if len(ss)<5:
			continue
		uip=ss[0]
		url=ss[2]
		path=ss[5]
		uid=ss[4]
		ref=ss[3]
		tm=ss[9]
		if ('i.ifeng.com' in url):
			sql_in="insert into wapnew.test_iplist_20180622_0830 (uip,url,tm_1) values(:1,:2,:3)"
			try :
				cur.execute(sql_in,(uip,url,tm_1))
				con.commit()
			except:
				'error!to large!'
			

con.close()				


		
