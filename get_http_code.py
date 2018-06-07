#! /usr/bin/env python
# encoding:utf-8

import urllib2
import cx_Oracle
import os,sys
import time

os.environ['NLS_LANG'] = '.UTF8'

def db_19():
        con = cx_Oracle.connect('用户名','密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur


def get_data():
	
	con,cur = db_19()
	
	sql="select 'http://m.ifeng.com/news/shareNews?guid='||substr(id,7) url from app.linshi where id like 'video%' and info=0"

	cur.execute(sql)

	url_list=cur.fetchall()

	for (url,) in url_list:
		response=urllib2.urlopen(url)

		if (response.getcode()==200):
			continue

		else:
	
			print response.getcode(),'url is not open! url =',url

	con.close()

if __name__ == '__main__':

	get_data()
