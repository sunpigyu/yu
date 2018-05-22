#!-*-coding:utf-8 -*-

import cx_Oracle
import pyhs2
import os
import time
import urllib
import json
import sys
os.environ['NLS_LANG'] = '.UTF8'

def hivedb():
        server = pyhs2.connect(host= '#.187',port =10000,user='hadoop',authMechanism = 'PLAIN')
        client = server.cursor()
        return server,client

def db_77():
        con = cx_Oracle.connect('web','#','#:1521/orcl')
        cur = con.cursor()
        return con,cur

def doc(date):

	server,client = hivedb()
	con,cur = db_77()

	

	hql="select a.dt,a.pv,a.doc,coalesce(b.title,'None'),a.uv,a.ci,coalesce(b.source,'None') from (select dt,pv,doc,uv,ci  from web_doc where dt='%s' and pv >=100)  a left outer join hbase_doc_title_src b on a.doc=b.url"%date

	client.execute(hql)

	result = client.fetchall()
	
	for (dt,pv,doc,title,uv,ci,source) in result:
		in_sql = "insert into web.auto_doc_20180521 (dt,pv,doc,title,uv,ci,source) values(:1,:2,:3,:4,:5,:6,:7)"
		cur.execute(in_sql,(dt,pv,doc,title,uv,ci,source))
		con.commit()
	con.close()
	server.close()		


if __name__ == '__main__':


	if 'bushu' in sys.argv:

                sdate = time.mktime(time.strptime('2018-03-01','%Y-%m-%d'))
                edate = time.mktime(time.strptime('2018-05-20','%Y-%m-%d'))
                while sdate <= edate:
                        dateStr = time.strftime('%Y-%m-%d',time.localtime(sdate))
                        print dateStr,'starts @',time.strftime('%Y-%m-%d %H:%M:%S')

                        funcs = [doc]
                        for func in funcs:
                                func(dateStr)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        sdate += 86400
                        print dateStr,'done @',time.strftime('%Y-%m-%d %H:%M:%S')




		

		
