# /user/bin/env python
# coding:utf-8
import sys,glob,gzip
import time
import os
import cx_Oracle


def db_19():
        con = cx_Oracle.connect('web','adm2017in09g08web','10.90.19.2:1521/orcl')
        cur = con.cursor()
        return con,cur




def get(start):
	con,cur = db_19()
	
	pv=0
	uv=0
	ip=0

	dic={}


	for data in glob.glob('/data1/logs/wap_new/%s/*.sta.gz'%start):
		for line in gzip.open(data):
		
	
			ss  = line.split('\t')
			if len(ss)<5:
				continue
			uip=ss[0]
			url=ss[2]
			uid=ss[4]
			if ('://m.ifeng.com/huaweillq' in url):
				typ='http://m.ifeng.com/huaweillq'
				dic.setdefault(typ,[0,set(),set()])
				dic[typ][0]=dic[typ][0]+1
				dic[typ][1].add(uid)
				dic[typ][2].add(uip)
			elif ('://m.ifeng.com/huaweillqtest' in url):

				typ='http://m.ifeng.com/huaweillqtest'
				dic.setdefault(typ,[0,set(),set()])
				dic[typ][0]=dic[typ][0]+1
				dic[typ][1].add(uid)
				dic[typ][2].add(uip)
		
			else:
				continue

	for d in dic:
		typ=d
		pv=dic[d][0]
		uv=len(dic[d][1])
		ip=len(dic[d][2])	
				
		sql_in="insert into web.test_ip_20181108 (typ,pv,uv,ip,tm) values(:1,:2,:3,to_date(:4,'yyyy-mm-dd'))"
		try :
			cur.execute(sql_in,(typ,pv,uv,ip,start))
			con.commit()
		except:
			'error!to large!'
			

	con.close()				



if __name__ == "__main__":

	sdate = time.mktime(time.strptime('2018-10-09','%Y-%m-%d'))
	edate = time.mktime(time.strptime('2018-11-07','%Y-%m-%d'))

	while sdate <= edate:

		start = time.strftime("%Y-%m-%d",time.localtime(sdate))
		get(start)

		sdate += 86400

		



	
