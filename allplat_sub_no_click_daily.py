
#!-*-coding:utf-8 -*-

import time
import os
import sys
import cx_Oracle
import pyhs2
import urllib
import urllib2
import json
import traceback
import multiprocessing

#import sns_get_title


reload(sys)
sys.setdefaultencoding('utf-8')

os.environ['NLS_LANG'] = '.UTF8'

def hive_web():
        server = pyhs2.connect(host= 'web集群',port =10000,user='hadoop',authMechanism = 'PLAIN')
        client = server.cursor()
	return server,client

def hive_app():
        server = pyhs2.connect(host= 'app集群',port =10000,user='hadoop',authMechanism = 'PLAIN')
        client = server.cursor()
	return server,client
UDF_list1 = [
"add jar /home/hadoop/WordCountPeter.jar",
"create temporary function getHashcode as 'com.ifeng.tongji.udf.MyUDF'"]



def db_77():
        con = cx_Oracle.connect('app','app库密码','10.90.19.1:1521/orcl')
        cur = con.cursor()
        return con,cur

def db_19():
        con = cx_Oracle.connect('web','web库密码','10.90.19.2:1521/orcl')
        cur = con.cursor()
        return con,cur



def getMessage(id):

	m_list=[]

	sub_id=id.split('sub_')[1]

        url = "http://local.tongji.ifeng.com/adwall/getSourceGrade?sourceType=sub&idEncode=%s"%(sub_id)

        try:
                html = urllib.urlopen(url).read()

	except:
#		print id,"url error!"
		return m_list
	try:

		dic = json.loads(html)
	except:
#		print id,'json loads html error!'
		return m_list

	if (len(dic["category"])==0):
		return m_list

	category=dic["category"]
	le=len(category)

	if (le%3==0):
		count=le/3
		for i in range(0,count):

			word=category[i*3]
			cate=category[i*3+1]
			rate=category[i*3+2]
			
			m_list.append((word,cate,rate))

		return m_list

	else:
		count=int(le/3)

		for i in range(0,count):

			word=category[i*3]
			cate=category[i*3+1]
			rate=category[i*3+2]
			
			m_list.append((word,cate,rate))

		return m_list
				






def get_data(date,num,hc):

	con,cur = db_19()

	server,client = hive_web()
	for UDF in UDF_list1:
                try:
                        client.execute(UDF)
                except:
                        print UDF,'is Wrong'
                        continue

	get_hql="select id from sub_no_click_daily where dt='%s' and getHashcode(id,%d)=%d"%(date,num,hc)

        client.execute(get_hql)
        result = client.fetchall()
	

	for (id,) in result:
		
		try: 
			m_list = getMessage(id)

			if (len(m_list)==0):
#				print id,'getMessage interface get error-----'
				continue
					
			
		except :
#			print id,'getMessage error!-----'
			continue

		for (word,cate,rate) in m_list:
			
			try:

				in_sql = "insert into web.d_doc_tag_sub_noclick(id,word,cate,rate,tm) values(:1,:2,:3,:4,to_date(:5,'yyyy-mm-dd'))"
			
				cur.execute(in_sql,(id,word,cate,rate,date))

				con.commit()

			except :
				print 'insert',id,'error-------'
				continue
	
	server.close()
	con.close()

def continuehc(date,num):
	jobs=[]

	for i in range(0,num):
	
		p=multiprocessing.Process(target=get_data,args=(date,num,i))

		jobs.append(p)
	


	for p in jobs:
#               p.daemon=true
                p.start()
        for p in jobs:
                p.join()





if __name__ == '__main__':

        if 'daily' in sys.argv:
                dateStr = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400))
                print dateStr,'starts @',time.strftime('%Y-%m-%d %H:%M:%S')
                funcs = [continuehc]


                for func in funcs:
                        try:
                                func(dateStr,5)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                                dateStr,func,'is Wrong'
                                print traceback.print_exc()
                print dateStr,'is done @',time.strftime('%Y-%m-%d %H:%M:%S')
	
        if 'test' in sys.argv:
                dateStr = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*2))
                print dateStr,'is starts @',time.strftime('%Y-%m-%d %H:%M:%S')
                
                funcs = [continuehc]
                for func in funcs:
                        try:
                                func(dateStr,5)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                                dateStr,func,'is Wrong'
                                print traceback.print_exc()
                print dateStr,'is done @',time.strftime('%Y-%m-%d %H:%M:%S')
        if 'bushu' in sys.argv:
                sdate = time.mktime(time.strptime('2018-06-13','%Y-%m-%d'))
                edate = time.mktime(time.strptime('2018-06-13','%Y-%m-%d'))
                while sdate >= edate:
                        dateStr = time.strftime('%Y-%m-%d',time.localtime(sdate))
                        print dateStr,'starts @',time.strftime('%Y-%m-%d %H:%M:%S')

                        funcs = [continuehc]
                        for func in funcs:
                                func(dateStr,20)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        sdate -= 86400
                        print dateStr,'done @',time.strftime('%Y-%m-%d %H:%M:%S')


	
