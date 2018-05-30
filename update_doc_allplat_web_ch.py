#!-*-coding:utf-8 -*-

import time
import os
import sys
import cx_Oracle
import pyhs2
import urllib2
import json
import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

os.environ['NLS_LANG'] = '.UTF8'

UDF_list1 = ["add jar /home/hadoop/Hive_UDF.jar",
"add jar /home/hadoop/HivePlugins.jar",
"create temporary function getNewsDocId as 'other.getNewsDocId'",
"create temporary function pgfilter as 'client.PagetypeFilter'",
"create temporary function getreftype as 'client.GetRefTypeField'",
"create temporary function getDocSrcType as 'other.getDocSrcType'",
"create temporary function getPar as 'ifeng.videoapp.GetParameter'"]




def hive_web():
        server = pyhs2.connect(host= 'web hive地址',port =10000,user='hadoop',authMechanism = 'PLAIN')
        client = server.cursor()
        return server,client

def hive_app():
        server = pyhs2.connect(host= 'app hive地址',port =10000,user='hadoop',authMechanism = 'PLAIN')
        client = server.cursor()
        return server,client



def db_77():
        con = cx_Oracle.connect('app','app密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur



def db_19():
        con = cx_Oracle.connect('web','web密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur


def get_data(date):
	
	

	dic=get_list()	
	
	con,cur = db_19()
	
	

	sql="select distinct a.id,b.url_pc from (select id from web.f_doc_allplat where day=to_date('%s','yyyy-mm-dd') and length(id)=15 and web_ch is null) a join web.d_doc_new b on 'cmpp_'||a.id = b.id"%date
	cur.execute(sql)
	result=cur.fetchall()	
	
	for (id,url_pc) in result:
		url=url_pc.split('http://')[1].split('.ifeng')[0]
		url_home='http://'+url+'.ifeng.com/'
		
		if dic.has_key(url_home):
			chname=dic[url_home]
			
			update_sql="update web.f_doc_allplat set web_ch = '%s' where day = to_date('%s','yyyy-mm-dd') and id = '%s'"%(chname,date,id)
			cur.execute(update_sql)
			
			con.commit()


	
	con.close()


	
def get_list():
	
	dic={}

	sql0="select distinct b.chname,a.full_url from (select substr(full_name,3)||'频道' ch,full_name,full_url from media.d_channel where lay=1 and full_name like '%%凤凰%%' and full_name!='凤凰网首页' and full_name!='凤凰自媒体' and full_name!='汽车频道' and full_name!='视频频道') a join web.d_cmppid_path b on a.ch=trim(b.chname)"
	
	con,cur = db_19()

	cur.execute(sql0)

	for (chname,full_url) in cur.fetchall():
		dic.setdefault(full_url,chname)
	
	con.close()

	return dic
		
	
	
	








if __name__ == '__main__':
	if 'daily' in sys.argv:
		dateStr = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400))
                print dateStr,'starts @',time.strftime('%Y-%m-%d %H:%M:%S')
                funcs = [get_data]

                for func in funcs:
                        try:
                                func(dateStr)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                                dateStr,func,'is Wrong'
                                print traceback.print_exc()
                print dateStr,'is done @',time.strftime('%Y-%m-%d %H:%M:%S')



        if 'test' in sys.argv:
                dateStr = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400))
                print dateStr,'is starts @',time.strftime('%Y-%m-%d %H:%M:%S')
                
		funcs = [get_data]
                for func in funcs:
                        try:
                                func(dateStr)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                                dateStr,func,'is Wrong'
                                print traceback.print_exc()
                print dateStr,'is done @',time.strftime('%Y-%m-%d %H:%M:%S')
        if 'bushu' in sys.argv:
                sdate = time.mktime(time.strptime('2018-03-27','%Y-%m-%d'))
                edate = time.mktime(time.strptime('2018-03-01','%Y-%m-%d'))
                while sdate >= edate:
                        dateStr = time.strftime('%Y-%m-%d',time.localtime(sdate))
                        print dateStr,'starts @',time.strftime('%Y-%m-%d %H:%M:%S')
                        
			funcs = [get_data]
                        for func in funcs:
                                func(dateStr)
                                print dateStr,func,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S')
                        sdate -= 86400
                        print dateStr,'done @',time.strftime('%Y-%m-%d %H:%M:%S')







