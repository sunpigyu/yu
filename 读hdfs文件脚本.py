#! /usr/bin/env python
#!-*-coding:utf-8 -*-
import os
import time
import cx_Oracle
from hdfs.client import Client

os.environ['NLS_LANG'] = '.UTF8'

def db_192():
	con = cx_Oracle.connect('web','数据库密码','数据库地址:1521/orcl')
	cur = con.cursor()
	return con,cur
def fhhSet():
#        file = open('/data/logs/newsapp/audit_json/' + yesterday + '/wemedia_account.json', 'r')

	lines = []

#	client = Client(url, root=None, proxy=None, timeout=None, session=None) 
	client = Client("http://10.90.7.168:50070")

	

	with client.read("/user/hadoop/audit_json/%s/result/wemedia_account.json/part-00000"%yesterday, encoding='utf-8', delimiter='\n') as reader:
		for line in reader:
#			print line
			lines.append(line.strip())

        fset = set()
        for line in lines:
                try:
                        oid = line.split('"_id":{"$oid":"')[1].split('"}')[0]
                        if '"accountType":' in line:
                                type = line.split('"accountType":')[1].split(',')[0]
                                if type in ['1', '2']:
                                        fset.add(oid)
                except:
                        pass
        return fset
def audit(yesterday):
#        file = open('/data/logs/newsapp/audit_json/' + yesterday + '/article.json', 'r')

	client = Client("http://10.90.7.168:50070")

	adict = {}
        fset = fhhSet()
	
	for dir_path in os.popen("hdfs dfs -ls /user/hadoop/audit_json/%s/result/article.json| awk '{print $8}'"%yesterday).readlines():
		dir_path =  dir_path.strip()
		try:
		  with client.read(dir_path, encoding='utf-8', delimiter='\n') as reader:
			for line in reader:
                		try:
                        		wemedia = line.split('"weMediaId":{"$oid":"')[1].split('"}')[0]
                        		if wemedia not in fset:
                                		continue

                        		date = line.split('"auditTime":{"$date":"')[1].split('T')[0]
                        		if date not in adict:
                                		adict[date] = [0 for i in range(0,11)]
                        		op = line.split('"operationStatus":')[1].split(',')[0]
                        		op = int(op)
                        		if op > 11:
                                		continue
                        		op = op -1
                        		adict[date][op] = adict[date][op] + 1
                		except:
                        		pass				
		except:
			pass
#		with client.read("/user/hadoop/audit_json/%s/result/article.json/part-%d"%yesterday, encoding='utf-8', delimiter='\n') as reader:

        dateset = set()
        con,cur = db_192()
        for date in adict:
                alist = ''
                for a in adict[date]:
                        if date not in datelist:
                                continue
                        if date in dateset:
                                continue
                        try:
                                online_all=int(adict[date][3])+int(adict[date][8])
				admin_online=adict[date][3]
				admin_out=adict[date][5]
				sys_online=adict[date][8]
				sys_out=adict[date][9]
				user_out=adict[date][4]
                                print date, '\t', int(adict[date][3])+int(adict[date][8]),'\t',adict[date][3],'\t',adict[date][5],'\t',adict[date][8],'\t',adict[date][9],'\t',adict[date][4]
                                dateset.add(date)
                                cur.execute("delete from web.f_zmt_onlinedayly_new where tm = to_date('%s','yyyy-mm-dd')"%date)
                                con.commit()
                                sql = "insert into web.f_zmt_onlinedayly_new values(to_date(:1,'yyyy-mm-dd'),:2,:3,:4,:5,:6,:7)"
				cur.execute(sql,(date,online_all,admin_online,admin_out,sys_online,sys_out,user_out))	
				con.commit()

                        except Exception,e:
                        	print e
        con.close()




			


if __name__ == '__main__':
        #t = time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time()-86400*1))
	yesterday = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*1))
	
	#print t
        datelist = map(lambda x: time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*x)), range(1, 2))
        print yesterday
        print datelist

        print '上线下线数据'

#	fhhSet()
        audit(yesterday)
	#t1 = time.strftime('%Y-%m-%d-%H:%M:%S',time.localtime(time.time()-86400*1))
	#print t1
