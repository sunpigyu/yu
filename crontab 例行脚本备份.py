#! /usr/bin/python
# encoding: utf8
import sys,os,time,cx_Oracle
import urllib2,urllib
import json
import re

reload(sys)
sys.setdefaultencoding('utf8')
os.environ["NLS_LANG"] = ".utf8"

#server_ip = os.popen('hostname -i').read().replace('\n','')
hdfs_nn_online='hdfs地址'
hdfs_nn_standby='hdfs地址'

def backups_cron_script():
	#脚本开始时间
	t = time.time()
        date = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t))
	print "开始备份：%s" %(date)

	#获取例行信息
	os.system('crontab -l > ./crontab.log')
	file = './crontab.log'

	#确定HDFS_ONLINE_SERVER_IP
	hdfs_server_ip=hdfs_nn_online
        is_online=os.system("hadoop fs -test -e 'hdfs://%s:8020/user/hadoop/'"%(hdfs_nn_online))
        if is_online !=0:
                hdfs_server_ip=hdfs_nn_standby
	print "在线HDFS-Server: %s" %(hdfs_server_ip)
	
	#查看hdfs文件夹是否存在，不存在就创建
        server_ip = os.popen('hostname -i').read().replace('\n','').split()[0]
        folder_exits = os.system("hadoop fs -test -e 'hdfs://%s:8020/user/hadoop/tongji_script/%s'" %(hdfs_server_ip,server_ip))
        if folder_exits != 0:
        	os.system("hadoop fs -mkdir 'hdfs://%s:8020/user/hadoop/tongji_script/%s'" %(hdfs_server_ip,server_ip))
        	print "'hdfs://%s:8020/user/hadoop/tongji_script/%s' has been create" %(hdfs_server_ip,server_ip)
	else:
		print "'hdfs://%s:8020/user/hadoop/tongji_script/%s' is already exists" %(hdfs_server_ip,server_ip)

	#遍历每行获取脚本路径并上传至HDFS指定路径
	for line in open(file):
		if '#' in line or '=' in line:
			continue
		s1 = re.split('\s|>>|2>&1',line)
		if len(s1) < 7:
			continue
		while '' in s1:
			s1.remove('')
		#print s1
		minute = s1[0]
		hour = s1[1]
		day = s1[2]
		month = s1[3]
		weekly = s1[4]
		script = s1[6]
		if len(s1) == 7:
			parameter = ' '
			logs = ' '
		elif len(s1) == 8:
			if '.err' in s1[7]:
				parameter = ' '
	                        logs = s1[7]
			else:
				parameter = s1[7]
				logs = ' '
		else:
			parameter = s1[7]
			logs = s1[8]
		regular = month + '-' + weekly + '-' + day + '-' + hour + '-' + minute
#		print server_ip,script,parameter,logs,regular
		
		#复制脚本文件到指定路径
		print "hadoop fs -put -f %s 'hdfs://%s:8020/user/hadoop/tongji_script/%s'" %(script,hdfs_server_ip,server_ip)
        	os.system("hadoop fs -put -f %s 'hdfs://%s:8020/user/hadoop/tongji_script/%s'" %(script,hdfs_server_ip,server_ip))
	print "hadoop fs -put -f %s 'hdfs://%s:8020/user/hadoop/tongji_script/%s'" %(file,hdfs_server_ip,server_ip)
	os.system("hadoop fs -put -f %s 'hdfs://%s:8020/user/hadoop/tongji_script/%s'" %(file,hdfs_server_ip,server_ip))
	print "结束备份：%s" %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
		
			



if __name__ == '__main__':
	#例行脚本备份至HDFS
	backups_cron_script()



