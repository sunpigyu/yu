# /user/bin/env python
# coding:utf-8
import sys,glob,gzip
import time
import os


def get_data(day,hm,day_h,h):

	if (os.path.exists('/data5/iyc_logs/%s'%day)==False):
		
		os.system("mkdir /data5/iyc_logs/%s"%day)

	path="/data5/iyc_logs/%s/%s.log"%(day,day_h)
	with open(path,'a') as f:
		for data in glob.glob('/data1/logs/wap_new/%s/%s.sta.gz'%(day,hm)):
			for line in gzip.open(data):
			
				ss  = line.split('\t')
				if len(ss)<5:
					continue
				url=ss[2]
				uid=ss[4]
				ref=ss[3]
				tm=ss[9]
				if('http://iyc.ifeng.com/' in url):
					
					f.write("%s\t%s\t%s\t%s\n"%(url,ref,uid,tm))

	f.close()


if __name__ == '__main__':


	delay = 5 * 60
	t = time.time()-delay
	day = time.strftime("%Y-%m-%d", time.localtime(t))
	hm = time.strftime('%H%M',time.localtime(t))
	day_h = time.strftime('%Y-%m-%d-%H',time.localtime(t))
	h = time.strftime('%H',time.localtime(t))
	print day,hm
	try :

		get_data(day,hm,day_h,h)


	except Exception,e:
		import traceback
		print traceback.print_exc()
		print day,hm,'Wrong!',e


	
			
