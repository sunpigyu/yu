#encoding:utf-8
import time, glob,gzip
import os,urllib, re,sys
import datetime
import random
import db,cx_Oracle
import smtplib
from email.Header import Header
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import email.MIMEBase
import email
import db
os.environ['NLS_LANG'] = '.UTF8'

def openDB():
        con=cx_Oracle.connect("app","数据库密码","数据库地址:1521/orcl")
        cur=con.cursor()
        return con,cur

def sendmail(sender,mail_list,msg):
        server = smtplib.SMTP('mail.staff.ifeng.com')
        con,cur = db.open_media()
        sql = "select uname,pw from webadmin.f_mail_pw where uname = '邮箱用户名'"
        cur.execute(sql)
        uname,pwd = cur.fetchone()
        con.close()
        server.login(uname,pwd)
        server.sendmail(sender, mail_list, msg)
        server.quit()

def getMailcontent(subject,text,attachfile=''):
        main_msg = MIMEMultipart()
        file_name = attachfile

        text_msg = MIMEText(text)
        main_msg.attach(text_msg)

        if not file_name == '':
                contype = 'application/octet-stream'
                maintype, subtype = contype.split('/', 1)

                data = open(file_name, 'rb')
                file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
                file_msg.set_payload(data.read( ))
                data.close( )
                email.Encoders.encode_base64(file_msg)

                basename = os.path.basename(file_name)
                file_msg.add_header('Content-Disposition','attachment', filename = basename)
                main_msg.attach(file_msg)

        main_msg['subject'] = subject
        return main_msg.as_string()

def getMailcontent2(subject,text,attachfile=['']):
        main_msg = MIMEMultipart()
        file_list = attachfile

        text_msg = MIMEText(text)
        main_msg.attach(text_msg)

        for file_name in file_list:
                if file_name == '':
                        continue
                contype = 'application/octet-stream'
                maintype, subtype = contype.split('/', 1)

                data = open(file_name, 'rb')
                file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
                file_msg.set_payload(data.read( ))
                data.close( )
                email.Encoders.encode_base64(file_msg)

                basename = os.path.basename(file_name)
                file_msg.add_header('Content-Disposition','attachment', filename = basename)
                main_msg.attach(file_msg)

        main_msg['subject'] = subject
        return main_msg.as_string()


def get_data(date):

	list=['http://toujiao.ifeng.com/class/detail?pid=004&ct=3','http://toujiao.ifeng.com/class/detail?pid=008&ct=88','http://toujiao.ifeng.com/class/detail?pid=004&ct=1','http://toujiao.ifeng.com/class/detail?pid=004&ct=8','http://toujiao.ifeng.com/class/play?pid=004&ct=0','http://toujiao.ifeng.com/class/play?pid=009&ct=0','http://toujiao.ifeng.com/expertbbs/detail','http://toujiao.ifeng.com/expert/detail']

	dic={}

	path='/data1/logs/web/%s/*.sta.gz'%date
		
	dir=glob.glob(path)
	dir_new=sorted(dir)

	for data in dir_new:
		for line in gzip.open(data):
			ss  = line.split('\t')
			url=ss[0].strip()
			uid=ss[3].strip()

			if 'http://toujiao.ifeng.com/' not in url:
				continue
			if ('pid=' in url and 'ct=' in url):
				
				url_s1=url.split('?')[0]+'?'+url.split('&')[1]+'&'+url.split('&')[2]

				if url_s1 in list:
					dic.setdefault(url_s1,[0,set()])
					dic[url_s1][0] += 1
					dic[url_s1][1].add(uid)
					

			else:
				
				url_s1=url.split('?')[0]
				
				if url_s1 in list:

					dic.setdefault(url_s1,[0,set()])
                                        dic[url_s1][0] += 1
                                        dic[url_s1][1].add(uid)
			
	for d in dic:
		url=d
		pv=dic[d][0]
		uv=len(dic[d][1])
		print url,pv,uv	
	
	return dic


def get_toujiao(date):

	dic_list={'http://toujiao.ifeng.com/expertbbs/detail':'砖家谈','http://toujiao.ifeng.com/class/detail?pid=004&ct=1':'5元课','http://toujiao.ifeng.com/class/detail?pid=004&ct=3':'提高班','http://toujiao.ifeng.com/expert/detail':'老师主页','http://toujiao.ifeng.com/class/detail?pid=008&ct=88':'实战课','http://toujiao.ifeng.com/class/play?pid=004&ct=0':'免费课','http://toujiao.ifeng.com/class/play?pid=009&ct=0':'大咖课','http://toujiao.ifeng.com/class/detail?pid=004&ct=8':'优惠学'}


	dic={}	

	result = []
	line1a = '\xe7\xb1\xbb\xe5\x9e\x8b'.decode('utf-8')  #类型
        line1b = 'pv'
        line1c = 'uv'
	#line1c = u'自媒体文章数'
        line1d = '\xc8\xd5\xc6\xda'.decode('gbk') #日期
	#line1d = u'自媒体文章占比'
	result.append((line1a,line1b,line1c,line1d))
	#date1 = time.strftime('%Y-%m-%d',time.localtime(time.mktime(time.strptime(date,"%Y-%m-%d"))-86400))
	print date

	

	dic=get_data(date)

	for d in dic:
		type=dic_list[d]
		pv=dic[d][0]
		uv=len(dic[d][1])
		line=(type,pv,uv,date)
		result.append(line)

        list=sorted(result,key=lambda x:x[1],reverse=True)
        result=list
#        print result
        return result


def toujiao(date):
        toujiao_name='\xe5\x87\xa4\xe5\x87\xb0\xe6\x8a\x95\xe6\x95\x99\xe5\xad\x90\xe9\xa1\xb5\xe9\x9d\xa2\xe6\xb5\x81\xe9\x87\x8f'#凤凰投教子页面流量
        toujiao_stat=get_toujiao(date)
	if len(toujiao_stat) == 1 :
                print 'empty!!!'
                sys.exit()
        result={toujiao_name:toujiao_stat}
#	print result
        text=""
        subject="凤凰投教子页面流量数据"
        sender ="tongji@ifeng.com"

#        mail_list=['yufei1@ifeng.com', 'panfeng@ifeng.com', 'caizw@ifeng.com', 'wuchenguang@yidian-inc.com','xuefeng@yidian-inc.com','zhaochenguang@yidian-inc.com', 'zouming@ifeng.com', 'jiangyan@ifeng.com', 'hewx1@ifeng.com', 'yuhao@ifeng.com', 'hujn@ifeng.com', 'shibing@ifeng.com', 'yangty@ifeng.com', 'yanff@ifeng.com', 'baojuan@ifeng.com', 'xuyue1@ifeng.com', 'shaocong@ifeng.com', 'cuimc@ifeng.com', 'yangle@ifeng.com', 'gaomy@ifeng.com', 'zhanglin3@ifeng.com', 'luhui@ifeng.com', 'chengws@ifeng.com', 'sunxm@ifeng.com','yangyu1@ifeng.com','chenyk@ifeng.com'] 

        mail_list=['yutw1@ifeng.com'] 
#        mail_list=['liuyi5@ifeng.com']
        filelist='/data/dz/special/toujiao/凤凰投教子页面流量数据%s.xls'%(date)
        import yifileutil_yu
        yifileutil_yu.obj2excel(filelist,result)
#	print yifileutil.obj2excel(filelist,result)
        contact_text="\n\n具体数据请见附件\n\n \
			\n\n凤凰统计http://tongji.ifeng.com/ \
			\n\n有疑问请联系技术部数据分析研发组 于天威 yutw1@ifeng.com\n\n"
	
        msg=getMailcontent(subject,text+contact_text,filelist)
        sendmail(sender,mail_list,msg)
        return 'm'
	
if __name__ == '__main__':
        os.environ["NLS_LANG"] = ".utf8"
        os.putenv('ORACLE_HOME','/usr/lib/oracle/11.2/client64/bin')
        os.putenv('LD_LIBRARY_PATH', '/usr/lib/oracle/11.2/client64/lib')
        date = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*1))

	start = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	print start,'start------------'

        toujiao(date)

#	get_data(date)
	print date,'is Done'

	end = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	print end,'end-----'

