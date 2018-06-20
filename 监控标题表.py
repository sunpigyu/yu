#! /usr/bin/python
#coding:utf-8
import os
import time
import cx_Oracle
import socket
import glob
import getZmtSrc
import smtplib
from email.Header import Header
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import email.MIMEBase
import email


timeout = 60
socket.setdefaulttimeout(timeout)
os.environ['NLS_LANG'] = '.UTF8'



dic_url={'Biz':'http://biz.cmpp.ifeng.com/Cmpp/runtime/interface_152.jhtml?startTime=%s&endTime=%s','Book':'http://book.cmpp.ifeng.com/Cmpp/runtime/interface_15413.jhtml?startTime=%s','Ent':'http://ent.cmpp.ifeng.com/Cmpp/runtime/interface_82.jhtml?startTime=%s&endTime=%s','Fashion':'http://fashion.cmpp.ifeng.com/Cmpp/runtime/interface_89.jhtml?startTime=%s&endTime=%s','Finance':'http://finance.cmpp.ifeng.com/Cmpp/runtime/interface_381.jhtml?startTime=%s&endTime=%s','House':'http://house.ifeng.com/news/api/statistics?token=a6f54e0b9dba367c3ec582a44afb8a3e&startTime=%s','HouseSale':'http://house.ifeng.com/sale/tongji/index?token=a6f54e0b9dba367c3ec582a44afb8a3e&startTime=%s','Music':'http://ent.cmpp.ifeng.com/Cmpp/runtime/interface_316.jhtml?start=%s','News':'http://news.cmpp.ifeng.com/Cmpp/runtime/interface_93.jhtml?startTime=%s&endTime=%s','O':'http://o.cmpp.ifeng.com/Cmpp/runtime/interface_93.jhtml?startTime=%s&endTime=%s','V':'http://v.cmpp.ifeng.com/Cmpp/runtime/interface_542.jhtml?startTime=%s','V2':'http://v.cmpp.ifeng.com/Cmpp/runtime/interface_538.jhtml?startTime=%s','Car':'http://cms.data.auto.ifeng.com/api/doc_list.php?startTime=%s','CarNew':'http://cms.data.auto.ifeng.com/api/doc_list.php?startTime=%s','Icms':'http://c1.icms.ifeng.com/?_c=statistic&_a=get-news-xml&startTime=%s'}


def db_19():
        con = cx_Oracle.connect('web','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur

def open_localhost():
        con=cx_Oracle.connect("media", "数据库密码", "数据库地址:1521/orcl")
        cur=con.cursor()
        return con,cur

def getCmsXml(start,end,url,path,day):
        #url = 'http://biz.icms.ifeng.com/?_c=statistic&_a=get-news-xml&startTime=%s'%(start)
        os.chdir(path)
#        day =  time.strftime("%Y-%m-%d",time.localtime(time.time()-time_interval))
        if not os.path.exists('%s'%day):
                os.system('mkdir %s'%day)
        os.chdir(path+'%s/'%day)
        start = start.replace(':','')
        os.system("wget --output-document=data_%s.xml '%s' "%(start,url))



def getCmsDoc(xml_file):
        result = []
        import xml.dom.minidom,glob
        try:
                        dom = xml.dom.minidom.parse(xml_file)
                        items = dom.getElementsByTagName("doc")
                        for item in items:
                                tmp = []
                                for n in item.childNodes:
                                        if n.nodeType not in (n.ELEMENT_NODE, n.CDATA_SECTION_NODE):
                                                continue
                                        if n.firstChild and n.firstChild.data:
                                                tmp.append(n.firstChild.data)
                                        else:
                                                tmp.append('')
                                if len(tmp)!=12:
                                        continue
                                result.append((tmp[0],tmp[1],tmp[2],tmp[3],tmp[4],tmp[5],tmp[6],tmp[7],tmp[8].strip(),tmp[9].strip(),tmp[10].strip(),tmp[11].strip()))
        except Exception,e:
                        print e

        return result


def ETL_source(source):
        if u'《' in source :
                source = source.split(u'《')[1]
                if u'》' in source:
                        source = source.split(u'》')[0]
                #       print source.strip()
                        return source.strip().encode('utf8')
                else:
                #       print source.strip()
                        return source.strip().encode('utf8')
        else:
                return source.encode('utf8')

def getdomain3(url):
        list = ['news.ifeng.com/history/','news.ifeng.com/mil/','news.ifeng.com/opinion/','news.ifeng.com/sports/']
        passlist = ['stadig.ifeng.com/']
        for key in list:
                if key in url:
                        return key[:-1]

        for key in passlist:
                if key in url:
                        return '#'

        tmp = url.split('/')
        if len(tmp) < 3:
                return '#'
        domain = tmp[2].split('.')
        if len(domain) < 4:
                return tmp[2]
        if not ('ifeng.' in tmp[2] or 'phoenixtv.com' in tmp[2]):
                        site = tmp[2]
                        if '.qzone.qq.com' in site:
                                site = 'qzone.qq.com'
                        if 'mail.' in site:
                                t = site.split('.')
                                if len(t) >= 3 and 'mail' in t[-3]:
                                        site = t[-3]+'.'+t[-2]+'.'+t[-1]
                                elif len(t) >= 4 and 'mail' in t[-4]:
                                        site = t[-4]+'.'+t[-3]+'.'+t[-2]+'.'+t[-1]
                        return site

        return domain[-3] + '.' + domain[-2] + '.' + domain[-1]




def storeCmsDoc(xml_file):
        result = getCmsDoc(xml_file)
        con,cur = openDB
        count = 0
        for line in result:
                (id, title, editormail, source, tm1, pubtime, url,sourceLink,is_original,zmt,fhh,uri) = line
                if id:
                        id = '_'.join(['cmpp',id])
                        if not title or not url:
                                continue
                        if not editormail:
                                editormail = ""
                        source = ETL_source(source)
                        if not source:
                                source = getZmtSrc.getSrc(zmt)
                        if sourceLink:
                                sourceLink = getdomain3(sourceLink)
                        if not sourceLink:
                                sourceLink = ""
                        if pubtime and len(pubtime) > 20:
                                pubtime = pubtime[0:10]+''+pubtime[11:19]
                        Is_original = int(is_original)
                        if Is_original !=1:
                                Is_original = 0
                        tmp = (id,url.encode('utf8'),title.encode('utf8'),source,pubtime.encode('utf8'),editormail.encode('utf8'),sourceLink.encode('utf8'),Is_original,zmt.encode('utf8'),fhh.encode('utf8'),uri.encode('utf8'))
#                       sql_select = "select title from web.d_doc_new where id = '%s'" % (id)
 #                       cur.execute(sql_select)
  #                      result = cur.fetchall()
                        sql_insert = "insert into web.d_doc_new(id,ch_id,url_pc,title,src,tm,type,editor_mail,source_link,is_original,zmt,fhh,url_iifeng,insert_time) values (:1,0,:2,:3,:4,to_date(:5,'yyyy-mm-dd hh24:mi:ss'),7,:6,:7,:8,:9,:10,:11,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%insert_time
                        try:
                                cur.execute(sql_insert,tmp)
                        except Exception,e:
                                print e
                                continue
                                up_sql = "update web.d_doc_new set url_iifeng = :1,url_pc=:2 where id = :3"
                                try:
                                        cur.execute(up_sql,(tmp[10],tmp[1],tmp[0]))
                                except Exception,e:
                                        print e,id,title
        con.commit()
        con.close()







def get_list(date):
	
	dir_path='/data/logs/GetTitle/PC/'
	
	list_end_1=[]
	list_end_2=[]
	
	dir_list_all_1=[]
	dir_list_all_2=[]

	dir_list1=['Biz/','Book/','Ent/','Fashion/','Finance/','House/','HouseSale/','Music/','News/','O/','V/','V2/']
	dir_list2=['Car/','CarNew/','Icms/']

	list_data=[]

	lines = open('/data/logs/GetTitle/PC/timelog.txt','r')

	for line in lines:
		
		list_data.append(line.strip())

	lines.close()	
	

	for dir1 in dir_list1:
		dir_list_all_1.append(dir_path+dir1)
	
	for dir2 in dir_list2:
                dir_list_all_2.append(dir_path+dir2)



	for dir in dir_list_all_1:
		list1=glob.glob(dir+date+'/*.xml')
		list1.sort()

		if (len(list1)==1440):
			continue
		else:
			for list in list_data:
				data_time=dir+date+'/data_'+date+'+'+list+'.xml'
				if data_time in list1:
					continue
				else:
					list_end_1.append(data_time)
						


	for dir in dir_list_all_2:

		list2=glob.glob(dir+date+'/*.xml')
		list2.sort()
		
		if (len(list2)==1440):
			continue
                else:
                        for list in list_data:
				list_t=list[:-2]
                                data_time=dir+date+'/data_'+date+'+'+list_t+'.xml'
                                if data_time in list2:
                                        continue
                                else:
                                        list_end_2.append(data_time) 

	

	return list_end_1,list_end_2



def store_list(date):

	xml_list=[]

	dir_path='/data/logs/GetTitle/PC/'

	list1,list2=get_list(date)

	openDB=open_localhost()

	for list in list1:

		path=list.split(date)[0]
		
		start=list.split('+')[1].split('.xml')[0]

		start_time=start[0:2]+':'+start[2:4]+':'+start[4:6]
		
		start_new=date+'+'+start_time

		end_new=time.strftime('%Y-%m-%d+%H:%M:%S',time.localtime(time.mktime(time.strptime(start_new,'%Y-%m-%d+%H:%M:%S'))+660))

		insert_time=date+' '+start_time

		dir_ch=list.split(date)[0].split("/")[-2]
		
		if (dir_ch=='News'):
			end_new=time.strftime('%Y-%m-%d+%H:%M:%S',time.localtime(time.mktime(time.strptime(start_new,'%Y-%m-%d+%H:%M:%S'))+360))
			

		url_tmp=dic_url[dir_ch]
		
		if ('endTime' in url_tmp):
			url=url_tmp%(start_new,end_new)

		else:
			url=url_tmp%(start_new)

		getCmsXml(start_new,end_new,url,path,date)

		start_newnew = start_new.replace(':','')
		xml_file = path+'%s/data_%s.xml'%(date,start_newnew)
	
		xml_list.append(xml_file)	
		print xml_file,'missing!'

		

	for list in list2:

		path=list.split(date)[0]

		start=list.split('+')[1].split('.xml')[0]

                start_time=start[0:2]+':'+start[2:4]

                start_new=date+'+'+start_time

                end_new=time.strftime('%Y-%m-%d+%H:%M',time.localtime(time.mktime(time.strptime(start_new,'%Y-%m-%d+%H:%M'))+660))

                insert_time=date+' '+start_time+':00'

		dir_ch=list.split(date)[0].split("/")[-2]

                url_tmp=dic_url[dic_ch]

                if ('endTime' in url_tmp):
                        url=url_tmp%(start_new,end_new)

                else:
                        url=url_tmp%(start_new)
		


                getCmsXml(start_new,end_new,url,path,date)

                start_newnew = start_new.replace(':','')
                xml_file = path+'%s/data_%s.xml'%(date,start_newnew)

		xml_list.append(xml_file)
		print xml_file,'missing!'

			
	con,cur=openDB

	con.close()

	return xml_list		



def sendmail(sender,mail_list,msg):
        server = smtplib.SMTP('mail.staff.ifeng.com')
        con,cur = db_19()
        sql = "select uname,pw from webadmin.f_mail_pw where uname = 'tongji'"
        cur.execute(sql)
        uname,pwd = cur.fetchone()
        con.close()
        server.login(uname,pwd)
        server.sendmail(sender, mail_list, msg)
        server.quit()


def getMailcontent(subject,text,attachfile=''):
        main_msg = MIMEMultipart()
        file_name = attachfile

#        text_msg = MIMEText(text)
        text_msg = MIMEText(text,'plain','utf-8')
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



def run(xml_list,date):
	if (len(xml_list)==0):
		print date,'title dir is OK!'

	else :
		text='以下为标题表采集缺失的文件列表：\n\n'
		t=''
		for xml in xml_list:
			t=t+xml+'\n'
		text=text+t
		subject="标题表下载文件缺失 %s"%date
		sender ="tongji@ifeng.com"
		mail_list=['yutw1@ifeng.com']
		contact_text="  \n\n凤凰统计http://tongji.ifeng.com/ \
                        \n\n有疑问请联系技术部数据分析研发组 于天威 yutw1@ifeng.com\n\n"
		msg=getMailcontent(subject,text+contact_text)

		sendmail(sender,mail_list,msg)
		
		



	

if __name__ == '__main__':

	date = time.strftime('%Y-%m-%d',time.localtime(time.time()-86400*1))
	print date



	xml_list=store_list(date)

	print xml_list

	run(xml_list,date)
	print date,'lisen title end!'



	
	
