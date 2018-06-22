#! /usr/bin/python
#coding:utf-8
import os
import time
import cx_Oracle
import socket
import getZmtSrc
timeout = 60
socket.setdefaulttimeout(timeout)
os.environ['NLS_LANG'] = '.UTF8'
path = '/data/v2_test/GetTitle/PC/logs_test/Biz/'

def open_localhost():
        con=cx_Oracle.connect("media", "数据库密码", "数据库地址:1521/orcl")
        cur=con.cursor()
        return con,cur

def getCmsXml(start,end):
	#url = 'http://biz.icms.ifeng.com/?_c=statistic&_a=get-news-xml&startTime=%s'%(start)
	url = 'http://biz.cmpp.ifeng.com/Cmpp/runtime/interface_152.jhtml?startTime=%s&endTime=%s'%(start,end)
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
                                if len(tmp)!=14:
                                        continue
                                result.append((tmp[0],tmp[1],tmp[2],tmp[3],tmp[4],tmp[5],tmp[6],tmp[7],tmp[8].strip(),tmp[9].strip(),tmp[10].strip(),tmp[11].strip(),tmp[12].strip(),tmp[13].strip()))
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
		(id, title, editormail, source, tm1, pubtime, url,sourceLink,is_original,zmt,fhh,uri,type_art,page) = line
		if id:
			id = '_'.join(['cmpp',id])

			Page=int(page)
		
			tmp = (id,Page,type_art)
#			sql_select = "select title from web.d_doc_new where id = '%s'" % (id)
 #                       cur.execute(sql_select)
  #                      result = cur.fetchall()
                        sql_insert = "insert into web.d_doc_page_type_test(id,num,type_art) values (:1,:2,:3)"
                        try:
                                cur.execute(sql_insert,tmp)
                        except Exception,e:
				print e
				continue
        con.commit()
        con.close()
#        os.system("rm -rf /data1/logs/cms/biz/*")
def try_to_execute(cursor,sql1,tmp,sql2,count):
	try:
		if count > 5: return 0
		count = count+1
		print count
		cursor.execute(sql1,tmp)
	except Exception,e:
		print e
		cursor.execute(sql2,(tmp[0],))
		try_to_execute(cursor,sql1,tmp,sql2,count)



if __name__ == '__main__':
	time_interval = 60*10
	# 30*60 = 30 minites

	import os,time,cx_Oracle
	os.environ["NLS_LANG"] = ".UTF8"

	sdate = time.mktime(time.strptime('2018-06-01 00:00','%Y-%m-%d %H:%M'))
	edate = time.mktime(time.strptime('2018-06-21 23:50','%Y-%m-%d %H:%M'))
	while sdate <= edate:
		date = time.strftime('%Y-%m-%d',time.localtime(sdate))
		print date,'starts @',time.strftime('%Y-%m-%d %H:%M:%S')
		


		start = time.strftime("%Y-%m-%d+%H:%M:00",time.localtime(sdate))
		end = time.strftime("%Y-%m-%d+%H:%M:00",time.localtime(sdate+time_interval))
		
		insert_time= time.strftime("%Y-%m-%d %H:%M:00",time.localtime(sdate))
		
		day =  time.strftime("%Y-%m-%d",time.localtime(sdate))
	        getCmsXml(start,end)
	
		sdate += time_interval

     		os.environ["NLS_LANG"] = ".UTF8"

		start = start.replace(':','')
		xml_file = path+'%s/data_%s.xml'%(day,start)

                openDB=open_localhost()
                storeCmsDoc(xml_file)

