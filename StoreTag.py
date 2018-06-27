# -*- coding:utf8 -*-

import urllib2
import time
import os
import urllib,cx_Oracle,json

os.environ["NLS_LANG"] = ".UTF8"

from urllib2 import URLError
import sys
reload(sys)

sys.setdefaultencoding("utf-8")

def openDB():
        con=cx_Oracle.connect("app", "数据库密码", "数据库地址:1521/orcl")
        cur=con.cursor()
        return con,cur

def db_19():
        con = cx_Oracle.connect('web','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur

def db_test():
	con = cx_Oracle.connect('app','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur


def get_list(hm):
	
	article_list=[]

	pic_list=[]

	video_list=[]

	con,cur = db_19()
	

	sql_1="select id from web.d_doc_new where insert_time=to_date('%s','yyyy-mm-dd hh24:mi:ss') and type_art='article'"%hm
	
	cur.execute(sql_1)
	for (id,) in cur.fetchall():

		article_list.append(id)

	sql_2="select id from web.d_doc_new where insert_time=to_date('%s','yyyy-mm-dd hh24:mi:ss') and type_art='slide'"%hm	

	cur.execute(sql_2)
	for (id,) in cur.fetchall():
		pic_list.append(id)

	sql_3="select id,title from web.d_doc_new where insert_time=to_date('%s','yyyy-mm-dd hh24:mi:ss') and id like 'video%%'"%hm

        cur.execute(sql_3)
        for (id,title) in cur.fetchall():

                video_list.append((id,title))



	con.close()


	return article_list,pic_list,video_list
		

def get_data(hm):

        dic = {}
	result={}

	con,cur = db_19()

	article_list,pic_list,video_list=get_list(hm)

	for art in article_list:
		art_id=art.split('cmpp_')[1]

        	url = "http://local.tongji.ifeng.com/adwall/getSourceGrade?type=doc&id=%s&title=&text="%art_id
        	try:

            		html = urllib.urlopen(url).read()

        	except:

            		print "url error!"
			continue

		try :

			dic = json.loads(html)
		except :
			print art,'json loads html error!'
			continue

	
		if (len(dic["category"])==0):
			continue

		category=dic["category"]
		le=len(category)

		if (le%3==0):
			count=le/3

			for i in range(0,count):
				
				
				word=category[i*3]
				cate=category[i*3+1]
				rate=category[i*3+2]
				try:
					in_sql="insert into web.d_doc_tag (id,word,cate,rate,tm) values (:1,:2,:3,:4,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%hm
					cur.execute(in_sql,(art,word,cate,rate))
					con.commit()
				except:
					print art,'insert error!'


		else :
			count=int(le/3)
			for i in range(0,count):
				word=category[i*3]
				cate=category[i*3+1]
				rate=category[i*3+2]

				try:
                                        in_sql="insert into web.d_doc_tag (id,word,cate,rate,tm) values (:1,:2,:3,:4,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%hm
                                        cur.execute(in_sql,(art,word,cate,rate))
                                        con.commit()
                                except:
                                        print art,'insert error!'






	for pic in pic_list:
		pic_id=pic.split('cmpp_')[1]

		url = "http://local.tongji.ifeng.com/adwall/getSourceGrade?type=pic&id=%s&title=&text="%pic_id
		
		try:

            		html = urllib.urlopen(url).read()

        	except:

            		print "url error!"

			continue
		try :
			dic = json.loads(html)

		except:
			print pic,'json loads html error!'
			continue	
		
		if (len(dic["category"])==0):
			continue
		category=dic["category"]
		le=len(category)
		if (le%3==0):
			count=le/3

			for i in range(0,count):
								
				word=category[i*3]
				cate=category[i*3+1]
				rate=category[i*3+2]
				try:
					in_sql="insert into web.d_doc_tag (id,word,cate,rate,tm) values (:1,:2,:3,:4,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%hm
					cur.execute(in_sql,(pic,word,cate,rate))
					con.commit()
					print pic,'pic'
				except:
					print pic,'insert error!'

		else :
			count=int(le/3)
			for i in range(0,count):
				word=category[i*3]
				cate=category[i*3+1]
				rate=category[i*3+2]

				try:
					in_sql="insert into web.d_doc_tag (id,word,cate,rate,tm) values (:1,:2,:3,:4,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%hm
					cur.execute(in_sql,(pic,word,cate,rate))
					con.commit()
					print pic,'pic'
            			except:
					print pic,'insert error!'

	for (video,title) in video_list:

                video_id=video.split('video_')[1]

#		title_en=urllib.quote(title.decode(sys.stdin.encoding).encode('utf8'))

		title_en = urllib.quote(title)

                url = "http://local.tongji.ifeng.com/adwall/getSourceGrade?type=video&id=%s&title=%s&text="%(video_id,title_en)


                try:

                        html = urllib.urlopen(url).read()


                except:

                        print "url error!"

                        continue
		try:


                	dic = json.loads(html)
		except:
			print video,'json loads html error!'
			continue
		
                if (len(dic["category"])==0):

                        continue
                category=dic["category"]
                le=len(category)
		
		if (le%3==0):
                        count=le/3

                        for i in range(0,count):


                                word=category[i*3]
                                cate=category[i*3+1]
                                rate=category[i*3+2]
                                try:
                                        in_sql="insert into web.d_doc_tag (id,word,cate,rate,tm) values (:1,:2,:3,:4,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%hm
                                        cur.execute(in_sql,(video,word,cate,rate))
                                        con.commit()
                                except:
                                        print video,'insert error!'


		else :
                	count=int(le/3)
                	for i in range(0,count):
                        	word=category[i*3]
                        	cate=category[i*3+1]
                        	rate=category[i*3+2]

                        	try:
                                        in_sql="insert into web.d_doc_tag (id,word,cate,rate,tm) values (:1,:2,:3,:4,to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%hm
                                        cur.execute(in_sql,(video,word,cate,rate))
                                        con.commit()
                                except:
                                        print video,'insert error!'
		
		
		




        con.close()

if __name__ == '__main__':

	delay=25*60
	start = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	print start,'start-----'

	hm = time.strftime("%Y-%m-%d %H:%M:00",time.localtime(time.time()-delay))	

	print hm

        get_data(hm)

	end = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
	print end,'end-----'

