#coding:utf-8

import time
import os
import sys
import cx_Oracle
os.environ['NLS_LANG'] = '.UTF8'

def db_192():
        con = cx_Oracle.connect('web','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur

def store():

	con,cur = db_192()
	
	sql="select aa.id,aa.num,aa.type_art from (select distinct * from web.d_doc_page_type_test)aa join web.d_doc_new bb on aa.id=bb.id"

	cur.execute(sql)

	for (id,num,type_art) in cur.fetchall():
		print id

		if ('video_' in id):
			
			try:

				sql_up_1="update web.d_doc_new set num = %d where id = '%s'"%(num,id)

				cur.execute(sql_up_1)
				con.commit()
			except:
				print id,'error!','video----'

				continue
		

		else:
			try:
		

                                sql_up_2="update web.d_doc_new set num = %d,type_art='%s' where id = '%s'"%(num,type_art,id)

                                cur.execute(sql_up_2)
                                con.commit()
                        except:
                                print id,'error!','cmpp----'

                                continue


	con.close()


if __name__ == '__main__':

	store()


