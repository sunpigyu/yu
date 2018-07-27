import time
import db
def getMaxDayFromRt_index_doc_ch():
	con,cur = db.open_15()
	try:
		cur.execute("select to_char(max(day),'yyyy-mm-dd hh24:mi:ss') from web.rt_index_doc_ch")
		obj = cur.fetchone()
		day = obj[0]
	except Exception,e:
		print e
		day = time.strftime('%Y-%m-%d %H:%M:00',time.localtime(time.time()-300))
		pass
	con.close()
	return day
	
def getMaxDayFromRt_wap_doc_ch():
	con,cur = db.open_15()
	try:
		cur.execute("select to_char(max(day),'yyyy-mm-dd hh24:mi:ss') from wapnew.rt_index_doc_zhishu")
		obj = cur.fetchone()
		day = obj[0]
	except Exception,e:
		print e
		day = time.strftime('%Y-%m-%d %H:%M:00',time.localtime(time.time()-400))
		pass
	con.close()
	return day

if __name__=="__main__":
	day = getMaxDayFromRt_index_doc_ch()
	print day
	

