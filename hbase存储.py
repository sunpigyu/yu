#coding=utf-8
import subprocess
import httplib
from thrift import Thrift
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from pyhs2.error import Pyhs2Exception
from hbase import Hbase
from hbase.ttypes import *
import cx_Oracle,time,os,time,sys,random
os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'

def open_24():
        con=cx_Oracle.connect("web", "数据库名称", "数据库地址:1521/orcl")
        cur=con.cursor()
        return con,cur
'''
con,cur = open_24()
sql="select * from web.f_Doc where day=to_date('2014-10-01','yyyy-mm-dd') and title is not null"
cur.execute(sql)
transport = TSocket.TSocket('10.32.21.35', '9090')
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()
print client.getTableNames()
for item in cur:
        rowkey=item[2]+'~'+'2014-10-01'
	info=str(item[1])+'#'+str(item[4])+'#'+str(item[5])
	mutations = [Mutation(column='content:info', value=info)] 
	client.mutateRow('tongji_doc_test',rowkey,mutations,None)
transport.open()
'''
def decoding(s0):
    cl = ['utf-8', 'gbk', 'gb18030']
    #cl = ['gbk']
    for a in cl:
        try:
            return s0.decode(a).encode('utf-8')
        except:
            pass
    return s0

def get_transport():
	
	ip_list=['hbase地址1','hbase地址2','hbase地址3','hbase地址4']
	for ip in ip_list:
		try:
			transport = TSocket.TSocket(ip, '9090')
			return transport
			
		except:
			continue


def hbase_input_doc2():
	con,cur = open_24()
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
	print 'aaa'
        for i in range(1050,1200):
		n=0
		count=0
		array=[]
                dateStr = time.strftime("%Y-%m-%d",time.localtime(time.time()-86400*i))
                print dateStr
                sql="select * from web.f_Doc where day=to_date('%s','yyyy-mm-dd') "%dateStr
                cur.execute(sql)
                for item in cur:
                    try:
                        rowkey=item[2]+'~'+dateStr
                        title=str(item[3])
                        #info=str(item[1])+'#'+title.decode('utf-8')+'#'+str(item[4])+'#'+str(item[5])
                        info=str(item[1])+'#'+title+'#'+str(item[4])+'#'+str(item[5])
			mutations = [Mutation(column='content:info', value=info)]
                        bm=BatchMutation(rowkey,mutations)
			array.append(bm)
			n=n+1
			if n>=10000:
                                client.mutateRows('tongji_doc_test',array,None)
                                array=[]
                                n=0
                                print count
			count=count+1
			#print 'aaa'
                    except:
                        continue
		print len(array)
		client.mutateRows('tongji_doc_test',array,None)
        transport.close()



def hbase_input_doc_ch():
	import hiveserver
	transport,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10004)
	clienta.execute("select * from hbase_ch")
	transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
	count=0
	while 1:
		try:
                	line = clienta.fetchOne()
			tmp=line.split('\t')
			#print tmp
			doc,pv,uv,ci,id=tmp[0],tmp[1],int(tmp[2]),tmp[3],tmp[4]
			rank=100000000-uv
			rowkey='2014-12-03'+'~'+id+'~'+str(rank)
			#print rowkey
			info=doc+'#'+str(uv)+'#'+'@'+'#'+str(pv)+'#'+str(ci)
			mutations = [Mutation(column='content:info', value=info)]
			client.mutateRow('tongji_doc_test',rowkey,mutations,None)
			count=count+1
			print count
		except:
                        continue
	transport.close()	

def hbase_input_doc_ch_batch(date):
        import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
	clienta.execute("add jar /home/hadoop/Hive_UDF.jar")
	clienta.execute("create temporary function getCh as 'web.ChannelMaker'")
	
	try:
		con,cur = open_24()
		sql = "select full_url,id from media.d_channel"
		cur.execute(sql)
		chs = cur.fetchall()
		if len(chs) > 1710:
			tar = open('/data1/logs/web/channels.log','w')
			for ch,id in chs:
				print >> tar,str(ch),'\t',str(id)
			tar.close()
			
			#os.system("scp -r /data1/logs/web/channels.log hadoop@10.32.21.167:/data1/logs/web/")
				
			hql0 = "LOAD DATA LOCAL INPATH '/data1/logs/web/channels.log' \
				OVERWRITE INTO TABLE hbase_ch_id"
			clienta.execute(hql0)
			print 'update hbase_ch_id done'
		con.close()
	except Exception as e:
		print e
		print 'get channel from media.d_channel fall'
	
	hql1="insert overwrite table hbase_doc_ch \
		select doc,pv,uv,ci,getCh(ci,1) as ch1,getCh(ci,2) as ch2,getCh(ci,3) as ch3,getCh(ci,4) as ch4 from web_doc where dt='%s'"%date
	hql2="insert overwrite table hbase_ch_tmp select b.doc,b.pv,b.uv,b.ci,a.id  from hbase_ch_id a join  hbase_doc_ch b on trim(a.ch)=b.ch1"
	hql3="insert into table hbase_ch_tmp select b.doc,b.pv,b.uv,b.ci,a.id  from hbase_ch_id a join  hbase_doc_ch b on trim(a.ch)=b.ch2"
	hql4="insert into table hbase_ch_tmp select b.doc,b.pv,b.uv,b.ci,a.id  from hbase_ch_id a join  hbase_doc_ch b on trim(a.ch)=b.ch3"
	hql5="insert into table hbase_ch_tmp select b.doc,b.pv,b.uv,b.ci,a.id  from hbase_ch_id a join  hbase_doc_ch b on trim(a.ch)=b.ch4"
	hql6="insert overwrite table hbase_ch partition(dt='%s') \
        select a.doc,a.pv,a.uv,a.ci,a.id,coalesce(b.title,'None') from hbase_ch_tmp a left outer join (select url,title from hbase_doc_title_src group by url,title) b on a.doc=b.url"%date
	clienta.execute(hql1)
	print "hql1 done"
	clienta.execute(hql2)
	print "hql2 done"
	clienta.execute(hql3)
	print "hql3 done"
        clienta.execute(hql4)
	print "hql4 done"
	clienta.execute(hql5)
	print "hql5 done"
        clienta.execute(hql6)
	print "hql6 done"
	clienta.execute("select * from hbase_ch where dt='%s' and pv>50  Distribute by id sort by id,uv desc,pv desc"%date)
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
	array=[]
	preid='#'
	n=0
	i=0
        while 1:
                try:
                        try:
                        	line = clienta.fetchone()
				#print line
                	except Exception,e:
                        	line = None
                	if line == None or len(line) ==0:
                        	break
			tmp=line
                        doc,pv,uv,ci,id,title,date=tmp[0],int(tmp[1]),int(tmp[2]),tmp[3],tmp[4],tmp[5],tmp[6]
			if preid!=id:
				preid=id
				n=0
			n=n+1
			if n>1000:
				continue
                        rank=100000000-uv
			rank_pv=100000000-pv
                        rowkey=date+'~'+id+'~'+str(rank)+'~'+str(rank_pv)+str(random.randint(1,100))
			info=doc+'@'+str(uv)+'@'+str(title)+'@'+str(pv)+'@'+str(ci)
                        mutations = [Mutation(column='content:info', value=info)]
			bm=BatchMutation(rowkey,mutations)
			array.append(bm)
			i=i+1
			if i>=10000:
                        	client.mutateRows('tongji_doc_test',array,None)
				array=[]
				i=0
				print count	
                        count=count+1
                except:   
			continue
	client.mutateRows('tongji_doc_test',array,None)
	print len(array)
	print count
        transport.close()
	transporta.close()

def hbase_input_doc_ch_bu(date):
	print date
	con,cur = open_24()
	sql="select url,pv,uv,ci,id,title from ( \
	    select url,uv,pv,ci,id,title,row_number() over(partition by id,day order by uv desc) rn from \
	    (select title,url,pv,uv,to_char(day,'yyyy-mm-dd') day,rownum,d.id as id,ci \
            from web.f_doc f \
            join media.d_channel d on (f.url like '%%.'||substr(d.full_url,8)||'%%' or f.url like d.full_url||'%%') or (f.ci like '%%.'||substr(d.full_url,8)||'%%' or f.ci like d.full_url||'%%') \
	    where day=to_date('%s','yyyy-mm-dd')) test ) a where rn <201 "%str(date)
	#print sql
	cur.execute(sql)
	print "sql done"
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
        array=[]
	pre=0
        i=0
        preid='#'
        n=0
	for tmp in cur:
                    try:
			doc,pv,uv,ci,id,title=tmp[0],tmp[1],int(tmp[2]),tmp[3],tmp[4],tmp[5]
                    	if preid!=id:
                                preid=id
                                n=0
                        n=n+1
                        if n>200:
                                continue
                        rank=100000000-uv
                        rank_pv=100000000-pv
                        rowkey=date+'~'+id+'~'+str(rank)+'~'+str(rank_pv)+str(random.randint(1,100))
                        info=doc+'@'+str(uv)+'@'+str(title)+'@'+str(pv)+'@'+str(ci)
                        mutations = [Mutation(column='content:info', value=info)]
                        bm=BatchMutation(rowkey,mutations)
                        array.append(bm)
                        i=i+1
                        if i>=10000:
                                client.mutateRows('tongji_doc_test',array,None)
                                array=[]
                                i=0
                                print count
                        count=count+1
		    except:
                        continue
	print len(array)
	client.mutateRows('tongji_doc_test',array,None)
	print count
	transport.close()
	con.close()


def hbase_input_doc_all_bu(date):
        print date
        con,cur = open_24()
        sql="select url,pv,uv,ci,id,title from ( \
            select url,uv,pv,ci,id,title,row_number() over(partition by id,day order by uv desc) rn from \
            (select title,url,pv,uv,to_char(day,'yyyy-mm-dd') day,rownum,d.id as id,ci \
            from web.f_doc f \
            join media.d_channel d on (f.url like '%%.'||substr(d.full_url,8)||'%%' or f.url like d.full_url||'%%') or (f.ci like '%%.'||substr(d.full_url,8)||'%%' or f.ci like d.full_url||'%%') \
            where day=to_date('%s','yyyy-mm-dd')) test ) a where rn <201 "%str(date)
        sql="select  url,pv,uv,ci,title from (select url,pv,uv,ci,title,rownum from web.f_doc where day=to_date('%s','yyyy-mm-dd') order by uv desc ) a where rownum<201"%date
	#print sql
        cur.execute(sql)
        print "sql done"
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
        array=[]
        pre=0
        i=0
        for tmp in cur:
                    try:
                        doc,pv,uv,ci,title=tmp[0],tmp[1],int(tmp[2]),tmp[3],tmp[4]
                        if pre==uv:
                                rank=100000000-uv+1
                        else:
                                 rank=100000000-uv
                        if title=='' or title==None:
                                title='None'
                        rowkey=date+'~'+'all'+'~'+str(rank)
                        #print rowkey
                        info=doc+'@'+str(uv)+'@'+str(title)+'@'+str(pv)+'@'+str(ci)
                        #print info
                        mutations = [Mutation(column='content:info', value=info)]
                        client.mutateRow('tongji_doc_test',rowkey,mutations,None)
                        #print count
                        pre=uv
                    except:
                        continue
        transport.close()
        con.close()


def hbase_input_doc_all(date):
        print date
	import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
        clienta.execute("select a.dt,a.pv,a.doc,coalesce(b.title,'None'),a.uv,a.ci,coalesce(b.source,'None') from (select dt,pv,doc,uv,ci  from web_doc where dt='%s' order by uv desc limit 1000)  a left outer join hbase_doc_title_src b on a.doc=b.url"%date)
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
        array=[]
        pre=0
        i=0
	while 1:
                    try:
			
                        try:
                                line = clienta.fetchone()
                        except Exception,e:
                                line = None
                        if line == None or len(line) ==0:
                                break
                        tmp=line
                        date,pv,url,title,uv,ci,src=tmp[0],tmp[1],tmp[2],tmp[3],int(tmp[4]),tmp[5],tmp[6]
                        if pre==uv:
                                rank=100000000-uv+1
                        else:
                                 rank=100000000-uv
                        if title=='' or title==None:
                                title='None'
                        rowkey=date+'~'+'all'+'~'+str(rank)
			print rowkey
                        #print rowkey
                        info=url+'@'+str(uv)+'@'+str(title)+'@'+str(pv)+'@'+str(ci) +'@'+str(src)
                        #print info
                        mutations = [Mutation(column='content:info', value=info)]
                        client.mutateRow('tongji_doc_test',rowkey,mutations,None)
                        #count=count+1
                        #print count
                        pre=uv
                    except:
                        continue
        transport.close()
	

def hbase_input_doc_batch(day):
        import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
        clienta.execute("select * from (select a.dt as dt,a.pv as pv,a.doc as doc,coalesce(b.title,'None') as title,a.uv as uv,a.ci as ci,coalesce(b.source,'None') as src from (select dt,pv,doc,uv,ci  from web_doc where dt='%s')  a left outer join hbase_doc_title_src b on a.doc=b.url) a order by uv"%day)
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
        array=[]
        i=0
        while 1:
                try:
                        try:
                         	line = clienta.fetchone()
                        except Exception,e:
                                line = None
                        if line == None or len(line) ==0:
                                break
                        tmp=line
                        #print tmp
                        date,pv,url,title,uv,ci,src=tmp[0],tmp[1],tmp[2],tmp[3],int(tmp[4]),tmp[5],tmp[6]
                        rank=100000000-uv
			rowkey=url+'~'+date
                        #print rowkey
                        #info=doc+'#'+str(uv)+'#'+'@'+'#'+str(pv)+'#'+str(ci)
			info=str(pv)+'#'+title+'#'+str(uv)+'#'+ci+'#'+src
                        mutations = [Mutation(column='content:info', value=info)]
                        bm=BatchMutation(rowkey,mutations)
                        array.append(bm)
                        i=i+1
                        if i>=100000:
                                client.mutateRows('tongji_doc_test',array,None)
                                array=[]
                                i=0
                                print count
                        count=count+1
                        #print count,rowkey
                except:
                        continue
        client.mutateRows('tongji_doc_test',array,None)
        print len(array)
        print count
        transport.close()
        transporta.close()


def hbase_input_doc_ot(day):
        import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
	file=open('/data1/logs/hbasedata/web_ot_doc/%s.log'%day,'r')
	lines=file.readlines()
        count=0
        array=[]
        i=0
        for line in lines:
                try:
                        if line == None or len(line) ==0:
                                break
                        tmp=line.split('\t')
                        #print tmp
                        date,pv,url,uv=tmp[0],tmp[1],tmp[2].strip(),int(tmp[3])
			title='None'
			ci='None'
                        rank=100000000-uv
                        rowkey=url+'~'+date
                        print rowkey
                        #info=doc+'#'+str(uv)+'#'+'@'+'#'+str(pv)+'#'+str(ci)
                        info=pv+'#'+title+'#'+str(uv)+'#'+ci
                        mutations = [Mutation(column='content:info', value=info)]
                        bm=BatchMutation(rowkey,mutations)
                        array.append(bm)
                        i=i+1
                        if i>=100000:
                                client.mutateRows('tongji_doc_test',array,None)
                                array=[]
                                i=0
                                print count
                        count=count+1
                        #print count
                except:
                        continue
        client.mutateRows('tongji_doc_test',array,None)
        print len(array)
        print count
        transport.close()
        transporta.close()


def hbase_input_pv_batch(day):
        import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
	clienta.execute("add jar /home/hadoop/Hive_UDF.jar")
	clienta.execute("create temporary function getdomain as 'web.GetDomain'")
	clienta.execute("create temporary function subStr as 'media.SubStr'")
        clienta.execute("select subStr(refer,512),subStr(url,512),pv from (select ref as refer,url,cast(num as bigint) as pv,ci from web_pv where dt = '%s' and (ref like '%%ifeng.%%' or ref like '%%phoenixtv.%%') union all select getdomain(ref) as refer,url,sum(num) as pv,ci from web_pv where dt = '%s' and not (ref like '%%ifeng.%%' or ref like '%%phoenixtv.%%') group by getdomain(ref),url,ci) err where pv > 100  order by pv desc"%(day,day))
	transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
        array=[]
        i=0
	top=0
        while 1:
                try:
                        try:
                                line = clienta.fetchone()
                        except Exception,e:
                                line = None
                        if line == None or len(line) ==0:
                                break
                        tmp=line
                        #print tmp
                        ref,url,pv=tmp[0],tmp[1],int(tmp[2])
			top=top+1
			rank=100000000-pv
			topRank=str(rank)+'_'+str(top)
                        rowkeya=day+'~'+'ref'+'~'+ref+'~'+str(rank)+'~'+url
			rowkeyb=day+'~'+'url'+'~'+url+'~'+str(rank)+'~'+ref
			rowkeyc=day+'~'+'all'+'~'+ref+'~'+url
			rowkeyd=day+'~'+'top'+'~'+topRank+'~'+ref+'~'+url
                        #print rowkey
                        #info=doc+'#'+str(uv)+'#'+'@'+'#'+str(pv)+'#'+str(ci)
                        info=str(pv)
                        mutations = [Mutation(column='content:info', value=info)]
                        bma=BatchMutation(rowkeya,mutations)
			bmb=BatchMutation(rowkeyb,mutations)
			bmc=BatchMutation(rowkeyc,mutations)
                        array.append(bma)
			array.append(bmb)
			array.append(bmc)
			if top<1001:
				bmd=BatchMutation(rowkeyd,mutations)
				array.append(bmd)
                        i=i+1
                        if i>=20000:
                                client.mutateRows('tongji_pv',array,None)
                                array=[]
                                i=0
                                print count
                        count=count+1
                        #print count
                except:
                        continue
        client.mutateRows('tongji_pv',array,None)
        print len(array)
        print count
        transport.close()
        transporta.close()

def habse_input_media_pv_batch(day):
        import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
        clienta.execute("add jar /home/hadoop/Hive_UDF.jar")
        clienta.execute("create temporary function getdomain as 'web.GetDomain'")
        clienta.execute("create temporary function subStr as 'media.SubStr'")
        clienta.execute("select subStr(refer,512),subStr(url,512),pv from (select ref as refer,url,cast(num as bigint) as pv,ci from web_pv where dt = '%s' and (ref like '%%ifeng.%%' or ref like '%%phoenixtv.%%') union all select getdomain(ref) as refer,url,sum(num) as pv,ci from web_pv where dt = '%s' and not (ref like '%%ifeng.%%' or ref like '%%phoenixtv.%%') group by getdomain(ref),url,ci) err where pv > 100 and  (url like '%%v.ifeng.com%%') order by pv desc limit 1000"%(day,day))
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        count=0
        array=[]
        i=0
        top=0
        while 1:
                try:
                        try:
                                line = clienta.fetchone()
                        except Exception,e:
                                line = None
                        if line == None or len(line) ==0:
                                break
                        tmp=line
                        #print tmp
                        ref,url,pv=tmp[0],tmp[1],int(tmp[2])
                        top=top+1
                        rank=100000000-pv
                        topRank=str(rank)+'_'+str(top)
                        rowkeya=day+'~'+'topMedia'+'~'+topRank+'~'+ref+'~'+url
                        #print rowkeya
                        #info=doc+'#'+str(uv)+'#'+'@'+'#'+str(pv)+'#'+str(ci)
                        info=str(pv)
                        mutations = [Mutation(column='content:info', value=info)]
                        bma=BatchMutation(rowkeya,mutations)
                        array.append(bma)
                except:
                        continue
        client.mutateRows('tongji_pv',array,None)
        print len(array)
        #print count
        transport.close()
        transporta.close()

def hbase_input_doc(dateStr):
        con,cur = open_24()
        transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        print 'aaa'
        sql="select * from(select * from web.f_doc where  day=to_date('%s','yyyy-mm-dd') order by uv desc) WHERE ROWNUM <= 200 "%dateStr
        cur.execute(sql)
	count=1
        for item in cur:
                    try:
                        rowkey=dateStr+'~'+'all'+count
                        title=str(item[3])
                        info=str(item[1])+'#'+title+'#'+str(item[4])+'#'+str(item[5])
                        mutations = [Mutation(column='content:info', value=info)]
                        client.mutateRow('tongji_doc_test',rowkey,mutations,None)
                        count=count+1
		    except:
                        continue
        transport.close()

def doc_title_putout(date):
	import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10000)
	con,cur = open_24()
	file=open('/data1/web_title/%s.title'%date,'w')
	sql="select dd.uri uri,dd.title title,dd.src src from (select d.uri uri,d.title title,d.src src,row_number() over(partition by d.uri order by d.tm desc) rank from (select url_pc uri, title, src, tm from D_DOC_NEW where insert_time between to_date('%s 0:0:0', 'yyyy-mm-dd hh24:mi:ss') and to_date('%s 23:59:59', 'yyyy-mm-dd hh24:mi:ss'))d)dd where dd.rank=1"%(date,date)
	cur.execute(sql)
	for item in cur:
		try:
			print>> file,item[0]+'\t'+item[1]+'\t'+item[2]
			
		except:
                        continue
	con.close()
	#os.system("scp /data1/web_title/%s.title 10.32.21.167:/data1/web_title"%date)
	clienta.execute("LOAD DATA LOCAL INPATH '/data1/web_title/%s.title'  OVERWRITE INTO TABLE hbase_doc_title_src PARTITION(dt='%s')"%(date,date))
	transporta.close()

def doc_title_putout_bu():
        con,cur = open_24()
        file=open('/data1/web_title/bu.title','w')
        sql="select uri,title,src from D_DOC where insert_date<to_date('2017-07-23 0:0:0','yyyy-mm-dd hh24:mi:ss')"
        cur.execute(sql)
        for item in cur:
                try:
                        print>> file,item[0]+'\t'+item[1]+'\t'+item[2]

                except:
                        continue
        con.close()

def ch_id_output():
        con,cur = open_24()
        file=open('/data1/web_title/chID.log','w')
        sql="select full_url,id from media.d_channel"
        cur.execute(sql)
        for item in cur:
		print item
                print>> file,str(item[0])+'\t'+str(item[1])
        con.close()

def hbase_delete_test():
	import hiveserver
        transporta,clienta = hiveserver.getHiveThriftClient('10.90.7.187',10004)
	transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
	clienta.execute("select * from hbase_ch where dt='2015-01-12' and pv>50 ")
	while 1:
                #try:
                        try:
                                line = clienta.fetchOne()
                        except Exception,e:
                                line = None
                        if line == None or len(line) ==0:
                                break
                        tmp=line.split('\t')
                        #print tmp
                        doc,pv,uv,ci,id,title,date=tmp[0],tmp[1],int(tmp[2]),tmp[3],tmp[4],tmp[5],tmp[6]
                        rank=100000000-uv
                        rowkey=date+'~'+id+'~'+str(rank)
	                client.deleteAllRow("tongji_doc_test",rowkey,None)
	transporta.close()
	transport.close()

def hbase_scan_test():
	filter  = "PrefixFilter('2015-01-12~')" 
	transport = get_transport()
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
	scan = TScan()  
        scan.filterString=filter
	scanner = client.scannerOpenWithScan("tongji_doc_test", scan,"content") 
	for i in range(1,100):  
        	print "============%d============" %(i)  
        	get_arr = client.scannerGetList(scanner,1)  
        	if not get_arr :   
            		break;  
        	for  rowresult in get_arr:  
            		print rowresult.row
	client.scannerClose( scan )
	transport.close()
		


if __name__ == "__main__":
	date = time.strftime("%Y-%m-%d", time.localtime(time.time() - 86400))
	if 'web' in sys.argv:
                doc_title_putout(date)
                hbase_input_doc_batch(date)
                hbase_input_doc_ch_batch(date)
                hbase_input_doc_all(date)
		hbase_input_pv_batch(date)
		habse_input_media_pv_batch(date)
        if 'ot' in sys.argv:
                hbase_input_doc_ot(date)
	if 'bushu' in sys.argv:
		sdate = time.mktime(time.strptime('2018-03-28','%Y-%m-%d'))
		edate = time.mktime(time.strptime('2018-03-28','%Y-%m-%d'))
		while sdate <= edate:
			date = time.strftime('%Y-%m-%d',time.localtime(sdate))	
	#	doc_title_putout(date)
		#	hbase_input_doc_batch(dateStr)	
			doc_title_putout(date)
			hbase_input_doc_batch(date)
			hbase_input_doc_ch_batch(date)
			hbase_input_doc_all(date)
			hbase_input_pv_batch(date)
			habse_input_media_pv_batch(date)
			hbase_input_doc_ot(date)
			print date,'is Done @',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
			sdate = sdate + 86400

	if 'test' in sys.argv:
		a=get_transport()
		print a
