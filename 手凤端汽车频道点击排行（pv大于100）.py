#!/usr/local/python-2.7.8/bin/python
#coding=utf8

# To change this template, choose Tools | Templates
# and open the template in the editor.



import sys,urllib,hashlib,json
import time
import os
import httplib
import subprocess
import cx_Oracle
import ConfigParser
#import warn
import string
from pyhs2.error import Pyhs2Exception


#获取配置信息
try:
    #config = ConfigParser.ConfigParser()
    #config.readfp(open("/data/script/cron_hive/config_wap_new.ini", "rb"))
    #hiveserver_ip = str(config.get("hiveserver", "ip"))      #hiveserver连接地址
    #hiveserver_port = int(config.get("hiveserver", "port"))  #hiveserver接口
    #database_host = str(config.get("database", "host"))           #数据库地址
    #database_username = str(config.get("database", "username"))   #数据库登录用户名
    #database_password = str(config.get("database", "password"))   #数据库登录密码

    hiveserver_ip = "服务器地址"
    hiveserver_port =10000
    database_host="数据库地址:1521/orcl"
    database_username="数据库用户名称"
    database_password="数据库密码"
except Exception, e:
    logger.error('getConfig err : ' + e.message())


#wapSQLDict = { 
#"wap_doc" : ["select url,ch,pv,uv,screen from wap_doc where pv>=100 and dt='%s'"%dateNew, "insert into wapnew.f_doc_20180521(day,url,ch,pv,uv,version) select day,:1,:2,:3, :4,:5 from media.d_date where day=to_date(:6,'yyyy-mm-dd')"], \
#}

#updateSQLDict = {
#"wap_update_title" : "update wapnew.f_doc_20180521 f set title=(select title from wapnew.d_url2title d where d.url=f.url) where f.day=to_date('%s','yyyy-mm-dd') and INSTR(f.url,chr(35),1,1)=0 and (INSTR(f.url,'_',1,1)=0 or  (INSTR(f.url,'_',1,1)>0 and f.url like '%%aid=zmt_%%'))"%dateStr,\
#"wap_update_title2" : "update wapnew.f_doc_20180521 f set title=(select title from wapnew.d_url2title d where d.url=SUBSTR(f.url,1,INSTR(f.url,chr(35),1,1)-1)) where f.day=to_date('%s','yyyy-mm-dd') and INSTR(f.url,chr(35),1,1)>0 "%dateStr,\
#"wap_update_title3" : "update wapnew.f_doc_20180521 f set title=(select title from wapnew.d_url2title d where d.url=CONCAT(SUBSTR(f.url,1,INSTR(f.url,chr(95),1,1)-1),SUBSTR(f.url,INSTR(f.url,chr(47),-1,1)))) where f.day=to_date('%s','yyyy-mm-dd') and instr(f.url,chr(95))>0 and not f.url like '%%aid=zmt_%%' "%dateStr,\
#"wap_doc_update_src" : "update wapnew.f_doc_20180521 f set source=(select src from wapnew.d_doc_src d where d.url=f.url) where f.day=to_date('%s','yyyy-mm-dd')"%dateStr,\
#}


def sqlExecute(table_l, sqlDict):
    try:
        conn,cursor = openDB()
        cursor.arraysize = 100
    except Exception, e:
        logger.error('DBConnection err : ' + e.message())
        conn.close()
        sys.exit()

    for key in table_l:
        transportT,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
        value = sqlDict[key]
        err, num, uni = 0, 0, set()
        [s_sql, i_sql] = value
        client.execute(s_sql)
        while 1:
            row = None
            try:
                row = client.fetchone()
            except Exception,e:
                print e
            if (row == None or len(row) == 0):
                break
            #strs = string.split(row, '\t')
            #strs.append(dateStr)
            row.append(dateStr)
            try:
                cursor.execute(i_sql, row)
                num += 1
            except Exception, e:
                err += 1
                print e,row
                if err > 10000:
                    print 'Table Err:', key, e, err
                    break
                else:
                    pass
	    if num > 10000 and err < 10000:
	        conn.commit()
	        num =0 
        if err <= 10000:
            conn.commit()
        transportT.close()
        print key+' done'
        logger.info(key+' done')
    cursor.close()
    conn.close()

def store_zmt_idrelation():
    os.system("wget --output-document=%szmttree.html '%s'" % (wapDir, 'http://cdn.iclient.ifeng.com/v/res/c/tree.html'))
    interfacefile=open('%szmttree.html' % (wapDir)).read()
    dictree=json.loads(interfacefile.decode("utf-8"))
    print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()),'interface end'
    try:
        conn,cursor = openDB()
        cursor.arraysize = 100
        transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    except Exception, e:
        print e
        logger.error('DBConnection err : ' + e.message())
        conn.close()
        sys.exit()
    data = []
    hql = "select id,columnid from wap_zmt_idrelation where dt='%s' "%dateNew
    client.execute(hql)
    print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()),'hql end'
    while 1:
        row = None
        try:
            row = client.fetchone()
        except Exception,e:
            pass
        if (row == None or len(row) == 0):
            break
        #strs = string.split(row, '\t')
        data.append(row)
    sql = "insert into wapnew.d_zmtid(day,id,columnid,articlename,columnname,kind) select day,:1,:2,:3,:4,:5 from media.d_date where day=to_date(:6,'yyyy-mm-dd') and :1 is not null"
    for (id,columnid) in data:
        articlename=getZmtarticle(id)
	columnname,kind=getZmtkind(columnid,dictree)
        tmp = (id,columnid,articlename,columnname,kind,dateStr)
        try:
            cursor.execute(sql,tmp)
        except Exception, e:
            print 'table Err: wap_zmt_idrelation ',  e
    conn.commit()
    conn.close()
    transport.close()

def getZmtkind(id,dictree):
	for i in dictree["list"]:
        	for j in i["children"]:
            		if j["id"]=="%s"%id:
                		return j["name"],i["name"]
	return 'Unknown','Unknown'


def getZmtarticle(id):
	try:
		interfacearticle=urllib.urlopen('http://api.iclient.ifeng.com/api_vampire_document?aid='+id )
		if interfacearticle.getcode()==404:
        		return 'Unknown'
		interfacearticle=interfacearticle.read()
		dicarticle=json.loads(interfacearticle)
		return dicarticle['body']['title']
	except Exception,e:
		print e
		return 'Unknown'

def chsExecute():
    try:
        conn,cursor = openDB()
        cursor.arraysize = 100
        transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    except Exception, e:
        print e
        logger.error('DBConnection err : ' + e.message())
        conn.close()
        sys.exit()
    data = []
    hql = "select ch,pv,uv,chnn from wap_chs where ch<>'tt_font=%%' and  pv>100 and dt='%s' and length(ch)<20"%dateNew
    client.execute(hql)
    while 1:
        row = None
        try:
            row = client.fetchone()
        except Exception,e:
            pass
        if (row == None or len(row) == 0):
            break
        #strs = string.split(row, '\t')
        data.append(row)
    sql = "insert into wapnew.f_chs(day,ch,pv,uv,chnn,chname,chtype,chtype2) select day,:1,:2,:3,:4,:5,:6,:7 from media.d_date where day=to_date(:8,'yyyy-mm-dd') and :1 is not null"
    try:
        chnsorcl=getChnsorcl(cursor)
    except Exception,e:
        print e
        chnsorcl={}
    for (ch,pv,uv,chnn) in data:
	if ch.startswith('sp_'):
            chn=chnsorcl.get(ch,['','\xca\xd3\xc6\xb5'.decode('gbk'),'\xce\xde\xcf\xdf\xca\xd3\xc6\xb5'.decode('gbk')])
        else:
            chn=chnsorcl.get(ch,['\xce\xb4\xd6\xaa'.decode('gbk'),'\xc6\xe4\xcb\xfb'.decode('gbk'),'\xc6\xe4\xcb\xfb'.decode('gbk')])
        chname,chtype,chtype2=chn[0],chn[1],chn[2]
        tmp = (ch, pv, uv, chnn, chname, chtype, chtype2, dateStr)
        try:
            cursor.execute(sql,tmp)
        except Exception, e:
            print 'table Err: wap_chs ',  e
    conn.commit()
    conn.close()
    transport.close()

def getChnsorcl(cur):
    rs={}
    sql="select CHID,CHNAME,CHTYPE,FCHNAME from wapnew.d_chs_new"
    try:
	cur.execute(sql)
    except Exception,e:
	print e
    for line in cur.fetchall():
	rs.setdefault(line[0],['','',''])
	rs[line[0]][0]=line[1].decode('utf-8')
	rs[line[0]][1]=line[2].decode('utf-8')
	rs[line[0]][2]=line[3].decode('utf-8')
    return rs

def getChnsxml():
    result={}
    os.system("wget --output-document=%schs.xml '%s'" % (wapDir, 'http://channel.3g.ifeng.com/interface/wapChannel.php'))
    for line in open('%schs.xml' % (wapDir)):
        try:
            if line.startswith('<chcode>'):
                chcode = line[8:-10]
            elif line.startswith('<chname>') and chcode != '':
                line = line.decode('utf-8')
                result.setdefault(chcode,['','',''])
                result[chcode][0] = line[8:-10].encode('utf-8')
            elif line.startswith('<firstClass>') and chcode != '':
                line = line.decode('utf-8')
                result.setdefault(chcode,['','',''])
                result[chcode][1] = line[12:-14].encode('utf-8')
            elif line.startswith('<secClass>') and chcode != '':
                line = line.decode('utf-8')
                result.setdefault(chcode,['','',''])
                result[chcode][2] = line[10:-12].encode('utf-8')
            else:
                chcode = ''
        except Exception,e:
            print e
    return result

def getChs(ch):
    chs = ch
    qqlist = ['qq_yl02','qq_xz02','qq_cj02','qq_qc02','qq_jk02','qq_kj02','qq_xw02','qq_js02','qq_xy02','qq_nx02',\
        'qq_ty02','qq_nba02','qq_xs02']
    chslist2 = ['FFSZ','FFBD','FFDM','FFFT','FFJS','FFYD','FFZD','UUCN','XSDM','XSFT','XSJS','XSUU','XSYD','XSZY','ZYWX']
    if ch.lower().startswith('go') and ch != 'gok':
        chs = 'go'
    if ch.lower().startswith('yl_'):
        chs = 'yl_'
    if ch.lower().startswith('uc') and ch.lower() != 'ucwap.ifeng.com':
        chs = 'uc'
    if ch.lower().startswith('qq') and (ch not in qqlist):
        chs = 'qq'
    for c in chslist2:
        if chs.upper().startswith(c):
            chs = c
    return chs

def updateExecute(sqlDict):
    try:
        conn,cursor = openDB()
        cursor.arraysize = 100
    except Exception, e:
        logger.error('DBConnection err : ' + e.message())
        conn.close()
        sys.exit()

    keys = sqlDict.keys()
    for key in keys:
        value = sqlDict[key]
        try:
            cursor.execute(value)
            print key, value
        except:
            print key, value, 'update not right.'
        conn.commit()
    cursor.close()
    conn.close()

def getCmsDoc(ids):
    id2title = {}
    id = ''
    i = len(ids) / 50
    j, k = 0, 0
    while j < i + 1:
        if j == i:
            k = len(ids)
        else:
            k = (j + 1) * 50
        os.system("wget --output-document=%stitle.xml '%s%s'" % (wapDir, 'http://api.3g.ifeng.com/searchinterface?ids=', string.join(ids[j * 50:k], '|')))
        for line in open('%stitle.xml' % (wapDir)):
            try:
                if line.startswith('<id>'):
                    id = line[4:-7]
                elif line.startswith('<title>') and id != '':
                    line = line.decode('utf-8')
                    id2title[id] = line[16:-13].encode('utf-8')
                else:
                    id = ''
            except:
                continue
        j = j + 1
    return id2title

def getTitles(urls):
    ids = []
    id2url = {}
    url2title = {}
    id2titlebu={}
    for url in urls:
        try:
            id = url[url.index('aid=') + 4:].split('#')[0]
            ids.append(id)
            if id2url.has_key(id):
                id2url[id].append(url)
            else:
                id2url[id] = [url]
        except:
            continue
    id2title = getCmsDoc(ids)
    for id in id2url:
        if not id in id2title and ',' not in id:
            idbu=[]
            idbu.append(id)
            id2titlebupart = getCmsDoc(idbu)
            for id in id2titlebupart:
                id2titlebu.setdefault(id,id2titlebupart[id])
    for id in id2url:
        for url in id2url[id]:
            url2title[url] = id2title.get(id, '')
            if url2title[url]=='':
                url2title[url]=id2titlebu.get(id, '')
    return url2title

def updateTitle():
    try:
        conn,cursor = openDB()
        cursor.arraysize = 100
    except Exception, e:
        logger.error('DBConnection err : ' + e.message())
        conn.close()
        sys.exit()
    geturlsql="select url from wapnew.f_doc_group f where f.day=to_date('%s','yyyy-mm-dd') and title is null"%dateStr
    try:
        cursor.execute(geturlsql)
    except:
        print 'geturlsql not right.'
    urls=[]
    for (url,) in cursor.fetchall():
        #print url
        urls.append(url)
    url2title = getTitles(urls)
    for k in url2title:
        if url2title[k]=='' or url2title[k]==None or len(url2title[k])==0:
                continue
        try:
            cursor.execute("merge into wapnew.d_url2title d using dual on (d.url=:1) when matched then update set d.title=:2 when not matched then insert values(:3,:4,to_date(:5,'yyyy-mm-dd'))", (k[0:512], url2title[k], k[0:512], url2title[k],dateStr))
        except Exception, e:
            print k, url2title[k], 'update not right.',e
        conn.commit()
    conn.commit()
    conn.close()
    print 'updateTitle Done'


def openDB():
    #con=cx_Oracle.connect("wapnew", "wapnew", "10.32.21.77:1521/orcl")
    con=cx_Oracle.connect(database_username,database_password,database_host)
    cur=con.cursor()
    return con,cur

def warningSms(plist,pcontent):
    for pnum in plist:
	sendSms(pnum,pcontent)

def sendSms(phoneNumber,content):
    conn = httplib.HTTPConnection("218.206.84.105:8083")
    conn.request("GET","/service/sendsms?sender=130&mobile="+phoneNumber+"&msgtype=0&subject=666&content="+content)
    conn.close()

def store_user_depth(dateNew,date):
    con,cur = openDB()
    transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    dic = {}
    total = 0
    hql = "select title,num from wap_user_depth where dt = '%s'"%dateNew
    client.execute(hql)
    while 1:
        line = None
        try:
            line = client.fetchone()
        except Exception,e:
            pass
        if line == None or len(line) ==0:
                    break
        #tmp = line.split('\t')
        if len(line) != 2:
                continue
        depth,num = int(line[0]),int(line[1])
        total += num
        if depth > 20:
                depth = '20\xe4\xbb\xa5\xe4\xb8\x8a'
        elif depth > 10:
                depth = '11-20'
        elif depth > 5:
                depth = '6-10'
        elif depth >= 3:
                depth = '3-5'
        dic.setdefault(depth,0)
        dic[depth] += num
    for key in dic:
#       print key,dic
        sql = "insert into wapnew.f_user_depth values(to_date('%s','yyyy-mm-dd'),'%s',%.4f)"%(date,key,float(dic[key])/total)
        try:
            cur.execute(sql)
        except Exception,e:
            print e
            print key,dic[key]
    con.commit()
    con.close() 
    transport.close()

def store_user_page(dateNew,date):
    con,cur = openDB()
    transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    dic = {}
    total = 0
    hql = "select title,num from wap_user_page where dt = '%s'"%dateNew
    client.execute(hql)
    while 1:
        line = None
        try:
            line = client.fetchone()
        except Exception,e:
            pass
        if line == None or len(line) ==0:
                    break
        #tmp = line.split('\t')
        if len(line) != 2:
                continue
        depth,num = int(line[0]),int(line[1])
        total += num
        if depth > 30:
                depth = '30\xe4\xbb\xa5\xe4\xb8\x8a'
        elif depth > 10:
                depth = '11-30'
        elif depth > 5:
                depth = '6-10'
        elif depth >= 3:
                depth = '3-5'
        dic.setdefault(depth,0)
        dic[depth] += num
    for key in dic:
#       print key,dic
        sql = "insert into wapnew.f_user_page values(to_date('%s','yyyy-mm-dd'),'%s',%.4f)"%(date,key,float(dic[key])/total)
        try:
            cur.execute(sql)
        except Exception,e:
            print e
            print key,dic[key]
    con.commit()
    con.close()
    transport.close()

def store_user_visit(dateNew,date):
    con,cur = openDB()
    transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    dic = {}
    total = 0
    hql = "select title,num from wap_user_visit where dt = '%s'"%dateNew
    client.execute(hql)
    while 1:
        line = None
        try:
            line = client.fetchone()
        except Exception,e:
            pass
        if line == None or len(line) ==0:
                    break
        #tmp[0] = line.split('\t')
        if len(line) != 2:
                continue
        depth,num = int(line[0]),int(line[1])
        total += num
        if depth >= 3:
                depth = '3\xe5\x8f\x8a\xe4\xbb\xa5\xe4\xb8\x8a'
        dic.setdefault(depth,0)
        dic[depth] += num

    for key in dic:
#       print key,dic
        sql = "insert into wapnew.f_user_visit values(to_date('%s','yyyy-mm-dd'),'%s',%.4f)"%(date,key,float(dic[key])/total)
        try:
            cur.execute(sql)
        except Exception,e:
            print e
            print key,dic[key]
    con.commit()
    con.close()
    transport.close()

def store_waptype_bak(date):
    con,cur = openDB()
    transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    dic={}

    hql="select ch,type,pv,uv from wap_pagetype where dt='%s_new'"%date
    client.execute(hql)
    while 1:
        line = None
        try:
            line = client.fetchone()
        except Exception,e:
            pass
        if line == None or len(line) ==0:
                    break
        #tmp = line.split('\t')
        #print line
        ch,type,pv,uv=line[0],line[1],int(line[2]),int(line[3])
        if len(line) != 4:
                continue
        if dic.has_key(ch):
           if type=='photo':
              dic[ch][0],dic[ch][1]=pv,uv
           if type=='doc':
              dic[ch][2],dic[ch][3]=pv,uv
        else:
           dic.setdefault(ch,[0,0,0,0])
           if type=='photo':
              dic[ch][0],dic[ch][1]=pv,uv
           if type=='doc':
              dic[ch][2],dic[ch][3]=pv,uv
    print dic
    for ch in dic:
        sql = "insert into wapnew.f_pagetype values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d, null, null)"%(date,ch,dic[ch][0],dic[ch][1],dic[ch][2],dic[ch][3])
        try:
            cur.execute(sql)
        except Exception,e:
            print e
    con.commit()
    con.close()
    transport.close()

def new_store_waptype(date):
    con,cur = openDB()
    transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    dic={}
    hql="select ch,type,pv,uv from new_wap_pagetype where dt='%s_new'"%date
    client.execute(hql)
    while 1:
        line = None
        try:
            line = client.fetchone()
        except Exception,e:
            pass
        if line == None or len(line) ==0:
                    break
        #tmp = line.split('\t')
        print line
        ch,type,pv,uv=line[0],line[1],int(line[2]),int(line[3])
        if len(line) != 4:
                continue
        if dic.has_key(ch):
           if type=='photo':
              dic[ch][0], dic[ch][1] = pv, uv
           if type=='doc':
              dic[ch][2], dic[ch][3] = pv, uv
           if type=="other":
              dic[ch][4], dic[ch][5] = pv, uv
           if type=="index":
              dic[ch][6], dic[ch][7] = pv, uv
        else:
           dic.setdefault(ch,[0,0,0,0,0,0,0,0])
           if type=='photo':
              dic[ch][0], dic[ch][1] = pv, uv
           if type=='doc':
              dic[ch][2], dic[ch][3] = pv, uv
           if type=="other":
              dic[ch][4], dic[ch][5] = pv, uv
           if type=="index":
              dic[ch][6], dic[ch][7] = pv, uv
    for ch in dic:
        sql = "insert into wapnew.f_pagetype_new values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d,%d,%d,%d,%d)"%(date,ch,dic[ch][0],dic[ch][1],dic[ch][2],dic[ch][3], dic[ch][4], dic[ch][5], dic[ch][6], dic[ch][7])
        print sql
        try:
            cur.execute(sql)
        except Exception,e:
            print e
    con.commit()
    con.close()
    transport.close()

def store_waptype(date):
    con,cur = openDB()
    transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
    dic={}
    hql="select ch,type,pv,uv from wap_pagetype where dt='%s_new'"%date
    client.execute(hql)
    while 1:
        line = None
        try:
            line = client.fetchone()
        except Exception,e:
            pass
        if line == None or len(line) ==0:
                    break
        #tmp = line.split('\t')
        #print line
        ch,type,pv,uv=line[0],line[1],int(line[2]),int(line[3])
        if len(line) != 4:
                continue
        if dic.has_key(ch):
           if type=='photo':
              dic[ch][0],dic[ch][1]=pv,uv
           if type=='doc':
              dic[ch][2],dic[ch][3]=pv,uv
           if type=="other":
              dic[ch][4], dic[ch][5] = pv, uv
        else:
           dic.setdefault(ch,[0,0,0,0, 0, 0])
           if type=='photo':
              dic[ch][0],dic[ch][1]=pv,uv
           if type=='doc':
              dic[ch][2],dic[ch][3]=pv,uv
           if type=="other":
              dic[ch][4], dic[ch][5] = pv, uv
    for ch in dic:
        sql = "insert into wapnew.f_pagetype values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d, %d, %d)"%(date,ch,dic[ch][0],dic[ch][1],dic[ch][2],dic[ch][3], dic[ch][4], dic[ch][5])
        print sql
        try:
            cur.execute(sql)
        except Exception,e:
            print e
    con.commit()
    con.close()
    transport.close()
    '''
    con,cur = openDB()
    try:
        if dic.get("site"):
            ch = "site"
            sql = "update wapnew.f_pagetype a set a.other_pv = %d where ch = '%s' and day = to_date('%s', 'yyyy-mm-dd')" % (dic["site"][5], "site", date)
            print sql, "update"
            cur.execute(sql)
    except Exception, e:
        print e
    con.commit()
    con.close()
    '''
def store_detail():
        server,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
        con,cur = openDB()
        delsql ="delete from f_phone_brand_uv_detail where day = to_date('%s','yyyy-mm-dd')"%dateStr
        cur.execute(delsql)
        hql = "select dt,phone,count(distinct uid) uv from wap_phone where dt = '%s' group by phone,dt" %dateStr
        insql = "insert into  f_phone_brand_uv_detail values(to_date(:1,'yyyy-mm-dd'),:2,:3)"
        client.execute(hql)
        result = client.fetchall()
        for (dt,phone,uv) in result:
                try:
                        cur.execute(insql,(dt,phone,uv))
                except Exception,e:
                        print e
        con.commit()
        server.close()
        con.close()
def store_brand():
        con,cur = openDB()
        delsql = "delete from f_phone_brand_uv where day = to_date('%s','yyyy-mm-dd')"%dateStr
        cur.execute(delsql)
        sumuv ="select sum(c.uv) from (select b.day,a.brand,b.uv from d_phone_brand a inner  join (select phone_type,day,sum(uv) uv from f_phone_brand_uv_detail where day = to_date('%s','yyyy-mm-dd') group by phone_type,day) b on trim(a.phone_type) = trim(b.phone_type) where a.brand !='other')c" %dateStr
        cur.execute(sumuv)
        sun_uv= cur.fetchall()
        sum_uv=sun_uv[0][0]
        get_data = "select b.day,a.brand,sum(b.uv) from d_phone_brand a inner join (select phone_type,day,sum(uv) uv from f_phone_brand_uv_detail where day = to_date('%s','yyyy-mm-dd') group by phone_type,day) b on trim(a.phone_type) = trim(b.phone_type) where a.brand != 'other' group by b.day,a.brand"%dateStr
        store = "insert into f_phone_brand_uv values(:1,:2,:3,:4,:5)"
        cur.execute(get_data)
        for (day,brand,uv) in cur.fetchall():
                try:
                        rate =uv*1.00/sum_uv*100

                        cur.execute(store,(day,brand,uv,rate,'detail'))
                except Exception,e:
                        print e
        con.commit()
        con.close()
def store_brand_show():
        dic ={}
        con,cur = openDB()
        delsql ="delete from f_phone_brand_uv where day = to_date('%s','yyyy-mm-dd') and type ='show'"%dateStr
        cur.execute(delsql)
        sqlshow = "select day,trim(brand), sum(uv),sum(rate) from f_phone_brand_uv where day = to_date('%s','yyyy-mm-dd') and type ='detail'  group by day,brand" %dateStr
        insqlshow = "insert into f_phone_brand_uv values(:1,:2,:3,:4,:5)"
        cur.execute(sqlshow)
        for (day,brand,uv,rate) in cur.fetchall():
                try:
                        if rate <0.2 or brand =='china_mobile':
                                brand ='其它'
                        dic.setdefault((day,brand),[0,0])
                        dic[(day,brand)][0]+=uv
                        dic[(day,brand)][1]+=rate

                #       cur.execute(insqlshow,(day,brand,uv,rate,'show'))
                except Exception,e:
                        print e
        for ((day,brand),[uv,rate]) in dic.iteritems():
                try:
                        cur.execute(insqlshow,(day,brand,uv,rate,'show'))
                except Exception,e:
                        print e
        con.commit()
        con.close()
def store_chann_zmt(dateStr):
	con,cur = openDB()
	delsql = "delete from wapnew.f_chann_zmt where day = to_date('%s','yyyy-mm-dd')"%dateStr
	cur.execute(delsql)
	insql = "insert into wapnew.f_chann_zmt values(:1,:2,:3,to_date(:4,'yyyy-mm-dd'),:5)"
	server,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
	UDFs = ["add jar /data/script/cron_hive/Hive_UDF.jar","create temporary function getClick as 'other.getClick'"]
	hql = "select ch,count(*),count(distinct uid),sum(getClick(url)) click from wap_zmt where dt = '%s' and ch like '%%ifeng.com%%' group by ch"%dateStr
	for UDF in UDFs:
                try:
                        client.execute(UDF)
                except:
                        print UDF
	client.execute(hql)
	for (ch,pv,uv,click) in client.fetchall():
		try:
			cur.execute(insql,(ch,pv,uv,dateStr,click))
		except Exception,e:
			print e
	con.commit()
        con.close()
	server.close()
def store_zmt_overall(dateStr):
        con,cur = openDB()
        delsql = "delete from wapnew.f_zmt_overall where day = to_date('%s','yyyy-mm-dd')"%dateStr
        cur.execute(delsql)
        insql = "insert into wapnew.f_zmt_overall(day,uv,pv,click) values(to_date(:1,'yyyy-mm-dd'),:2,:3,:4)"
        server,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
	UDFs = ["add jar /data/script/cron_hive/Hive_UDF.jar","create temporary function getClick as 'other.getClick'"]
        hql = "select count(*),count(distinct uid),sum(getClick(url)) click from wap_zmt where dt = '%s' "%dateStr
        for UDF in UDFs:
                try:
                        client.execute(UDF)
                except:
                        print UDF
        client.execute(hql)
        for (pv,uv,click) in client.fetchall():
                try:
                        cur.execute(insql,(dateStr,uv,pv,click))
                except Exception,e:
                        print e
        con.commit()
        con.close()
        server.close()
def store_zmt_doc(dateStr):
        con,cur = openDB()
        delsql = "delete from wapnew.f_zmt_doc where day = to_date('%s','yyyy-mm-dd')"%dateStr
        cur.execute(delsql)
        insql = "insert into wapnew.f_zmt_doc(day,pv,url,uv,ci,zmt_id) values(to_date(:1,'yyyy-mm-dd'),:2,:3,:4,:5,:6)"
        server,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
        hql = "select url,ch,split(zmt,'\\\\.')[0],count(*),count(distinct uid) from wap_zmt where dt = '%s' and ch like '%%ifeng.com%%' group by url,ch,split(zmt,'\\\\.')[0] having count(*) >99"%dateStr
#       hql = "select dt,url,ch,split(zmt,'\\\\.')[0],count(*),count(distinct uid) from wap_zmt where ch like '%%ifeng.com%%' group by dt,url,ch,split(zmt,'\\\\.')[0] having count(*) >99"
        client.execute(hql)
        for (url,ch,zmt_id,pv,uv) in client.fetchall():

                try:
                        cur.execute(insql,(dateStr,pv,url,uv,ch,zmt_id))
                except Exception,e:
                        import traceback
                        print e
                        print traceback.print_exc()
        con.commit()
        con.close()
        server.close()
def store_zmt_id_detail(dateStr):
        con,cur = openDB()
        delsql = "delete from wapnew.f_zmt_id_detail where day = to_date('%s','yyyy-mm-dd')"%dateStr
        cur.execute(delsql)
        insql = "insert into wapnew.f_zmt_id_detail(day,zmt_id,pv,uv,click) values(to_date(:1,'yyyy-mm-dd'),:2,:3,:4,:5)"
        server,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
        hql = "select split(zmt,'\\\\.')[0],count(*),count(distinct uid),sum(getClick(url)) from wap_zmt where dt = '%s' group by split(zmt,'\\\\.')[0] "%dateStr
	UDFs = ["add jar /data/script/cron_hive/Hive_UDF.jar","create temporary function getClick as 'other.getClick'"]
#       hql = "select dt,split(zmt,'\\\\.')[0],count(*),count(distinct uid) from wap_zmt group by dt,split(zmt,'\\\\.')[0]"
	for UDF in UDFs:
                try:
                        client.execute(UDF)
                except:
                        print UDF
        client.execute(hql)
        for (zmt_id,pv,uv,click) in client.fetchall():

                try:
                        cur.execute(insql,(dateStr,zmt_id,pv,uv,click))
                except Exception,e:
                        import traceback
                        print e
                        print traceback.print_exc()
        con.commit()
        con.close()
        server.close()	
def update_f_doc_title(dateStr):
        con = cx_Oracle.connect('wapnew','nem2017in09g08wap','10.90.19.2:1521/orcl')
        cur = con.cursor()
        urls = set()
        sql = "select url from wapnew.f_doc_20180521 where day = to_date('%s','yyyy-mm-dd') and title is null"%dateStr
        cur.execute(sql)
        for (url,) in cur.fetchall():
                urls.add(url)

        for url in urls:
                url_tmp = url.split('imgnum=')[0]
                url_tmp = url_tmp.split('#')[0]
                sql = "select title from wapnew.d_doc_src where url = '%s'"%url_tmp
                cur.execute(sql)
                title = cur.fetchone()
                if title == '' or title == None:
                        continue
                title = title[0]
                update_sql = "update wapnew.f_doc_20180521 set title = '%s' where day = to_date('%s','yyyy-mm-dd') and url = '%s'"%(title,dateStr,url)
                cur.execute(update_sql)
        con.commit()
        con.close()

def update_f_doc_group_title(dateStr):
        con = cx_Oracle.connect('wapnew','nem2017in09g08wap','10.90.19.2:1521/orcl')
        cur = con.cursor()
        urls = set()
        sql = "select url from wapnew.f_doc_group where day = to_date('%s','yyyy-mm-dd') and title is null"%dateStr
        cur.execute(sql)
        for (url,) in cur.fetchall():
                urls.add(url)

        for url in urls:
                url_tmp = url.split('imgnum=')[0]
                url_tmp = url_tmp.split('#')[0]
                sql = "select title from wapnew.d_doc_src where url = '%s'"%url_tmp
                cur.execute(sql)
                title = cur.fetchone()
                if title == '' or title == None:
                        continue
                title = title[0]
                update_sql = "update wapnew.f_doc_group set title = '%s' where day = to_date('%s','yyyy-mm-dd') and url = '%s'"%(title,dateStr,url)
                cur.execute(update_sql)
        con.commit()
        con.close()

if __name__ == "__main__":
#    import os
#    os.environ["NLS_LANG"] = ".UTF8"
#    print '========Wap Started @', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
#    import hiveserver
#    try:
#        transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
#    except Pyhs2Exception, e:
#	print e
#    try:
#        table_l = ["wap_doc"]
#        print database_host
#        sqlExecute(table_l, wapSQLDict)
#        updateExecute(updateSQLDict)
#    except Exception, e:
#        transport.close()

#    print dateStr
#    print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
#    update_f_doc_title(dateStr)
#    transport.close()
#    print '========Wap Finished @', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))


    if 'bushu' in sys.argv:
    	sdate = time.mktime(time.strptime('2018-03-01','%Y-%m-%d'))
	edate = time.mktime(time.strptime('2018-05-20','%Y-%m-%d'))
	while sdate <= edate:
		dateStr = time.strftime('%Y-%m-%d',time.localtime(sdate))
		dateNew = time.strftime('%Y-%m-%d',time.localtime(sdate))+'_new'
		print dateStr,dateNew

		import os
		os.environ["NLS_LANG"] = ".UTF8"
		print '========Wap Started @', dateStr
		import hiveserver
		try:
			transport,client = hiveserver.getHiveThriftClient(hiveserver_ip,hiveserver_port)
		except Pyhs2Exception, e:
			print e
		wapSQLDict = {"wap_doc" : ["select url,ch,pv,uv,screen from wap_doc where pv>=100 and dt='%s'"%dateNew, "insert into wapnew.f_doc_20180521(day,url,ch,pv,uv,version) select day,:1,:2,:3, :4,:5 from media.d_date where day=to_date(:6,'yyyy-mm-dd')"]}

		updateSQLDict = {"wap_update_title" : "update wapnew.f_doc_20180521 f set title=(select title from wapnew.d_url2title d where d.url=f.url) where f.day=to_date('%s','yyyy-mm-dd') and INSTR(f.url,chr(35),1,1)=0 and (INSTR(f.url,'_',1,1)=0 or  (INSTR(f.url,'_',1,1)>0 and f.url like '%%aid=zmt_%%'))"%dateStr,"wap_update_title2" : "update wapnew.f_doc_20180521 f set title=(select title from wapnew.d_url2title d where d.url=SUBSTR(f.url,1,INSTR(f.url,chr(35),1,1)-1)) where f.day=to_date('%s','yyyy-mm-dd') and INSTR(f.url,chr(35),1,1)>0 "%dateStr,"wap_update_title3" : "update wapnew.f_doc_20180521 f set title=(select title from wapnew.d_url2title d where d.url=CONCAT(SUBSTR(f.url,1,INSTR(f.url,chr(95),1,1)-1),SUBSTR(f.url,INSTR(f.url,chr(47),-1,1)))) where f.day=to_date('%s','yyyy-mm-dd') and instr(f.url,chr(95))>0 and not f.url like '%%aid=zmt_%%'"%dateStr,"wap_doc_update_src" : "update wapnew.f_doc_20180521 f set source=(select src from wapnew.d_doc_src d where d.url=f.url) where f.day=to_date('%s','yyyy-mm-dd')"%dateStr}


		try:
			table_l = ["wap_doc"]
			print database_host
			sqlExecute(table_l, wapSQLDict)
			updateExecute(updateSQLDict)
		except Exception, e:
			transport.close()
		print dateStr

		print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
		update_f_doc_title(dateStr)
		transport.close()
		print '========Wap Finished @',dateStr	
		
		sdate += 86400




