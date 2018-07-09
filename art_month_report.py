# encoding:utf-8
import time
import datetime
import cx_Oracle
import os

from  GetTimeAndDate import *

os.environ["NLS_LANG"] = ".UTF8"

def db_app():
        con = cx_Oracle.connect('app','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur

def db_web():
        con = cx_Oracle.connect('web','数据库密码','数据库地址:1521/orcl')
        cur = con.cursor()
        return con,cur

def video_data():
	con_web,cur_web = db_web()
	print '内容生产 原创视频'
	print '内容','\t','发布量','\t','app','\t','video','\t','pc','\t','wap','\t','点击率','\t','week'
	sql1 = '''
select name,
       avg(c.original_num) num,
       avg(app) app,
       avg(video) video,
       avg(web) pc,
       avg(wap) wap,
       sum(app) / sum(b.app_info) appinrate,
       year_month day
  from web.d_allplat_src_type a
  join web.f_allplat_src_type b on a.src = b.type and b.type='original'
  join web.f_allplat_src_type_num c on b.day=c.day
  left join media.d_date d on b.day = d.day
 where to_char(b.day,'yyyy-mm') between '%s' and '%s'
 group by name,year_month
 order by name,day desc''' % (threemonth,onemonth)
	cur_web.execute(sql1)
	for name,num,app,video,pc,wap,inrate,week in cur_web.fetchall():
		print name,'\t',num,'\t',app,'\t',video,'\t',pc,'\t',wap,'\t',inrate,'\t',week

	print '内容生产 大风号视频、凤凰抓取视频、一点同步视频'
	print '内容','\t','发布量','\t','app','\t','video','\t','pc','\t','wap','\t','点击率','\t','week'
	sql2 = '''
select a.name,
       avg(a.num) num,
       avg(a.app) app,
       0 video,
       avg(a.pc) pc,
       avg(a.wap) wap,
       sum(a.app) / sum(a.app_info) appinrate,
       year_month day
  from (select day,name,sum(num) num,sum(wap) wap,sum(web) pc,sum(app) app,sum(app_info) app_info
          from web.f_allplat_src_type_detail a join web.d_account_type b on a.accounttype = b.accounttype
         where type = 'zmt' group by day, name) a
  left join (select day, name, sum(video) video
               from web.f_allplat_src_type_detail_v a join web.d_account_type b on a.accounttype = b.accounttype
              where type = 'video' group by day, name  order by day) c on a.day = c.day and a.name = c.name
  left join media.d_date d on a.day = d.day
 where to_char(a.day,'yyyy-mm') between '%s' and '%s'
 group by a.name,year_month order by a.name, day desc''' % (threemonth,onemonth)
	cur_web.execute(sql2)
        for name,num,app,video,pc,wap,inrate,week in cur_web.fetchall():
                print name,'\t',num,'\t',app,'\t',video,'\t',pc,'\t',wap,'\t',inrate,'\t',week

	con_app,cur_app = db_app()
	print '播放量 凤凰新闻app'
	print '播放量','\t','点击率','\t','渗透率','\t','播放时长','\t','分享页vv','\t','week'
	sql3 = '''select aaaa.pv,
       aaaa.drate,
       aaaa.srate,
       aaaa.dur,
       bbbb.pv fpv,
       aaaa.mon dmon
  from (select aaa.pv, aaa.drate, aaa.pv / bbb.apv srate, aaa.dur, bbb.mon
          from (select sum(clickpv) pv,
                       sum(clickpv) / sum(infopv) drate,
                       to_char(to_date(floor(sum(perdur)), 'sssss'),
                               'HH24:MI:SS') dur,
                       day
                  from (select p.plat,
                               p.pagetype,
                               p.name,
                               floor(avg(p.clickpv)) clickpv,
                               floor(avg(p.infopv)) infopv,
                               round(avg(p.clickpv) / avg(p.sumuv), 2) perclick,
                               round(avg(p.infopv) / avg(p.sumuv), 2) perinfo,
                               decode(avg(p.infopv),
                                      0,
                                      '',
                                      null,
                                      '',
                                      to_char(avg(p.clickpv) * 100 /
                                              avg(p.infopv),
                                              'fm99990.00') || '%%') inrate,
                               decode(avg(p.uv_in),
                                      0,
                                      '',
                                      null,
                                      '',
                                      decode(avg(p.uv_in) / avg(p.infouv),
                                             null,
                                             '',
                                             to_char((avg(p.uv_in) /
                                                     avg(p.infouv)) * 100,
                                                     'fm99990.00') || '%%')) clickuvrate,
                               avg(p.dur) / avg(p.sumuv) perdur,
                               
                               year_month day
                          from (select u.plat,
                                       u.pagetype,
                                       u.name,
                                       u.clickpv,
                                       u.infopv,
                                       u.tm,
                                       u.infouv,
                                       u.dur,
                                       e.uv sumuv,
                                       u.uv_in
                                  from (select 'all' plat,
                                               t.pagetype,
                                               d.name,
                                               sum(t.pv) clickpv,
                                               sum(f.pv) infopv,
                                               sum(r.dur) dur,
                                               t.tm,
                                               sum(n.uv) uv_in,
                                               sum(f.uv) infouv
                                          from app.f_newsapp_pagetype_click_ua t
                                          left join app.f_newsapp_pagetype_click_in_ua n on t.plat =
                                                                                            n.plat
                                                                                        and t.pagetype =
                                                                                            n.pagetype
                                                                                        and t.ua = n.ua
                                                                                        and t.tm = n.tm
                                          left join app.f_newsapp_pagetype_info_ua f on t.plat =
                                                                                        f.plat
                                                                                    and t.pagetype =
                                                                                        f.pagetype
                                                                                    and t.ua = f.ua
                                                                                    and t.tm = f.tm
                                          left join app.f_newsapp_pagetype_dur_ua r on t.plat =
                                                                                       r.plat
                                                                                   and t.pagetype =
                                                                                       r.pagetype
                                                                                   and t.ua = r.ua
                                                                                   and t.tm = r.tm
                                          join app.d_newsapp_pagetype d on t.pagetype =
                                                                           d.type
                                         where 1 = 1
                                         group by t.pagetype, d.name, t.tm) u
                                  left join app.f_news_overall e on u.tm = e.tm) p
                          left join app.d_date da on p.tm = da.day
                         where year_month >= '%s'
                           and year_month <= '%s'
                         group by p.plat, p.pagetype, p.name, year_month) aa
                 where pagetype in ('article', 'pic')
                 group by day) aaa
          left join (select round(avg(a.clickpv)) apv, b.year_month mon
                      from app.f_news_overall a
                      left join media.d_date@app_web b on a.tm = b.day
                     where b.year_month >= '%s'
                       and b.year_month <= '%s'
                     group by b.year_month) bbb on aaa.day = bbb.mon) aaaa
  left join (select floor(avg(pv)) pv, year_month day
               from app.sns_overall t
               left join app.d_date d on t.day = d.day
              where t.day between to_date('%s', 'yyyy-mm-dd') and
                    to_date('%s', 'yyyy-mm-dd')
                and datatype = 'newsappsns'
              group by year_month) bbbb on aaaa.mon = bbbb.day
 order by dmon desc''' %(threemonth,onemonth,threemonth,onemonth,threemonth_day,onemonth_day)
	cur_app.execute(sql3)
	for vv,inrate,vuv_rate,time,snsvv,week in cur_app.fetchall():
		print vv,'\t',inrate,'\t',vuv_rate,'\t',time,'\t',snsvv,'\t',week

	print '播放量 凤凰视频app'
	print '播放量','\t','点击率','\t','uv占比','\t','播放时长','\t','分享页vv','\t','week'
	sql4 = '''
select avg(f.vv) vv,
       sum(f.vv) / sum(f.infopv) inrate,
       sum(f.uvvv) / sum(f.uv) uv_rate,
       sum(f.sumlast) / sum(f.uvvv) olast,
       avg(a.pv) snsvv,
       year_month day
  from app.f_video_overall f
  join app.d_date d on f.tm = d.day
  left join app.sns_overall a on a.day=f.tm and a.datatype='videoappsns'
 where to_char(f.tm,'yyyy-mm') between '%s' and '%s'
 group by year_month order by day desc''' % (threemonth,onemonth)
	cur_app.execute(sql4)
        for vv,inrate,uv_rate,time,snsvv,week in cur_app.fetchall():
                print vv,'\t',inrate,'\t',uv_rate,'\t',time,'\t',snsvv,'\t',week

	print '播放量 凤凰网pc'
	print 'vv','\t','渗透率','\t','站外vv','\t','week'
	sql5 = '''select aa.spv,aa.spv/bb.pv srate,0 out,aa.day mday from (select sum(pic_pv+doc_pv)spv,day from (select ch url,
       floor(avg(ifeng_pv)) ifeng_pv,
       floor(avg(index_pv)) index_pv,
       floor(avg(pic_pv)) pic_pv,
       floor(avg(doc_pv)) doc_pv,
       floor(avg(other_pv)) other_pv,
       floor(avg(video_pv)) video_pv,
       year_month day
  from web.f_pagetype_site f
  join media.d_date ddate on f.day = ddate.day
 where f.day between to_date('%s', 'yyyy-mm-dd') and
       to_date('%s', 'yyyy-mm-dd')
   and f.ch = 'site'
 group by year_month, ch) group by day)aa 
 left join 
 (select 
       pv,
       day
  from (select floor(avg(pv)) pv,
               floor(avg(uv)) uv,
               year_month day,
               floor(avg(visit)) visit,
               round((avg(exit) / avg(visit)) * 100, 2) || '%%' exit,
               floor(avg(abs(dur * visit / uv))) dur,
               to_char(to_date(floor(avg(abs(dur * visit / uv))), 'sssss'),
                       'HH24:MI:SS') durstr
          from web.f_overall f
          join media.d_date ddate on f.day = ddate.day
         where f.type = 'all'
           and f.day between to_date('%s', 'yyyy-mm-dd') and
               to_date('%s', 'yyyy-mm-dd')
         group by year_month))bb on aa.day=bb.day order by mday desc''' % (threemonth_day,onemonth_day,threemonth_day,onemonth_day)
	cur_web.execute(sql5)
	for vv,vuv_rate,outsite,week in cur_web.fetchall():
		print vv,'\t',vuv_rate,'\t',outsite,'\t',week

	print '播放量 手凤'
	print 'vv','\t','渗透率','\t','站外vv','\t','week'
	sql6 = '''select aa.spv,aa.spv/bb.mpv srate,0 out,aa.day from (select sum(pic_pv+doc_pv)spv ,day from (select f.ch url,
       nvl(d.chname, ' ') name,
       floor(avg(index_pv)) index_pv,
       floor(avg(pic_pv)) pic_pv,
       floor(avg(video_pv)) video_pv,
       floor(avg(doc_pv)) doc_pv,
       floor(avg(ifeng_pv)) ifeng_pv,
       floor(avg(special_pv)) special_pv,
       floor(avg(other_pv)) other_pv,
       year_month day
  from wapnew.f_pagetype_inner_new f
  join media.d_date ddate on f.day = ddate.day
  left join wapnew.d_path d on d.chpath = f.ch
 where f.day between to_date('%s', 'yyyy-mm-dd') and
       to_date('%s', 'yyyy-mm-dd')
   and f.ch = 'site'
 group by year_month, f.ch, d.chname) group by day)aa
left join   
(select floor(avg(major_pv)) mpv,
   year_month day
  from wapnew.f_overall f
  left join (select sum(pv) cp_pv, day
               from wapnew.f_ch_partner
              where day between to_date('%s', 'yyyy-mm-dd') and
                    to_date('%s', 'yyyy-mm-dd')
              group by day) cp on cp.day = f.day
  join media.d_date ddate on f.day = ddate.day
 where f.day between to_date('%s', 'yyyy-mm-dd') and
       to_date('%s', 'yyyy-mm-dd')
 group by year_month)bb on aa.day=bb.day order by day desc''' %(threemonth_day,onemonth_day,threemonth_day,onemonth_day,threemonth_day,onemonth_day) 
	cur_web.execute(sql6)
        for vv,vuv_rate,outsite,week in cur_web.fetchall():
                print vv,'\t',vuv_rate,'\t',outsite,'\t',week

	print '频道'
	print '频道','\t','播放总量','\t','新闻app','\t','视频客户端','\t','手凤','\t','pc'
	sql7 = '''select * from (select ccc.ch,bbb.ppv ppv,ccc.app_pv,ccc.video_pv,ccc.wap_pv,ccc.web_pv from (select aaa.ch,aaa.app_pv,aaa.video_pv,aaa.wap_pv,aaa.web_pv from (select ch,
       floor(avg(sumnum)) sumnum,
       floor(avg(original_num)) original_num,
       floor(avg(zmt_num)) zmt_num,
       floor(avg(othsrc_num)) othsrc_num,
       floor(avg(sumpv)) sumpv,
       floor(avg(web_pv)) web_pv,
       floor(avg(wap_pv)) wap_pv,
       floor(avg(app_pv)) app_pv,
       floor(avg(video_pv)) video_pv,
       floor(avg(newssns_pv)) newssns_pv,
       floor(avg(videosns_pv)) videosns_pv,
       year_month day
  from (select decode(chname, null, '未分类', chname) ch,
               sum(nvl(original_num, 0) + nvl(zmt_num, 0) +
                   nvl(othsrc_num, 0)) sumnum,
               sum(original_num) original_num,
               sum(zmt_num) zmt_num,
               sum(othsrc_num) othsrc_num,
               sum(nvl(web_pv, 0) + nvl(wap_pv, 0) + nvl(app_pv, 0) +
                   nvl(video_pv, 0) + nvl(newssns_pv, 0) +
                   nvl(videosns_pv, 0)) sumpv,
               sum(web_pv) web_pv,
               sum(wap_pv) wap_pv,
               sum(app_pv) app_pv,
               sum(video_pv) video_pv,
               sum(newssns_pv) newssns_pv,
               sum(videosns_pv) videosns_pv,
               day
          from web.f_allplat_ch_new a
          left join web.d_cmppid_path b on a.ch = b.id5
         group by decode(chname, null, '未分类', chname), day) a
  left join media.d_date ddate on a.day = ddate.day
 where year_month = '2018-04'
 group by ch, year_month)aaa where aaa.ch!='视频频道')ccc
 left join  
 (select ch,sum(web_pv+wap_pv+app_pv+video_pv) ppv from (select ch,
       floor(avg(sumnum)) sumnum,
       floor(avg(original_num)) original_num,
       floor(avg(zmt_num)) zmt_num,
       floor(avg(othsrc_num)) othsrc_num,
       floor(avg(sumpv)) sumpv,
       floor(avg(web_pv)) web_pv,
       floor(avg(wap_pv)) wap_pv,
       floor(avg(app_pv)) app_pv,
       floor(avg(video_pv)) video_pv,
       floor(avg(newssns_pv)) newssns_pv,
       floor(avg(videosns_pv)) videosns_pv,
       year_month day
  from (select decode(chname, null, '未分类', chname) ch,
               sum(nvl(original_num, 0) + nvl(zmt_num, 0) +
                   nvl(othsrc_num, 0)) sumnum,
               sum(original_num) original_num,
               sum(zmt_num) zmt_num,
               sum(othsrc_num) othsrc_num,
               sum(nvl(web_pv, 0) + nvl(wap_pv, 0) + nvl(app_pv, 0) +
                   nvl(video_pv, 0) + nvl(newssns_pv, 0) +
                   nvl(videosns_pv, 0)) sumpv,
               sum(web_pv) web_pv,
               sum(wap_pv) wap_pv,
               sum(app_pv) app_pv,
               sum(video_pv) video_pv,
               sum(newssns_pv) newssns_pv,
               sum(videosns_pv) videosns_pv,
               day
          from web.f_allplat_ch_new a
          left join web.d_cmppid_path b on a.ch = b.id5
         group by decode(chname, null, '未分类', chname), day) a
  left join media.d_date ddate on a.day = ddate.day
 where year_month = '%s'
 group by ch, year_month)aaa where aaa.ch!='视频频道' group by ch) bbb on ccc.ch=bbb.ch where ccc.ch!='未分类' and ccc.ch!='汽车频道' order by ppv desc) where rownum<12''' % (onemonth,)
	cur_web.execute(sql7)
	for name,vv,app,video,wap,pc in cur_web.fetchall():
		print name,'\t',vv,'\t',app,'\t',video,'\t',wap,'\t',pc

	print '稿源'
	print '稿源','\t','发文量','\t','播放量','\t','点击率'
	sql8 = '''select * from (select *
  from (select cate,
               avg(num) num,
               avg(uv) uv,
               case when sum(info) = 0 then 0 else sum(app_uv) / sum(info) end inrate
          from (select day,
                       case when b.cate is null then a.src else b.cate end cate,
                       sum(c.num) num,
                       sum(uv) as uv,
                       sum(app_uv) as app_uv,
                       sum(appinfo) as info
                  from web.f_doc_src_allplat_orc a
                  left join web.d_src_cate b on a.src = b.src
                  left join (select src, count(id) num,to_char(tm, 'yyyy-mm-dd') tm from web.d_doc_new group by to_char(tm, 'yyyy-mm-dd'), src) c 
                                        on a.src = c.src and to_char(a.day, 'yyyy-mm-dd') = c.tm
                 group by day,case when b.cate is null then a.src else b.cate end) aa
         where to_char(day,'yyyy-mm') = '%s'
         group by cate order by uv desc) aa where aa.cate !='None' and aa.cate !='none' and aa.cate != '#')aaa
 where rownum < 12''' % (onemonth,)
	cur_web.execute(sql8)
        for cate,num,uv,inrate in cur_web.fetchall():
                print cate,'\t',num,'\t',uv,'\t',inrate


        print '内容生产 机构稿源'
        print '内容','\t','发布量','\t','app','\t','video','\t','pc','\t','wap','\t','点击率','\t','week'
        sql9 = '''
select name,
       avg(c.othsrc_num) num,
       avg(app) app,
       avg(video) video,
       avg(web) pc,
       avg(wap) wap,
       sum(app) / sum(b.app_info) appinrate,
       year_month day
  from web.d_allplat_src_type a
  join web.f_allplat_src_type b on a.src = b.type and b.type='othsrc'
  join web.f_allplat_src_type_num c on b.day=c.day
  left join media.d_date d on b.day = d.day
 where to_char(b.day,'yyyy-mm') between '%s' and '%s'
 group by name,year_month
 order by name,day desc''' % (threemonth,onemonth)
        cur_web.execute(sql9)
        for name,num,app,video,pc,wap,inrate,week in cur_web.fetchall():
                print name,'\t',num,'\t',app,'\t',video,'\t',pc,'\t',wap,'\t',inrate,'\t',week





	con_web.close()
	con_app.close()
if __name__ == '__main__':


	threemonth,threemonth_day,threemonth_day_end = getMonthFirstDayAndLastDay(-3)

	onemonth,onemonth_day_start,onemonth_day = getMonthFirstDayAndLastDay(-1)


	video_data()
	print onemonth,threemonth
	print threemonth_day,onemonth_day



