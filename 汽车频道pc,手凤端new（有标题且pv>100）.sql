--汽车频道pc端（有标题且pv>100）
select distinct doc,title,pv,dt from web.auto_doc_20180521 where ci like '%auto%' and title !='None' order by dt,pv desc  

--汽车频道手凤端（有标题且pv>100）
select distinct url,
       nvl(title, ' ') title,
       pv,
       to_char(day, 'yyyy-mm-dd') day
  from (select f.source,
               url,
               decode(chname, '', decode(ch, '0', '', ch), chname) path,
               pv,
               uv,
               title,
               day,
               rownum
          from wapnew.f_doc_20180521 f
          left join wapnew.d_path t on t.chpath = f.ch
         where day >= to_date('2018-03-01', 'yyyy-mm-dd')
         and day <=to_date('2018-05-20','yyyy-mm-dd')  
           and f.url not like '%sharenews.%'
           and version = 'overall'
           and ch like 'http://iauto.ifeng.com/' || '%'
         ) where title is not null order by day ,pv desc
