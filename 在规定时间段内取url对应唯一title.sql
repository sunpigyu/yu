-- 在规定时间段内取url对应唯一title
select  dd.uri uri,dd.title title,dd.src src from (select d.uri uri,
       d.title title,
       d.src src,
       row_number() over(partition by d.uri order by d.tm desc) rank
  from (select url_pc uri, title, src, tm
          from D_DOC_NEW
         where insert_time between
               to_date('2018-05-22 0:0:0', 'yyyy-mm-dd hh24:mi:ss') and
               to_date('2018-05-22 23:59:59', 'yyyy-mm-dd hh24:mi:ss'))d)dd where dd.rank=1
