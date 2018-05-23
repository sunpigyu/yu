批量更新表title（离线）
update web.f_doc f
   set title = (select title
                  from (select url_pc,title,src,insert_time
                          from (select url_pc,
                                       title,
                                       src,
                                       insert_time,
                                       row_number() over(partition by url_pc order by insert_time desc) rank
                                  from web.d_doc_new)
                         where rank = 1) d
                 where f.url = d.url_pc)                
 where f.day = to_date('2018-05-16', 'yyyy-mm-dd')
 

批量插入表（离线）
delete from  web.f_doc_sort where day=date'2018-05-16'
insert into web.f_doc_sort select day,title,url,pv,ci from web.f_doc where title is not null and day = to_date('2018-05-16','yyyy-mm-dd')


批量更新表title（实时）
 update web.rt_pt_doc_history f
   set title = (select title
                  from (select title,url_pc
                          from (select url_pc,
                                       title, 
                                       row_number() over(partition by url_pc order by tm desc) rank
                                  from web.d_doc_new)
                         where rank = 1) d
                where f.url = d.url_pc)
                
