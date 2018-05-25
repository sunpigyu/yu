--查询逻辑

select midnum,cv,ucv,vv,uvv,a.provider from (select cv,ucv,vv,uvv,key_name as provider 
     from media_type_all where dt='%s' and key='provider' and cv is not null) a join 
     (select midnum,provider from media_provider where dt='%s') b on a.provider=b.provider
     
--入库逻辑   

insert into f_provider(day,provider_id,num,cv,ucv,vv,uvv) select d.day,c.id,:1,:2,:3,:4,:5 from d_date d,d_provider c where sp_id=:6 and day=to_date(:7,'yyyy-mm-dd')


--前端查询逻辑

select provider_id,to_char(day,'yyyy-mm-dd') day,cv,vv,ucv,uvv,num,name from media.F_PROVIDER f,media.D_PROVIDER d where f.provider_id=d.id and day = to_date('2018-05-24','yyyy-mm-dd') order by num desc


--新前端查询逻辑

select case  when b.name is null then a.provider else b.name end name ,a.midnum,a.cv,a.ucv,a.vv,a.uvv,a.day from  media.f_provider_new a left join media.d_provider b on a.provider=b.sp_id where a.day=to_date('2018-05-20','yyyy-mm-dd') order by midnum desc


--批量插入
insert into media.f_provider_new (midnum,cv,ucv,vv,uvv,provider,day) select a.num,a.cv,a.ucv,a.vv,a.uvv,b.sp_id,a.day from media.f_provider a left join media.d_provider b on a.provider_id=b.id where day < to_date('2018-05-24','yyyy-mm-dd')


select * from media.d_provider where sp_id='25012'

select * from media.f_provider_new where provider='25012'


select  from media.f_provider_new a left join  media.d_provider b on a.provider=b.sp_id where a.day=date'2018-05-23'  order by midnum desc



脚本位置 188: /data/script/cron_hive/provider_new.py

oracle表 media.f_provider_new
