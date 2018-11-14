select * from ts_fbd_device where fbd='FBD_01C_09'

select * from TD_VEHICLE_RECORD_DAY 

select cast(avg(speed) as decimal(5,2)) from TD_VEHICLE_RECORD_DAY where DEVICEID in (select DEVICEID from ts_fbd_device where fbd = 'FBD_01C_09') 

--------------
--round(avg(speed),2)
--25 29
select  round(avg(speed),2) from TD_VEHICLE_RECORD_DAY
where to_char(DATASAVETIME,'hh24:mi:ss')>='06:00:00'
and to_char(DATASAVETIME,'hh24:mi:ss')<'06:30:00'  
and DATASAVETIME >= to_date('20181030','yyyymmdd')-15
and datasavetime < to_date('20181030','yyyymmdd')
and DEVICEID in (select DEVICEID from ts_fbd_device where fbd = 'FBD_01C_10')
--order by DATASAVETIME desc
-------------------------

--and DATASAVETIME>=to_date('2018-10-31 06:00:00','yy-mm-dd hh24:mi:ss') +5/(24*60)*0  
--and DATASAVETIME<to_date('2018-10-31 06:00:00','yy-mm-dd hh24:mi:ss')+ 5/(24*60)*1 

select DATASAVETIME from TD_VEHICLE_RECORD_DAY where DEVICEID in (select DEVICEID from ts_fbd_device where fbd = 'FBD_01C_09')
and DATASAVETIME>=to_date('2018-10-31 06:00:00','yy-mm-dd hh24:mi:ss')
and DATASAVETIME<to_date('2018-10-31 06:05:00','yy-mm-dd hh24:mi:ss')

select * from ts_fbd_info where fbd='FBD_01C_10'
alter table ts_fbd_info add (free_flow_speed number(5,2) ,compute_time date)
--td_free_flow_speed
create table td_free_flow_speed(roadid varchar2(32),speed number(5,2), computetime date)
insert into td_free_flow_speed (roadid,speed,computetime) values('FBD_01C_09',80.00, to_date ( '2018-10-31 06:00:00' , 'yy-mm-dd hh24:mi:ss' ))
insert into td_free_flow_speed (roadid,speed,computetime) values('FBD_01C_09',81.00, to_date ( '2018-10-30 06:00:00' , 'yy-mm-dd hh24:mi:ss' ))
select * from td_free_flow_speed
select * from td_free_flow_speed where roadid='FBD_01C_09'