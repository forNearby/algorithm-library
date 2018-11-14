create table AV_AVERAGE_TRAVEL_FREQUENCY
(
  AREAID               VARCHAR2(20),
  FREQUENCY            VARCHAR2(5),
  PLATE_TYPE           VARCHAR2(10),
  AMOUNT           	   NUMBER
);
comment on table AV_AVERAGE_TRAVEL_FREQUENCY
  is '城市出行频度分析结果表';
comment on column AV_AVERAGE_TRAVEL_FREQUENCY.AREAID
  is '区域ID';
comment on column AV_AVERAGE_TRAVEL_FREQUENCY.FREQUENCY
  is '出行频度';
  comment on column AV_AVERAGE_TRAVEL_FREQUENCY.PLATE_TYPE
  is '号牌种类';
  comment on column AV_AVERAGE_TRAVEL_FREQUENCY.AMOUNT
  is '过车数量';
  
  
select * from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='41')
and ORI_PLATE_NO like '%川B%'
and DATASAVETIME >= to_date('20181107','yyyymmdd')-7
and DATASAVETIME < to_date('20181107','yyyymmdd')