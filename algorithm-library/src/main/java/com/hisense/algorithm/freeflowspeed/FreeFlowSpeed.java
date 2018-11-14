package com.hisense.algorithm.freeflowspeed;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hisense.dao.DaoUtil;

/**
 * 自有留速度算法
 * @author qiushuai
 *
 */
public class FreeFlowSpeed {

	private static Logger logger = Logger.getLogger(FreeFlowSpeed.class);
	
	public static void main(String[] args) {
		String json = args[0];
		//String json = "{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"},\"TS_FBD_INFO\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_FBD_INFO\"}},\"operational_param\":{\"fdb\":\"FBD_01C_09\"},\"output\":{\"TS_FBD_INFO\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_FBD_INFO\"}},\"spark_info\":{\"master\":\"local\"}}";
		//String json = "eyJkYXRhc291cmNlIjp7IlREX1ZFSElDTEVfUkVDT1JEX0RBWSI6eyJhY2NvdW50IjoiZ3hkYiIsImRhdGFiYXNlX2FkZHJlc3MiOiIyMC4yLjE1LjE4IiwidXNlciI6IkpIR1giLCJwYXNzd29yZCI6IkpIR1gwMDEiLCJ0YWJsZW5hbWUiOiJURF9WRUhJQ0xFX1JFQ09SRF9EQVkifSwiVFNfRkJEX0lORk8iOnsiYWNjb3VudCI6Imd4ZGIiLCJkYXRhYmFzZV9hZGRyZXNzIjoiMjAuMi4xNS4xOCIsInVzZXIiOiJKSEdYIiwicGFzc3dvcmQiOiJKSEdYMDAxIiwidGFibGVuYW1lIjoiVFNfRkJEX0lORk8ifX0sIm9wZXJhdGlvbmFsX3BhcmFtIjp7ImZkYiI6IkZCRF8wMUNfMDkifSwib3V0cHV0Ijp7IlRTX0ZCRF9JTkZPIjp7ImFjY291bnQiOiJneGRiIiwiZGF0YWJhc2VfYWRkcmVzcyI6IjIwLjIuMTUuMTgiLCJ1c2VyIjoiSkhHWCIsInBhc3N3b3JkIjoiSkhHWDAwMSIsInRhYmxlbmFtZSI6IlRTX0ZCRF9JTkZPIn19LH0=";
		json = new String(java.util.Base64.getDecoder().decode(json));
		logger.info("json字符串:"+json);
		JSONObject parseObject = JSON.parseObject(json);
		String fbd = parseObject.getJSONObject("operational_param").getString("fdb");
		//String master = parseObject.getJSONObject("spark_info").getString("master");
		String master ="local";
		String ip = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("database_address");
		String account = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("database");
		String url = "jdbc:oracle:thin:@//"+ip+":1521/"+account+"";
		String user = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("account");
		String password = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("password");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DaoUtil daoUtil = new DaoUtil();
		logger.info("获取到发布段为:"+fbd+"");
		//查询该发布段是否有历史记录
		Dataset<Row> table2 = daoUtil.getTable("(select * from ts_fbd_info where fbd='"+fbd+"') dbtable",master,url,user,password);
		List<Row> collectAsList2 = table2.collectAsList();
		Object object = collectAsList2.get(0).get(7);
		if(object==null){
			//没有历史记录
			logger.info("算法开始时间:"+new Date());
			double[] avgSpeed = new double[36];
			String ymd = getYMD();
			String startTime ="06:00:00";
			for(int i=0;i<=35;i++){
				String endTime = getHMS(startTime,30);
				if(i==35)
					endTime="23:59:59";
				String sql = "select  round(avg(speed),2) from TD_VEHICLE_RECORD_DAY where to_char(DATASAVETIME,'hh24:mi:ss')>='"+startTime+"' and to_char(DATASAVETIME,'hh24:mi:ss')<'"+endTime+"' and DATASAVETIME >= to_date('"+ymd+"','yyyymmdd')-15 and DATASAVETIME < to_date('"+ymd+"','yyyymmdd') and DEVICEID in (select DEVICEID from ts_fbd_device where fbd = '"+fbd+"')";
				//sql:select  DATASAVETIME from TD_VEHICLE_RECORD_DAY where to_char(DATASAVETIME,'hh24:mi:ss')>='06:00:00' and to_char(DATASAVETIME,'hh24:mi:ss')<'06:30:00' and DATASAVETIME >= to_date('20181025','yyyymmdd')-15 and DATASAVETIME < to_date('20181025','yyyymmdd') and DEVICEID in (select DEVICEID from ts_fbd_device where fbd = ' fbd')
				Dataset<Row> table = daoUtil.getTable("("+sql+") dbtable",master,url,user,password);
				List<Row> collectAsList = table.collectAsList();
				BigDecimal tmp = (BigDecimal) collectAsList.get(0).get(0) ;
				avgSpeed[i] = tmp.doubleValue();
				logger.info("第"+(i+1)+"个片段平均值为:"+avgSpeed[i]);
				startTime =endTime;
			}
			//升序排列
			Arrays.sort(avgSpeed);
			//取最大的1/9个片段求平均值
			double sum = 0.0;
			for(int i = 35 ;i>=32;i--){
				double tAvg = avgSpeed[i];
				sum = new BigDecimal(sum).add(new BigDecimal(tAvg)).doubleValue();
			}
			BigDecimal b1 = new BigDecimal(sum);
			BigDecimal b2 = new BigDecimal(4.0);
			double freeSpeed= b1.divide(b2,2,BigDecimal.ROUND_HALF_UP).doubleValue();
			logger.info("该发布段自由流速度为:"+freeSpeed);
			logger.info("算法结束时间:"+new Date());
			String nowTime = sdf.format(new Date());
			//update ts_fbd_info set free_flow_speed=9.0,computetime=to_date('2018-10-31 06:00:00','yy-mm-dd hh24:mi:ss') where fbd='FBD_01C_09'
			String sql = "update ts_fbd_info set free_flow_speed="+freeSpeed+",compute_time=to_date('"+nowTime+"','yy-mm-dd hh24:mi:ss') where fbd='"+fbd+"'";
			//daoUtil.update(sql, "20.2.15.18", "gxdb", "JHGX", "JHGX001");
			daoUtil.update(sql, parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("database_address"), parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("database"), parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("account"), parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("password"),"1521");
		}else{
			//该发布段有历史值
			logger.info("算法开始时间:"+new Date());
			BigDecimal oldSpeed = (BigDecimal) collectAsList2.get(0).get(7);
			String yymmdd = getYMDdelayOneDay();
			//查询当天速度平均值
			//select round(avg(speed),2) from TD_VEHICLE_RECORD_DAY where DEVICEID in (select DEVICEID from ts_fbd_device where fbd = 'FBD_01C_09') and DATASAVETIME>=to_date('2018-10-31 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME<to_date('2018-10-31 23:59:59','yy-mm-dd hh24:mi:ss')
			Dataset<Row> table = daoUtil.getTable("(select round(avg(speed),2) from TD_VEHICLE_RECORD_DAY where DEVICEID in (select DEVICEID from ts_fbd_device where fbd = '"+fbd+"') and DATASAVETIME>=to_date('"+yymmdd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME<to_date('"+yymmdd+" 23:59:59','yy-mm-dd hh24:mi:ss')) dbtable",master,url,user,password);
			List<Row> collectAsList = table.collectAsList();
			BigDecimal newSpeed = (BigDecimal) collectAsList.get(0).get(0);
			BigDecimal oldSpeedCount = oldSpeed.multiply(new BigDecimal(15.0));
			BigDecimal freeSpeed = oldSpeedCount.add(newSpeed).divide(new BigDecimal(16.0));
			logger.info("该发布段自由流速度为:"+freeSpeed);
			logger.info("算法结束时间:"+new Date());
			String nowTime = sdf.format(new Date());
			//update ts_fbd_info set free_flow_speed=9.0,computetime=to_date('2018-10-31 06:00:00','yy-mm-dd hh24:mi:ss') where fbd='FBD_01C_09'
			String sql = "update ts_fbd_info set free_flow_speed="+freeSpeed+",compute_time=to_date('"+nowTime+"','yy-mm-dd hh24:mi:ss') where fbd='"+fbd+"'";
			//daoUtil.update(sql, "20.2.15.18", "gxdb", "JHGX", "JHGX001");
			daoUtil.update(sql, parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("database_address"), parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("database"), parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("account"), parseObject.getJSONObject("output").getJSONObject("TS_FBD_INFO").getString("password"),"1521");
		}
		
	}
	
	//获取当前时间的年月日字符串
	private static String getYMD(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, 1);
		int year = cal.get(Calendar.YEAR);
		String yearS = year+"";
		int month = cal.get(Calendar.MONTH);
		String monthS ="";
		if(month<=9){
			monthS = "0"+month;
		}else{
			monthS = month+"";
		}
		String dateS = "";
		int date = cal.get(Calendar.DATE);
		if(date<=9){
			dateS = "0"+date;
		}else{
			dateS = date+"";
		}
		return yearS +monthS + dateS;
	}
	
	//获取当前时间的年月日字符串倒推一天
	private static String getYMDdelayOneDay(){
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.MONTH, 1);
		cal.add(Calendar.DAY_OF_YEAR, -1);
		int year = cal.get(Calendar.YEAR);
		String yearS = year+"";
		int month = cal.get(Calendar.MONTH);
		String monthS ="";
		if(month<=9){
			monthS = "0"+month;
		}else{
			monthS = month+"";
		}
		String dateS = "";
		int date = cal.get(Calendar.DATE);
		if(date<=9){
			dateS = "0"+date;
		}else{
			dateS = date+"";
		}
		return yearS +"-"+monthS +"-"+ dateS;
	}
	
	//获取从输入的开始时间叠加时间片段的时间字符串
	private static String getHMS(String startTime,int offsetMinute){
		int hour = Integer.valueOf(startTime.split(":")[0]);
		int minute = Integer.valueOf(startTime.split(":")[1]);
		int second = Integer.valueOf(startTime.split(":")[2]);
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		cal.add(Calendar.MINUTE, offsetMinute);
		String hourS = "";
		String minuteS = "";
		String secondS = "";
		if(cal.get(Calendar.HOUR_OF_DAY)<=9){
			hourS = "0"+cal.get(Calendar.HOUR_OF_DAY);
		}else{
			hourS = cal.get(Calendar.HOUR_OF_DAY)+"";
		}
		if(cal.get(Calendar.MINUTE)<=9){
			minuteS = "0" +cal.get(Calendar.MINUTE);
		}else{
			minuteS = cal.get(Calendar.MINUTE)+"";
		}
		if(cal.get(Calendar.SECOND)<=9){
			secondS = "0"+cal.get(Calendar.SECOND);
		}else{
			secondS = cal.get(Calendar.SECOND)+"";
		}
		return hourS+":"+minuteS+":"+secondS;
	}

}