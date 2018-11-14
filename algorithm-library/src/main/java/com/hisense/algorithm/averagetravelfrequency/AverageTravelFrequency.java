package com.hisense.algorithm.averagetravelfrequency;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hisense.algorithm.freeflowspeed.FreeFlowSpeed;
import com.hisense.dao.DaoUtil;

/**
 * 城市平均出行频度
 * @author qiushuai
 *
 */
public class AverageTravelFrequency {
	
	private static Logger logger = Logger.getLogger(AverageTravelFrequency.class);
	
	public static void main(String[] args) {
		logger.info("算法开始时间:"+new Date());
		String json = args[0];
		//String json ="eyJkYXRhc291cmNlIjp7IlREX1ZFSElDTEVfUkVDT1JEX0RBWSI6eyJhY2NvdW50IjoiZ3hkYiIsImRhdGFiYXNlX2FkZHJlc3MiOiIyMC4yLjE1LjE4IiwidXNlciI6IkpIR1giLCJwYXNzd29yZCI6IkpIR1gwMDEiLCJ0YWJsZW5hbWUiOiJURF9WRUhJQ0xFX1JFQ09SRF9EQVkifSwiVFNfREVWSUNFIjp7ImFjY291bnQiOiJneGRiIiwiZGF0YWJhc2VfYWRkcmVzcyI6IjIwLjIuMTUuMTgiLCJ1c2VyIjoiSkhHWCIsInBhc3N3b3JkIjoiSkhHWDAwMSIsInRhYmxlbmFtZSI6IlRTX0RFVklDRSJ9fSwib3BlcmF0aW9uYWxfcGFyYW0iOnsiYXJlYWlkIjoiNDEiLH0sIm91dHB1dCI6eyJBVl9BVkVSQUdFX1RSQVZFTF9GUkVRVUVOQ1kiOnsiYWNjb3VudCI6Imd4ZGIiLCJkYXRhYmFzZV9hZGRyZXNzIjoiMjAuMi4xNS4xOCIsInVzZXIiOiJKSEdYIiwicGFzc3dvcmQiOiJKSEdYMDAxIiwidGFibGVuYW1lIjoiQVZfQVZFUkFHRV9UUkFWRUxfRlJFUVVFTkNZIn19LH0=";
		json = new String(java.util.Base64.getDecoder().decode(json));
		logger.info("json字符串:"+json);
		JSONObject parseObject = JSON.parseObject(json);
		String areaid = parseObject.getJSONObject("operational_param").getString("areaid");
		//String master = parseObject.getJSONObject("spark_info").getString("master");
		String master ="local";
		String ip = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("database_address");
		String account = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("database");
		String url = "jdbc:oracle:thin:@//"+ip+":1521/"+account+"";
		String user = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("account");
		String password = parseObject.getJSONObject("datasource").getJSONObject("TD_VEHICLE_RECORD_DAY").getString("password");
		
		String outIp = parseObject.getJSONObject("output").getJSONObject("AV_AVERAGE_TRAVEL_FREQUENCY").getString("database_address");
		String outDBName = parseObject.getJSONObject("output").getJSONObject("AV_AVERAGE_TRAVEL_FREQUENCY").getString("database");
		String outUser = parseObject.getJSONObject("output").getJSONObject("AV_AVERAGE_TRAVEL_FREQUENCY").getString("account");
		String outPassword = parseObject.getJSONObject("output").getJSONObject("AV_AVERAGE_TRAVEL_FREQUENCY").getString("password");
		
		DaoUtil daoUtil = new DaoUtil();
		BigDecimal[][] data = new BigDecimal[7][12];
		for(int i =1;i<=7;i++){
			String ymd = getYMD(-i);
			
			//川A车辆（包含出租车）
			String sql = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川A%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and ORI_PLATE_NO!='--------'";
			Dataset<Row> table = daoUtil.getTable("("+sql+") dbtable", master, url, user, password);
			List<Row> collectAsList = table.collectAsList();
			BigDecimal count = (BigDecimal) collectAsList.get(0).get(0);
			logger.info("第"+i+"天,第一种车数量:"+count);
			data[i-1][0] = count;
			//川A车辆（包含出租车）一次检测
			String sqlOne = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川A%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and is_normal='3'";
			Dataset<Row> tableOne = daoUtil.getTable("("+sqlOne+") dbtable", master, url, user, password);
			List<Row> collectAsListOne = tableOne.collectAsList();
			BigDecimal countOne = (BigDecimal) collectAsListOne.get(0).get(0);
			logger.info("第"+i+"天,第一种车一次检测数量:"+countOne);
			data[i-1][1] = countOne;
			//川B车辆
			String sql1 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川B%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and ORI_PLATE_NO!='--------'";
			Dataset<Row> table1 = daoUtil.getTable("("+sql1+") dbtable", master, url, user, password);
			List<Row> collectAsList1 = table1.collectAsList();
			BigDecimal count1 = (BigDecimal) collectAsList1.get(0).get(0);
			logger.info("第"+i+"天,第二种车数量:"+count1);
			data[i-1][2] = count1;
			//川B车辆 一次检测
			String sqlOne1 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川B%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and is_normal='3'";
			Dataset<Row> tableOne1 = daoUtil.getTable("("+sqlOne1+") dbtable", master, url, user, password);
			List<Row> collectAsListOne1 = tableOne1.collectAsList();
			BigDecimal countOne1 = (BigDecimal) collectAsListOne1.get(0).get(0);
			logger.info("第"+i+"天,第二种车一次检测数量:"+countOne1);
			data[i-1][3] = countOne1;
			//川C车辆
			String sql2 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川C%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and ORI_PLATE_NO!='--------'";
			Dataset<Row> table2 = daoUtil.getTable("("+sql2+") dbtable", master, url, user, password);
			List<Row> collectAsList2 = table2.collectAsList();
			BigDecimal count2 = (BigDecimal) collectAsList2.get(0).get(0);
			logger.info("第"+i+"天,第三种车数量:"+count2);
			data[i-1][4] = count2;
			//川C车辆 一次检测
			String sqlOne2 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川C%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and is_normal='3'";
			Dataset<Row> tableOne2 = daoUtil.getTable("("+sqlOne2+") dbtable", master, url, user, password);
			List<Row> collectAsListOne2 = tableOne2.collectAsList();
			BigDecimal countOne2 = (BigDecimal) collectAsListOne2.get(0).get(0);
			logger.info("第"+i+"天,第三种车一次检测数量:"+countOne2);
			data[i-1][5] = countOne2;
			//川非ABC车辆
			String sql3 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO not like '%川A%' and ORI_PLATE_NO not like '%川B%' and ORI_PLATE_NO not like '%川C%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and ORI_PLATE_NO!='--------'";
			Dataset<Row> table3 = daoUtil.getTable("("+sql3+") dbtable", master, url, user, password);
			List<Row> collectAsList3 = table3.collectAsList();
			BigDecimal count3 = (BigDecimal) collectAsList3.get(0).get(0);
			logger.info("第"+i+"天,第四种数量:"+count3);
			data[i-1][6] = count3;
			//川非ABC车辆 一次检测
			String sqlOne3 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO not like '%川A%' and ORI_PLATE_NO not like '%川B%' and ORI_PLATE_NO not like '%川C%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and is_normal='3'";
			Dataset<Row> tableOne3 = daoUtil.getTable("("+sqlOne3+") dbtable", master, url, user, password);
			List<Row> collectAsListOne3 = tableOne3.collectAsList();
			BigDecimal countOne3 = (BigDecimal) collectAsListOne3.get(0).get(0);
			logger.info("第"+i+"天,第四种车一次检测数量:"+countOne3);
			data[i-1][7] = countOne3;
			//非川车辆
			String sql4 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO not like '%川%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and ORI_PLATE_NO!='--------'";
			Dataset<Row> table4 = daoUtil.getTable("("+sql4+") dbtable", master, url, user, password);
			List<Row> collectAsList4 = table4.collectAsList();
			BigDecimal count4 = (BigDecimal) collectAsList4.get(0).get(0);
			logger.info("第"+i+"天,第五种车数量:"+count4);
			data[i-1][8] = count4;
			//非川车辆  一次检测
			String sqlOne4 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO not like '%川%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and is_normal='3'";
			Dataset<Row> tableOne4 = daoUtil.getTable("("+sqlOne4+") dbtable", master, url, user, password);
			List<Row> collectAsListOne4 = tableOne4.collectAsList();
			BigDecimal countOne4 = (BigDecimal) collectAsListOne4.get(0).get(0);
			logger.info("第"+i+"天,第五种车一次检测数量:"+countOne4);
			data[i-1][9] = countOne4;
			//川A出租车
			String sql5 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川AT%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and ORI_PLATE_NO!='--------'";
			Dataset<Row> table5 = daoUtil.getTable("("+sql5+") dbtable", master, url, user, password);
			List<Row> collectAsList5 = table5.collectAsList();
			BigDecimal count5 = (BigDecimal) collectAsList5.get(0).get(0);
			logger.info("第"+i+"天,第六种车数量:"+count5);
			data[i-1][10] = count5;
			//川A出租车  一次检测
			String sqlOne5 = "select count(1) from TD_VEHICLE_RECORD_DAY where DEVICEID in(select deviceid from ts_device where AREAID='"+areaid+"') and ORI_PLATE_NO like '%川AT%' and DATASAVETIME >= to_date('"+ymd+" 06:00:00','yy-mm-dd hh24:mi:ss') and DATASAVETIME < to_date('"+ymd+" 23:59:59','yy-mm-dd hh24:mi:ss') and is_normal='3'";
			Dataset<Row> tableOne5 = daoUtil.getTable("("+sqlOne5+") dbtable", master, url, user, password);
			List<Row> collectAsListOne5 = tableOne5.collectAsList();
			BigDecimal countOne5 = (BigDecimal) collectAsListOne5.get(0).get(0);
			logger.info("第"+i+"天,第六种车一次检测数量:"+countOne5);
			data[i-1][11] = countOne5;
		}
		
		
		//计算7天的修正系数
		BigDecimal[] revise = new BigDecimal[7];
		for(int i=0;i<=6;i++){
			BigDecimal kt = data[i][1].add(data[i][3]).add(data[i][5]).add(data[i][7]).add(data[i][9]).add(data[i][11]);
			BigDecimal mt = data[i][0].add(data[i][2]).add(data[i][4]).add(data[i][6]).add(data[i][8]).add(data[i][10]);
			BigDecimal b = mt.subtract(kt).divide(mt,2,BigDecimal.ROUND_HALF_UP);
			revise[i] = b;
			logger.info("第"+(i+1)+"天的修正系数为:"+b);
		}
		//计算六种车数量
		BigDecimal[] result = new BigDecimal[6];
		result[0] = (revise[0].multiply(data[0][0])).add(revise[1].multiply(data[1][0])).add(revise[2].multiply(data[2][0])).add(revise[3].multiply(data[3][0])).add(revise[4].multiply(data[4][0])).add(revise[5].multiply(data[5][0])).add(revise[6].multiply(data[6][0]));
		result[1] = (revise[0].multiply(data[0][1])).add(revise[1].multiply(data[1][1])).add(revise[2].multiply(data[2][1])).add(revise[3].multiply(data[3][1])).add(revise[4].multiply(data[4][1])).add(revise[5].multiply(data[5][1])).add(revise[6].multiply(data[6][1]));
		result[2] = (revise[0].multiply(data[0][2])).add(revise[1].multiply(data[1][2])).add(revise[2].multiply(data[2][2])).add(revise[3].multiply(data[3][2])).add(revise[4].multiply(data[4][2])).add(revise[5].multiply(data[5][2])).add(revise[6].multiply(data[6][2]));
		result[3] = (revise[0].multiply(data[0][3])).add(revise[1].multiply(data[1][3])).add(revise[2].multiply(data[2][3])).add(revise[3].multiply(data[3][3])).add(revise[4].multiply(data[4][3])).add(revise[5].multiply(data[5][3])).add(revise[6].multiply(data[6][3]));
		result[4] = (revise[0].multiply(data[0][4])).add(revise[1].multiply(data[1][4])).add(revise[2].multiply(data[2][4])).add(revise[3].multiply(data[3][4])).add(revise[4].multiply(data[4][4])).add(revise[5].multiply(data[5][4])).add(revise[6].multiply(data[6][4]));
		result[5] = (revise[0].multiply(data[0][5])).add(revise[1].multiply(data[1][5])).add(revise[2].multiply(data[2][5])).add(revise[3].multiply(data[3][5])).add(revise[4].multiply(data[4][5])).add(revise[5].multiply(data[5][5])).add(revise[6].multiply(data[6][5]));
		for(int i=0;i<=5;i++){
			logger.info("第"+(i+1)+"种车数量为："+result[i]);
		}
		String sql1 = "insert into AV_AVERAGE_TRAVEL_FREQUENCY (AREAID,FREQUENCY,PLATE_TYPE,AMOUNT) values('"+areaid+"','7','川A',"+result[0]+")";
		daoUtil.update(sql1, outIp, outDBName, outUser, outPassword,"1521");
		
		String sql2 = "insert into AV_AVERAGE_TRAVEL_FREQUENCY (AREAID,FREQUENCY,PLATE_TYPE,AMOUNT) values('"+areaid+"','7','川B',"+result[1]+")";
		daoUtil.update(sql2, outIp, outDBName, outUser, outPassword,"1521");
		
		String sql3 = "insert into AV_AVERAGE_TRAVEL_FREQUENCY (AREAID,FREQUENCY,PLATE_TYPE,AMOUNT) values('"+areaid+"','7','川C',"+result[2]+")";
		daoUtil.update(sql3, outIp, outDBName, outUser, outPassword,"1521");
		
		String sql4 = "insert into AV_AVERAGE_TRAVEL_FREQUENCY (AREAID,FREQUENCY,PLATE_TYPE,AMOUNT) values('"+areaid+"','7','川非ABC',"+result[3]+")";
		daoUtil.update(sql4, outIp, outDBName, outUser, outPassword,"1521");
		
		String sql5 = "insert into AV_AVERAGE_TRAVEL_FREQUENCY (AREAID,FREQUENCY,PLATE_TYPE,AMOUNT) values('"+areaid+"','7','非川',"+result[4]+")";
		daoUtil.update(sql5, outIp, outDBName, outUser, outPassword,"1521");
		
		String sql6 = "insert into AV_AVERAGE_TRAVEL_FREQUENCY (AREAID,FREQUENCY,PLATE_TYPE,AMOUNT) values('"+areaid+"','7','川A出租车',"+result[5]+")";
		daoUtil.update(sql6, outIp, outDBName, outUser, outPassword,"1521");
	}
	
	//获取当前时间的年月日字符串倒推一天
		private static String getYMD(int i){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MONTH, 1);
			cal.add(Calendar.DAY_OF_MONTH, i);
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
			int date = cal.get(Calendar.DAY_OF_MONTH);
			if(date<=9){
				dateS = "0"+date;
			}else{
				dateS = date+"";
			}
			return yearS +"-"+monthS+"-"+ dateS;
		}
}