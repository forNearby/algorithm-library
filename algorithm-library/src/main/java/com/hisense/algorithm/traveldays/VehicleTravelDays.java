/**
 * 
 */
package com.hisense.algorithm.traveldays;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleRecordService;

/**
 * @author machao
 * @date 2018年10月31日
 * 车辆出行天数分析
 */
public class VehicleTravelDays {
	 private static Logger logger = Logger.getLogger(VehicleTravelDays.class);

	/**
	 * 基于过车数据表计算（一个月）路网内出现的车辆数，每辆车仅被统计一次（去重）month--"yyyy-mm"
	 */
	public long countAllVehicleRecordByMonth(DataBaseEntity dataBaseEntity,String month){
		logger.info("------------------ 基于过车数据表计算(一个月)路网内出现的车辆数-------------------");
		VehicleRecordService vehicleRecordService = new VehicleRecordService(dataBaseEntity);
		Dataset<Row> dataset = vehicleRecordService.countAllVehicleRecordByMonth(month);
		long number= (long) dataset.head().get(0);
		return number;
	}
	/**
	 * 基于过车数据表计算(过去几天的的)路网内出现的车辆数，每辆车仅被统计一次（去重）
	 */
	public long countAllVehicleRecordByDay(DataBaseEntity dataBaseEntity,int day){
		logger.info("---------------- 基于过车数据表计算(过去几天的的)路网内出现的车辆数----------------");
		VehicleRecordService vehicleRecordService = new VehicleRecordService(dataBaseEntity);
		Dataset<Row> dataset = vehicleRecordService.countAllVehicleRecordByDay(day);
		long number= (long) dataset.head().get(0);
		return number;
	}
	/**
	 * 计算(过去几天的的)每辆车的出行天数，车辆每天被检测到1次即认为车辆在该天内出行）
	 */
	public void getEveryVehicleRecordByDay(DataBaseEntity dataBaseEntity,int day){
		logger.info("------------------计算(过去几天的的)每辆车的出行天数------------------------------");
		VehicleRecordService vehicleRecordService = new VehicleRecordService(dataBaseEntity);
		Dataset<Row> dataset = vehicleRecordService.countEveryVehicleRecordByDay(day);
		dataset.show();
	}
	/**
	 * 计算(任意一个月)每辆车的出行天数，车辆每天被检测到1次即认为车辆在该天内出行）month--"yyyy-mm"
	 */
	public void getEveryVehicleRecordByMonth(DataBaseEntity dataBaseEntity,String month){
		logger.info("------------------计算(任意一个月)每辆车的出行天数------------------------------");
		VehicleRecordService vehicleRecordService = new VehicleRecordService(dataBaseEntity);
		Dataset<Row> dataset = vehicleRecordService.countEveryVehicleRecordByMonth(month);
		dataset.show();
	}
	/**
	 * 统计不同出行天数下的车辆数量及占比
	 * （昨天的出行天数占过去七天的车辆数量及占比）
	 */
	public VehicleDayRatio getAllVehicleRecordRatio(DataBaseEntity dataBaseEntity,int day,int allDay){
		
		logger.info("------------------统计不同出行天数下的车辆数量及占比------------------------------");
		Date date=new Date();//取时间
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		calendar.add(Calendar.DATE,-1);//把日期往前推一天
		date=calendar.getTime(); //这个时间就是日期往后推一天的结果 
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String vehicleDay = formatter.format(date);
		VehicleRecordService vehicleRecordService = new VehicleRecordService(dataBaseEntity);
		Dataset<Row> dataset1 = vehicleRecordService.countAllVehicleRecordByDay(day);
		Dataset<Row> dataset2 = vehicleRecordService.countAllVehicleRecordByDay(allDay);
		long A= (long) dataset1.head().get(0);
		long B= (long) dataset2.head().get(0);
		double ratio=(double)A/B;
        DecimalFormat decimalFormat = new DecimalFormat("0.0000");//格式化设置          
		VehicleDayRatio vehicleDayRatio=new VehicleDayRatio();
		vehicleDayRatio.setVehicleDay(vehicleDay);
		vehicleDayRatio.setVehicleDayRatio(decimalFormat.format(ratio));
		vehicleDayRatio.setVehicleNumber(A);
		logger.info("-------------昨天("+vehicleDay+")值:"+A+"--------------前七天的值:"+B+"--------------ratio的值:"+decimalFormat.format(ratio));
		return vehicleDayRatio;
	}
	public static void main(String[] args) throws IOException {
		//String jSONString=new String(args[0]);
		String jSONString=new String(java.util.Base64.getDecoder().decode(args[0]));
		DataBaseEntity dataBaseEntity = new DataBaseEntity();
		dataBaseEntity.getInfo(jSONString,"TD_VEHICLE_RECORD_DAY","AV_VEHICLE_DAY_RATIO_RESULTS");
		//System.out.println(dataBaseEntity.toString());
		VehicleTravelDays vehicleRecordAlgorithm = new VehicleTravelDays();
		VehicleDayRatio vehicleDayRatio=vehicleRecordAlgorithm.getAllVehicleRecordRatio(dataBaseEntity,1,7);
        VehicleRecordService vehicleRecordService=new VehicleRecordService(dataBaseEntity);
        vehicleRecordService.updateVehicleDayRatio(vehicleDayRatio);
	}
}
