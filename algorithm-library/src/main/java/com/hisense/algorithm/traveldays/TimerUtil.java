/**
 * 
 */
package com.hisense.algorithm.traveldays;


import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleRecordService;

/**
 * @author machao
 * @date 2018年11月3日
 * 每天判断一次一天占过去七天的车辆占比
 */
public class TimerUtil {
	private static Logger logger = Logger.getLogger(TimerUtil.class);
	public static void main(String[] args) throws IOException {
		Date date = new Date();
		Timer mTimer = new Timer();
		mTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				logger.info("**************************************车辆占比计算中**************************************");
				String jSONString =args[0];
				DataBaseEntity dataBaseEntity = new DataBaseEntity();
				dataBaseEntity.getInfo(jSONString,"TD_VEHICLE_RECORD_DAY","TD_VehicleDayRatio_Results");
				VehicleTravelDays vehicleRecordAlgorithm = new VehicleTravelDays();
				VehicleDayRatio vehicleDayRatio=vehicleRecordAlgorithm.getAllVehicleRecordRatio(dataBaseEntity,1,7);
		        VehicleRecordService vehicleRecordService=new VehicleRecordService(dataBaseEntity);
		        vehicleRecordService.updateVehicleDayRatio(vehicleDayRatio);
				logger.info("**************************************"+vehicleDayRatio.getVehicleDay()+"车辆占计算完成**************************************");

			}
		}, date, 24 * 60 * 60 * 1000);// 24 * 60 * 60 * 1000每天执行一次检查
	}
}
