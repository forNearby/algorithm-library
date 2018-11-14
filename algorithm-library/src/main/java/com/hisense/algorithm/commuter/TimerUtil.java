/**
 * 
 */
package com.hisense.algorithm.commuter;


import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleRecordService;

/**
 * @author machao
 * @date 2018年11月3日
 * 每月一号计算上个月的通勤车数量
 */
public class TimerUtil {
	private static Logger logger = Logger.getLogger(TimerUtil.class);
	public static void main(String[] args) throws IOException {
		Date date = new Date();
		Timer mTimer = new Timer();
		mTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				Calendar c = Calendar.getInstance();
				int day = c.get(Calendar.DAY_OF_MONTH);
				logger.info("**************************************通勤车识别月任务判断中**************************************");
				if (day == 1) {
					// 每月1号才执行
					logger.info("**************************************月任务开始执行***********************************************");
					String jSONString = args[0];
					logger.info("jSONString"+jSONString);
					DataBaseEntity dataBaseEntity = new DataBaseEntity();
					dataBaseEntity.getInfo(jSONString,"TD_VEHICLE_RECORD_DAY","TD_Vehicle_Results");
					VehicleRecordService vehicleRecordService=new VehicleRecordService(dataBaseEntity);
					logger.info("**************************************开始创建中间表***********************************************");
					vehicleRecordService.updateVehicheRecords();
					logger.info("*******************************中间表创建完成，即将执行聚类程序***********************************************");
					logger.info("**************************************开始执行聚类程序***********************************************");
					JudgeVehicle judgeVehicle=new JudgeVehicle();
					judgeVehicle.judge(dataBaseEntity).getResult(dataBaseEntity).saveResult(dataBaseEntity);
					logger.info("**************************************月任务执行成功***********************************************");
				}

			}
		}, date, 24 * 60 * 60 * 1000);// 24 * 60 * 60 * 1000每天执行一次检查
	}
}
