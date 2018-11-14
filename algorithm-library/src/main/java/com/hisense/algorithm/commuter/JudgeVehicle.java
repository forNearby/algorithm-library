/**
 *
 */
package com.hisense.algorithm.commuter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.hisense.algorithm.commuter.maxmin.DataAdapter;
import com.hisense.algorithm.commuter.maxmin.MaxMinDistance;
import com.hisense.algorithm.commuter.maxmin.Point;
import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleRecordService;

/**
 * @author machao
 * @date 2018年10月31日 通勤车辆判别算法入口
 */
public class JudgeVehicle {
	 private static Logger logger = Logger.getLogger(JudgeVehicle.class);

	public  List<Point> commuterCars=new  ArrayList<Point>();
	public   JudgeVehicle judge(DataBaseEntity dataBaseEntity) {
		commuterCars.clear();
		ArrayList<Point> datas = new ArrayList<Point>();
		DataAdapter dataAdapter = new DataAdapter();
		datas = dataAdapter.getAllData(dataBaseEntity);
		MaxMinDistance engine = new MaxMinDistance(datas);
		Long begin = System.currentTimeMillis();
		commuterCars= engine.makeCenter().makeFurthestCenter().makeOtherCenter().poly().sort().sift();
		logger.info("用时 : " + (System.currentTimeMillis() - begin) + "ms");
		return this;
		
	}
	public  JudgeVehicle getResult(DataBaseEntity dataBaseEntity) {
		logger.info("得到所有通勤车：");
		VehicleRecordService vehicleRecordService=new VehicleRecordService(dataBaseEntity);
		vehicleRecordService.getVehicleRecordResults().show();
		//commuterCars.forEach((e) -> logger.info(e));
		logger.info("所有通勤车数量 ：" + commuterCars.size());
		return this;
		
	}
	public  JudgeVehicle saveResult(DataBaseEntity dataBaseEntity) {
		VehicleRecordService vehicleRecordService=new VehicleRecordService(dataBaseEntity);
		vehicleRecordService.updateVehicheResult(commuterCars);
		return this;
		
	}
	public static void main(String[] args) throws IOException {
		String jSONString=new String(java.util.Base64.getDecoder().decode(args[0]));
		//String jSONString=new String(args[0]);
		logger.info("jSONString"+jSONString);
		DataBaseEntity dataBaseEntity = new DataBaseEntity();
		dataBaseEntity.getInfo(jSONString,"TD_VEHICLE_RECORD_DAY","AV_VEHICLE_RESULTS");
		VehicleRecordService vehicleRecordService=new VehicleRecordService(dataBaseEntity);
		logger.info("***************************************开始创建中间表***********************************************");
		vehicleRecordService.updateVehicheRecords();
		logger.info("*******************************中间表创建完成，即将执行聚类程序***************************************");
		logger.info("**************************************开始执行聚类程序***********************************************");
		JudgeVehicle judgeVehicle=new JudgeVehicle();
		judgeVehicle.judge(dataBaseEntity).getResult(dataBaseEntity).saveResult(dataBaseEntity);
	}
}
