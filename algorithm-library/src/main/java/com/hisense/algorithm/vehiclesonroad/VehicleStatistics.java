package com.hisense.algorithm.vehiclesonroad;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleOnRoadService;

/**
 * @author weichengjie
 * @date 2018年11月1日
 */
class VehicleStatistics {
	private static Logger logger = Logger.getLogger(VehicleStatistics.class);
	private VehicleOnRoadService vehicleRecordService = new VehicleOnRoadService();
	
	/**
     * @param vehicleEntity           数据源信息-过车表
	 * @return 
	 * 获取整个城市内10分钟内的车辆数
	 * */
	protected double getChengduCountVehicle10min(DataBaseEntity vehicleEntity){
		Dataset<Row> dataset = vehicleRecordService.getChengduVehicleOnRoadBySql(vehicleEntity);
		double vehicleCount=Double.parseDouble(dataset.head().get(0).toString());
		String vehicleCountLog="成都市道路上车辆总数为："+vehicleCount;
		logger.info(vehicleCountLog);
		return vehicleCount;
	}

	/**
     * @param vehicleEntity           数据源信息-过车表
	 * @return 
	 * 获取整个城市内10分钟内去重后的车辆数
	 * */
	protected double getChengduDuplicatesElimination(DataBaseEntity vehicleEntity){
		Dataset<Row> dataset = vehicleRecordService.getChengduDuplicatesEliminationBySql(vehicleEntity);
		double vehicleCount=Double.parseDouble(dataset.head().get(0).toString());
		String vehicleCountLog="成都市道路上车辆去重后的总数为："+vehicleCount;
		logger.info(vehicleCountLog);
		return vehicleCount;
	}
	
	/**
     * @param vehicleEntity           数据源信息-过车表
	 * @return
	 * 获取整个城市内10分钟内识别的车牌数量
	 * */
	protected double getChengduLicensePlateNumber(DataBaseEntity vehicleEntity){
		Dataset<Row> dataset = vehicleRecordService.getChengduLicensePlateNumberBySql(vehicleEntity);
		double vehicleCount=Double.parseDouble(dataset.head().get(0).toString());
		String vehicleCountLog="成都市道路上识别的车辆总数为："+vehicleCount;
		logger.info(vehicleCountLog);
		return vehicleCount;
	}
	
	/**
	 * @param str		                                      区域编号
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
	 * @return 
	 * 获取区域内10分钟内的车辆数
	 * */
	protected double getRegionCountVehicle10min(String str,DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
		Dataset<Row> dataset = vehicleRecordService.getRegionVehicleOnRoadBySql(str,vehicleEntity,deviceEntity);
		double vehicleCount=Double.parseDouble(dataset.head().get(0).toString());
		String vehicleCountLog="编号为："+str+"的区域道路上车辆总数为："+vehicleCount;
		logger.info(vehicleCountLog);
		return vehicleCount;
	}
	
	/**
	 * @param str		                                      区域编号
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
	 * @return 
	 * 获取区域内10分钟内去重后的车辆数
	 * */
	protected double getRegionDuplicatesElimination(String str,DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
		Dataset<Row> dataset = vehicleRecordService.getRegionDuplicatesEliminationBySql(str,vehicleEntity,deviceEntity);
		double vehicleCount=Double.parseDouble(dataset.head().get(0).toString());
		String vehicleCountLog="编号为："+str+"的区域内道路上车辆去重后的总数为："+vehicleCount;
		logger.info(vehicleCountLog);
		return vehicleCount;
	}
	
	/**
	 * @param str		                                      区域编号
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
	 * @return
	 * 获取区域内10分钟内识别的车牌数量
	 * */
	protected double getRegionLicensePlateNumber(String str,DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
		Dataset<Row> dataset = vehicleRecordService.getRegionLicensePlateNumberBySql(str,vehicleEntity,deviceEntity);
		double vehicleCount=Double.parseDouble(dataset.head().get(0).toString());
		String vehicleCountLog="编号为："+str+"的区域内道路上识别的车牌总数为："+vehicleCount;
		logger.info(vehicleCountLog);
		return vehicleCount;
	}
}
