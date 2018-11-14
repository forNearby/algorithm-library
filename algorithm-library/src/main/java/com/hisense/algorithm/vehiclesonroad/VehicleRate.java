package com.hisense.algorithm.vehiclesonroad;

import org.apache.log4j.Logger;

import com.hisense.dao.DataBaseEntity;

/**
 * 
 * @author weichengjie
 * @date 2018年11月1日
 */
class VehicleRate {
	private static Logger logger = Logger.getLogger(VehicleRate.class);
	
	/**
     * @param vehicleEntity           数据源信息-过车表
	 * @return获取成都市车辆识别率
	 */
	protected double getChengduVehicleRate(DataBaseEntity vehicleEntity){
		VehicleStatistics vehicleStatistics=new VehicleStatistics();
		double countVehicle10min=vehicleStatistics.getChengduCountVehicle10min(vehicleEntity);//抓拍车辆总数
		double licenseNumber=vehicleStatistics.getChengduLicensePlateNumber(vehicleEntity);//识别车牌的数量
		double rate=0;
		try{
			rate=licenseNumber/countVehicle10min;
		}catch(ArithmeticException e){
			e.getMessage();
		}
		String rateCountLog="成都市道路上车辆识别率为："+rate;
		logger.info(rateCountLog);
		return rate;
	}
	
	/**
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
	 * @return获取各区域市车辆识别率
	 */
	protected double getRegionVehicleRate(String str, DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
		VehicleStatistics vehicleStatistics=new VehicleStatistics();
		double countVehicle10min=vehicleStatistics.getRegionCountVehicle10min(str,vehicleEntity,deviceEntity);//抓拍车辆总数
		double licenseNumber=vehicleStatistics.getRegionLicensePlateNumber(str,vehicleEntity,deviceEntity);//识别车牌的数量
		double rate=0;
		try{
			rate=licenseNumber/countVehicle10min;
		}catch(ArithmeticException e){
			e.getMessage();
		}
		String rateCountLog="编号为："+str+"的区域内车辆识别率为："+rate;
		logger.info(rateCountLog);
		return rate;
	}
}
