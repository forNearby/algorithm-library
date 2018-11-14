package com.hisense.algorithm.commuter.maxmin;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleRecordService;

/**
 * @author machao
 * @date 2018年11月1日 处理spark传入的数据
 */
public class DataAdapter {
	private static Logger logger = Logger.getLogger(DataAdapter.class);

	public ArrayList<Point> getAllData(DataBaseEntity dataBaseEntity) {
		logger.info("--------------正在---------------适配通勤车聚类数据格式-------------------------------------");
		VehicleRecordService vehicleRecordService = new VehicleRecordService(dataBaseEntity);
		Dataset<Row> datasetScores = vehicleRecordService.getVehicleRecordScores();
		datasetScores.show();
		List<Row> vehicleRecordList = datasetScores.collectAsList();
		logger.info("-----------------------------------collectAsList成功------------------------------------");
		ArrayList<Point> points = new ArrayList<Point>();
		String reg = "[\\u4e00-\\u9fa5]+" ;//去掉武警等公用车
		for (Row s : vehicleRecordList) {
			if((s.getString(0)).substring(0,1).matches(reg))
			{
				Point point = new Point();
				point.setPeakFrequency(((BigDecimal) s.get(1)).doubleValue());
				point.setUnpeakFrequency(((BigDecimal) s.get(2)).doubleValue());
				point.setOneRoadTime(((BigDecimal) s.get(3)).doubleValue());
				point.setEntityID(s.getString(0));
				points.add(point);	
			}
			
		}
		logger.info("----------完成---"+points.size()+"条通勤车数据的适配--------------------------------------");
		return points;
	}
	public static void main(String[] args) {
		String a="wj鲁78788";
		String reg = "[\\u4e00-\\u9fa5]+" ;
			if(a.substring(0,1).matches(reg))
			{
				System.out.println("----------");
			}

	}
}
