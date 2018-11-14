package com.hisense.algorithm.vehiclesonroad;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.hisense.dao.DataBaseEntity;
import com.hisense.service.VehicleOnRoadService;

/**
 * @author weichengjie
 * @date 2018年11月1日
 */
public class VehiclesOnRoad {
	private static Logger logger = Logger.getLogger(VehiclesOnRoad.class);
	VehicleStatistics vehicleStatistics=null;
	static String[] regionNo={"510104","510105","510106","510107","510108","510109"};
	static String[] region={"锦江区","青羊区","金牛区","武侯区","成华区","高新区"};
	/**
	 * 
	 * @param args
	 * @throws IOException
	 * main方法
	 */
	public static void main(String[] args) throws IOException {
        //String jSONString = IoUtil.readFile("src/com/hisense/util/data.json", "UTF-8");
		String jSONString=new String(java.util.Base64.getDecoder().decode(args[0]));
		//String jSONString =args[0];
		//String jSONString="{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"database\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"port\":\"1521\",\"account\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"},\"TS_DEVICE\":{\"database\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"port\":\"1521\",\"account\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TS_DEVICE\"}},\"output\":{\"TD_VEHICLE_ONROAD\":{\"database\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"port\":\"1521\",\"account\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"AV_VEHICLE_ONROAD\"}},\"spark_info\":{\"master\":\"local\"}}";
		DataBaseEntity vehicleEntity=new DataBaseEntity();
        DataBaseEntity deviceEntity=new DataBaseEntity();
        vehicleEntity.getInfo(jSONString,"TD_VEHICLE_RECORD_DAY","TD_VEHICLE_ONROAD");
        deviceEntity.getInfo(jSONString, "TS_DEVICE","TD_VEHICLE_ONROAD");
        
		long startTime = System.currentTimeMillis(); 
		VehiclesOnRoad v=new VehiclesOnRoad();
		long cityNum=v.getChengduVehiclesOnRoad(vehicleEntity);
		System.out.println("成都市当前时刻在途车辆数="+cityNum);
		long[] regionNum=v.getRegionVehiclesOnRoad(vehicleEntity,deviceEntity);

	    VehicleOnRoadService v1=new VehicleOnRoadService();
		v1.writeVehicleOnRoad(cityNum, regionNum,vehicleEntity);	
		long endTime = System.currentTimeMillis();    //获取结束时间
		System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
	}	
	
    /**
     * @param vehicleEntity           数据源信息-过车表
     * @return 成都市在途车辆数
     */
	public long getChengduVehiclesOnRoad(DataBaseEntity vehicleEntity){
		vehicleStatistics=new VehicleStatistics();
		double countDuplicatesElimination=vehicleStatistics.getChengduDuplicatesElimination(vehicleEntity);
		VehicleRate vehicleRate=new VehicleRate();
		double rate=vehicleRate.getChengduVehicleRate(vehicleEntity);
		double vehiclesOnRoadCount = 0;
		try{
			vehiclesOnRoadCount=countDuplicatesElimination/rate;
		}catch(ArithmeticException e){
			e.getMessage();
		}
		String logInfo3="成都市当前时刻在途车辆数="+vehiclesOnRoadCount;
		logger.info(logInfo3);
		return new Double(vehiclesOnRoadCount).longValue();
	}
	
    /**				
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
     * @return 锦江区在途车辆数
     */
	public long[] getRegionVehiclesOnRoad(DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
		long[] vehiclesOnRoad=new long[6];
		vehicleStatistics=new VehicleStatistics();
		for(int i=0;i<regionNo.length;i++){
			double countDuplicatesElimination=vehicleStatistics.getRegionDuplicatesElimination(regionNo[i],vehicleEntity,deviceEntity);
			VehicleRate vehicleRate=new VehicleRate();
			double rate=vehicleRate.getRegionVehicleRate(regionNo[i],vehicleEntity,deviceEntity);
			//System.out.println(rate);
			long vehiclesOnRoadCount = 0;
			try{
				vehiclesOnRoadCount=(long) (countDuplicatesElimination/rate);
			}catch(ArithmeticException e){
				e.getMessage();
			}
			String logInfo3=region[i]+"当前时刻在途车辆数="+vehiclesOnRoadCount;
			logger.info(logInfo3);
			System.out.println(logInfo3);
			vehiclesOnRoad[i]=new Double(vehiclesOnRoadCount).longValue();
		}
		return vehiclesOnRoad;
	}
}
