package com.hisense.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.dao.DaoUtil;
import com.hisense.dao.DataBaseEntity;

/**
 * @author weichengjie
 * @date 2018年11月4日
 */
public class VehicleOnRoadService {
	private DaoUtil daoUtil = new DaoUtil();
    
    /**
     * @param vehicleEntity           数据源信息-过车表
     * @return Dataset<Row>
     * 获取成都市过往10分钟内的车辆数
     * */
    public Dataset<Row> getChengduVehicleOnRoadBySql(DataBaseEntity vehicleEntity){
    	//过往数据
    	String citySql="(select count(ID) from "+vehicleEntity.getTableName()
    			+" where DCOLLECTIONDATE >= sysdate-10/1440-10/1440 and DCOLLECTIONDATE <= sysdate-10/1440) dbtable";
    	//实时数据
//    	String citySql="(select count(ID) from TD_VEHICLE_RECORD_DAY "
//    			+"where DCOLLECTIONDATE >= sysdate-10/1440 and DCOLLECTIONDATE <= sysdate) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(citySql, vehicleEntity.getDatabaseAddress(), vehicleEntity.getPort(), vehicleEntity.getAccount(),vehicleEntity.getUser(),vehicleEntity.getPassword(),vehicleEntity.getMaster());
    	return dataset;
    }
    
    /**
     * @param vehicleEntity           数据源信息-过车表
     * @return Dataset<Row>
     * 获取成都市过往10分钟内去重后的车辆数
     * */
    public Dataset<Row> getChengduDuplicatesEliminationBySql(DataBaseEntity vehicleEntity){
    	//过往数据
    	String citySql="(select count(N) from "
    			+"(select count(ID) as n from "+vehicleEntity.getTableName()
    			+" where DCOLLECTIONDATE between sysdate-10/1440-10/1440 and sysdate-10/1440 group by ORI_PLATE_NO)) dbtable";
    	//实时数据
//    	String citySql="(select count(N) from "
//    			+"(select count(ID) as n from TD_VEHICLE_RECORD_DAY "
//    			+"where DCOLLECTIONDATE between sysdate-10/1440 and sysdate group by ORI_PLATE_NO)) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(citySql, vehicleEntity.getDatabaseAddress(), vehicleEntity.getPort(), vehicleEntity.getAccount(),vehicleEntity.getUser(),vehicleEntity.getPassword(),vehicleEntity.getMaster());
    	return dataset;
    }
    
    	/**
    	 * @param vehicleEntity           数据源信息-过车表
    	 * @return Dataset<Row>
    	 * 获取成都市10分钟内识别的车牌数量
    	 */
    public Dataset<Row> getChengduLicensePlateNumberBySql(DataBaseEntity vehicleEntity){
    	//过往数据
    	String citySql="(select count(ID) from "+vehicleEntity.getTableName()
    			+" where ORI_PLATE_NO!= '--------' "
    			+"and DCOLLECTIONDATE between sysdate-10/1440-10/1440 and sysdate-10/1440) dbtable";
    	//实时数据
//    	String citySql="(select count(ID) from TD_VEHICLE_RECORD_DAY "
//    			+"where ORI_PLATE_NO!= '--------' "
//    			+"and DCOLLECTIONDATE between sysdate-10/1440 and sysdate) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(citySql, vehicleEntity.getDatabaseAddress(), vehicleEntity.getPort(), vehicleEntity.getAccount(),vehicleEntity.getUser(),vehicleEntity.getPassword(),vehicleEntity.getMaster());
    	return dataset;
    }
    
    /**
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
     * @return Dataset<Row>
     * 获取各区域过往10分钟内的车辆数
     * */
    public Dataset<Row> getRegionVehicleOnRoadBySql(String str,DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
    	//过往数据
    	String regionSql1="(select count(ID) from "+vehicleEntity.getTableName()+" v,"+deviceEntity.getTableName()+" d "
    		        +"where (v.deviceid=d.deviceid " 
    	            +"and d.REGION=";
    	String regionSql2=" and v.DCOLLECTIONDATE between sysdate-10/1440-10/1440 and sysdate-10/1440)) dbtable";
    	//实时数据
//    	String regionSql1="(select count(ID) from TD_VEHICLE_RECORD_DAY v,TS_DEVICE d "
//		        +"where (v.deviceid=d.deviceid " 
//	            +"and d.REGION=";
//    	String regionSql2=" and v.DCOLLECTIONDATE between sysdate-10/1440 and sysdate)) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(regionSql1+str+regionSql2,vehicleEntity.getDatabaseAddress(), vehicleEntity.getPort(), vehicleEntity.getAccount(),vehicleEntity.getUser(),vehicleEntity.getPassword(),vehicleEntity.getMaster());
    	return dataset;
    }
    
    
    /**
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
     * @return Dataset<Row>
     * 获取区域过往10分钟内去重后的车辆数
     * */
    public Dataset<Row> getRegionDuplicatesEliminationBySql(String str,DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
    	//过往数据
    	String regionSql1="(select count(N) from "            
				+"(select count(ID) as n "
				+"from "+vehicleEntity.getTableName()+" v,"+deviceEntity.getTableName()+" d "
				+"where (v.deviceid=d.deviceid "
				+"and d.REGION=";
    	String regionSql2=" and (v.DCOLLECTIONDATE between sysdate-10/1440-10/1440 and sysdate-10/1440)) "
				+"group by v.ORI_PLATE_NO)) dbtable";
    	//实时数据
//    	String regionSql1="(select count(N) from "            
//				+"(select count(ID) as n "
//				+"from TD_VEHICLE_RECORD_DAY v,TS_DEVICE d "
//				+"where (v.deviceid=d.deviceid "
//				+"and d.REGION=";
//    	String regionSql2=" and (v.DCOLLECTIONDATE between sysdate-10/1440 and sysdate)) "
//				+"group by v.ORI_PLATE_NO)) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(regionSql1+str+regionSql2,vehicleEntity.getDatabaseAddress(), vehicleEntity.getPort(), vehicleEntity.getAccount(),vehicleEntity.getUser(),vehicleEntity.getPassword(),vehicleEntity.getMaster());
    	return dataset;
    }
    
	/**
     * @param vehicleEntity           数据源信息-过车表
     * @param deviceEntity            数据源信息-设备表
	 * @return Dataset<Row>
	 * 获取成都市10分钟内识别的车牌数量
	 */
    public Dataset<Row> getRegionLicensePlateNumberBySql(String str,DataBaseEntity vehicleEntity,DataBaseEntity deviceEntity){
    	//过往数据
    	String regionSql1="(select count(ID) from "+vehicleEntity.getTableName()+" v,"+deviceEntity.getTableName()+" d "
    			+"where (v.deviceid=d.deviceid "
    			+"and d.REGION=";
    	String regionSql2=" and v.ORI_PLATE_NO!= '--------' "
    			+"and v.DCOLLECTIONDATE between sysdate-10/1440-10/1440 and sysdate-10/1440)) dbtable";
    	//实时数据
//    	String regionSql1="(select count(ID) from TD_VEHICLE_RECORD_DAY v,TS_DEVICE d "
//    			+"where (v.deviceid=d.deviceid "
//    			+"and d.REGION=";
//    	String regionSql2=" and v.ORI_PLATE_NO!= '--------' "
//    			+"and v.DCOLLECTIONDATE between sysdate-10/1440 and sysdate)) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(regionSql1+str+regionSql2,vehicleEntity.getDatabaseAddress(), vehicleEntity.getPort(), vehicleEntity.getAccount(),vehicleEntity.getUser(),vehicleEntity.getPassword(),vehicleEntity.getMaster());
    	return dataset;
    }
    
    /**
     * 
     * @param cityNum			成都市在途车辆数
     * @param regionNum			各区域在途车辆数
     */
    public void writeVehicleOnRoad(long cityNum,long[] regionNum,DataBaseEntity vehicleEntity){
    	String sql1="insert into "+vehicleEntity.getOutPutTablename()+" (REGION_ID,REGION_NUM) values ";
    	String sql2="("+"510100"+","+cityNum+")";
    	int f=510103;
    	daoUtil.update(sql1+sql2,vehicleEntity.getOutPutDatabaseAddress(),vehicleEntity.getOutPutAccount(),vehicleEntity.getOutPutUser(),vehicleEntity.getOutPutPassword(),vehicleEntity.getOutPutPort());
    	for(int i=0;i<6;i++){
    		f=f+1;
    		sql2="("+f+","+regionNum[i]+")";
    		daoUtil.update(sql1+sql2,vehicleEntity.getOutPutDatabaseAddress(),vehicleEntity.getOutPutAccount(),vehicleEntity.getOutPutUser(),vehicleEntity.getOutPutPassword(),vehicleEntity.getOutPutPort());
    	}
    }

}
