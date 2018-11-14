package com.hisense.service;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.algorithm.commuter.maxmin.Point;
import com.hisense.algorithm.traveldays.VehicleDayRatio;
import com.hisense.dao.DataBaseEntity;
import com.hisense.dao.VehicleDaoUtil;

/**
 * @author machao
 * @date 2018年10月30日 通勤车识别服务类，用于各种数据库操作
 */
public class VehicleRecordService {
	private VehicleDaoUtil daoUtil = new VehicleDaoUtil();

	public VehicleRecordService(DataBaseEntity dataBaseEntity) {
		daoUtil.setDataBase(dataBaseEntity);
	}

	/**
	 * 基于过车数据表计算(过去几天的的)路网内出现的车辆数，每辆车仅被统计一次（去重）
	 */
	public Dataset<Row> countAllVehicleRecordByDay(int dayNum) {
		Dataset<Row> dataset = daoUtil.operateByDataBaseInfo(
				"(select distinct ORI_PLATE_NO"
				+ " from TD_VEHICLE_RECORD_DAY"
				+ " where( DCOLLECTIONDATE >= trunc(sysdate - "+dayNum+")"
				+" AND DCOLLECTIONDATE < trunc(sysdate))"
				+ ") dbtable", "ALL_RECORD_NO",
				"select count(*) from ALL_RECORD_NO");
		return dataset;
	}
	/**
	 * 基于过车数据表计算(任意一个月)内路网内出现的车辆数，每辆车仅被统计一次（去重）
	 */
	public Dataset<Row> countAllVehicleRecordByMonth(String month) {
		Dataset<Row> dataset = daoUtil.operateByDataBaseInfo(
				"(select distinct ORI_PLATE_NO"
				+ " from TD_VEHICLE_RECORD_DAY"
				+ " where to_char(DCOLLECTIONDATE, 'yyyy-MM') = '"+month+"') dbtable", "ALL_RECORD_NO",
				"select count(*) from ALL_RECORD_NO");
		return dataset;
	}

	/**
	 * 计算(过去几天的的)每辆车的出行天数，车辆每天被检测到1次即认为车辆在该天内出行）
	 */
	public Dataset<Row> countEveryVehicleRecordByDay(int dayNum) {
		Dataset<Row> dataset = daoUtil.operateByDataBaseInfo("(select ORI_PLATE_NO,count(ORI_PLATE_NO) "
				+ " from (select ORI_PLATE_NO,to_char(DCOLLECTIONDATE, 'yyyy-MM-dd') as day  "
				+ " from TD_VEHICLE_RECORD_DAY where DCOLLECTIONDATE >= trunc(sysdate - " + dayNum + ") "
				+ " group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE, 'yyyy-MM-dd'))" + " group by ORI_PLATE_NO) dbtable",
				"EVERY_NUMBER", "select *  from EVERY_NUMBER ");
		return dataset;
	}
	/**
	 * 计算某天的出行车辆数，车辆每天被检测到1次即认为车辆在该天内出行
	 */
	public Dataset<Row> countEveryVehicleRecordByBeforeDay(String day) {
		Dataset<Row> dataset = daoUtil.operateByDataBaseInfo("(select ORI_PLATE_NO,count(ORI_PLATE_NO) "
				+ " from (select ORI_PLATE_NO,to_char(DCOLLECTIONDATE, 'yyyy-MM-dd') as day  "
				+ " from TD_VEHICLE_RECORD_DAY where to_char(DCOLLECTIONDATE, 'yyyy-MM-dd') = '"+day+"' "
				+ " group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE, 'yyyy-MM-dd'))" + " group by ORI_PLATE_NO) dbtable",
				"EVERY_NUMBER", "select *  from EVERY_NUMBER ");
		return dataset;
	}
	/**
	 * 计算(任意一个月)每辆车的出行天数，车辆每天被检测到1次即认为车辆在该天内出行）
	 */
	public Dataset<Row> countEveryVehicleRecordByMonth(String month) {
		Dataset<Row> dataset = daoUtil.operateByDataBaseInfo("(select ORI_PLATE_NO,count(ORI_PLATE_NO) "
				+ " from (select ORI_PLATE_NO,to_char(DCOLLECTIONDATE, 'yyyy-MM-dd') as day  "
				+ " from TD_VEHICLE_RECORD_DAY where to_char(DCOLLECTIONDATE, 'yyyy-MM') = '"+month+"') "
				+ " group by (ORI_PLATE_NO, to_char(DCOLLECTIONDATE, 'yyyy-MM-dd')))" + " group by ORI_PLATE_NO) dbtable",
				"EVERY_NUMBER", "select *  from EVERY_NUMBER ");
		return dataset;
	}

	/**
	 * 获取工作日早晚高峰过车信息test
	 *
	 * @return 早晚高峰过车信息
	 */
	public Dataset<Row> getVehicleRecordByWorkday() {
		Dataset<Row> dataset = daoUtil.operateByDataBaseInfo(
				"(SELECT TDA.ORI_PLATE_NO,TDA.A,TDB.B ,trunc(dbms_random.value(1,3)) AS C"
						+ " FROM(SELECT ORI_PLATE_NO,count(id) AS A FROM Td_Vehicle_Record_Day_Test" + " WHERE"
						+ " ORI_PLATE_NO !='--------'"
						+ " AND (to_char(DCOLLECTIONDATE, 'd') !=1 AND to_char(DCOLLECTIONDATE, 'd') !=7)"
						+ " AND (to_char(DCOLLECTIONDATE, 'hh24') > 16 AND to_char(DCOLLECTIONDATE, 'hh24') <20)"
						+ " OR (to_char(DCOLLECTIONDATE, 'hh24') > 6  AND to_char(DCOLLECTIONDATE, 'hh24') <9)"
						+ " group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE,'yyyy-MM-dd')) TDA"
						+ " JOIN(SELECT ORI_PLATE_NO,count(id) AS B FROM Td_Vehicle_Record_Day_Test" + " WHERE"
						+ " ORI_PLATE_NO !='--------'"
						+ " AND (to_char(DCOLLECTIONDATE, 'd') !=1 AND to_char(DCOLLECTIONDATE, 'd') !=7)"
						+ " AND (to_char(DCOLLECTIONDATE, 'hh24') > 9 AND to_char(DCOLLECTIONDATE, 'hh24') <16)"
						+ " group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE,'yyyy-MM-dd')) TDB"
						+ " ON TDA.ORI_PLATE_NO = TDB.ORI_PLATE_NO) dbtable", "TD_VEHICLE_RECORD_DAY",
				"select * from TD_VEHICLE_RECORD_DAY");
		return dataset;
	}
	/**
	 * 获取所有TM_VEHICLE_SCORES中间表的数据
	 *
	 */
	public Dataset<Row> getVehicleRecordScores() {
		Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo("(select * from TM_VEHICLE_SCORES)  dbtable");
		return dataset;
	}
	/**
	 * 获取所有AV_VEHICLE_RESULTS表的数据
	 *
	 */
	public Dataset<Row> getVehicleRecordResults() {
		Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo("(select * from AV_VEHICLE_RESULTS)  dbtable");
		return dataset;
	}
	/**
	 * 获取所有AV_VEHICLE_DAY_RATIO_RESULTS表的数据
	 *
	 */
	public Dataset<Row> getVehicleDayRatioResults() {
		Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo("(select * from AV_VEHICLE_DAY_RATIO_RESULTS)  dbtable");
		return dataset;
	}
	/**
	 * 获取（上个周）的工作日过车信息，并存储到TM_VEHICLE_SCORES中间表
	 *
	 * @return 存储是否成功结果信息
	 */
	public int updateVehicheRecords() {
		daoUtil.update("declare                                                                                     "
				+ "     num   number;                                                                                "
				+ "     begin                                                                                        "
				+ "         select count(1) into num from user_tables where table_name = upper('TM_VEHICLE_SCORES') ;"
				+ "         if num > 0 then                                                                          "
				+ "             execute immediate 'delete from TM_VEHICLE_SCORES' ;                                   "
				+ "         end if;                                                                                  "
				+ "     end;                                                                                         ");
		int result = daoUtil
				.update("insert  into TM_VEHICLE_SCORES(Ori_Plate_No,A,B,C)"
						+ " (    SELECT TDA.ORI_PLATE_NO,NVL(TDA.A,0) A, NVL(TDB.B,0) B,ROUND(NVL(TDA.A,0)/(NVL(TDA.A,0)+ NVL(TDB.B,0)),4) C FROM "
						+ " (select ORI_PLATE_NO,count(ORI_PLATE_NO) AS A FROM "
						+ " ( SELECT ORI_PLATE_NO  FROM ( SELECT * FROM Td_Vehicle_Record_Day "
						+ " WHERE  (DCOLLECTIONDATE between trunc(sysdate,'iw') - 7 and trunc(sysdate,'iw') - 3) AND ORI_PLATE_NO !='--------')"
						+ " WHERE   to_char(DCOLLECTIONDATE, 'd') !=1 AND to_char(DCOLLECTIONDATE, 'd') !=7"
						+ "  AND (to_char(DCOLLECTIONDATE, 'hh24') > 16 AND to_char(DCOLLECTIONDATE, 'hh24') < 20) "
						+ " OR (to_char(DCOLLECTIONDATE, 'hh24') > 6  AND to_char(DCOLLECTIONDATE, 'hh24')< 9) "
						+ " group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE,'yyyy-MM-dd')) group by ORI_PLATE_NO   "
						+ " ) TDA   left JOIN  "
						+ "  (select ORI_PLATE_NO,count(ORI_PLATE_NO) AS B FROM "
						+ "  (SELECT ORI_PLATE_NO FROM ( SELECT * FROM Td_Vehicle_Record_Day "
						+ "   WHERE  (DCOLLECTIONDATE between trunc(sysdate,'iw') - 7 and trunc(sysdate,'iw') - 3) AND ORI_PLATE_NO !='--------' )"
						+ "   WHERE  to_char(DCOLLECTIONDATE, 'd') !=1 AND to_char(DCOLLECTIONDATE, 'd') !=7"
						+ "   AND (to_char(DCOLLECTIONDATE, 'hh24') > 9 AND to_char(DCOLLECTIONDATE, 'hh24') < 16)"
						+ "  group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE,'yyyy-MM-dd')) group by ORI_PLATE_NO "
						+ "  ) TDB  ON TDA.ORI_PLATE_NO = TDB.ORI_PLATE_NO)");// where asciistr(substr (ORI_PLATE_NO,0,1)) not like '%\\%'
		return result;
	}

	/**
	 * 获取上个月的工作日过车信息，并存储到TM_VEHICLE_SCORES中间表
	 *
	 * @return 存储是否成功结果信息------------test----------------
	 */
	public int updateVehicheRecords1() {
		daoUtil.update("declare                                                                                     "
				+ "     num   number;                                                                                "
				+ "     begin                                                                                        "
				+ "         select count(1) into num from user_tables where table_name = upper('TM_VEHICLE_SCORES') ;"
				+ "         if num > 0 then                                                                          "
				+ "             execute immediate 'delete from TM_VEHICLE_SCORES' ;                                   "
				+ "         end if;                                                                                  "
				+ "     end;                                                                                         ");
		int result = daoUtil
				.update("insert into TM_VEHICLE_SCORES(Ori_Plate_No,A,B,C)"
						+ " (SELECT TDA.ORI_PLATE_NO,NVL(TDA.A,0) A, NVL(TDB.B,0) B,ROUND(NVL(TDA.A,0)/(NVL(TDA.A,0)+ NVL(TDB.B,0)),4) C FROM "
						+ " (select ORI_PLATE_NO,count(ORI_PLATE_NO) AS A FROM "
						+ " (SELECT ORI_PLATE_NO  FROM Td_Vehicle_Record_Day_Test"
						+ " WHERE "
						+ " DCOLLECTIONDATE between trunc(sysdate,'iw') - 7 and trunc(sysdate,'iw') - 3"
						// +
						// " to_char(DCOLLECTIONDATE,'yyyy-MM')= to_char(add_months(trunc(sysdate),-0),'yyyy-MM')"
						+ " AND ORI_PLATE_NO !='--------'"
						+ " AND to_char(DCOLLECTIONDATE, 'd') !=1 AND to_char(DCOLLECTIONDATE, 'd') !=7"
						+ " AND (to_char(DCOLLECTIONDATE, 'hh24') > 16 AND to_char(DCOLLECTIONDATE, 'hh24') < 20)"
						+ " OR (to_char(DCOLLECTIONDATE, 'hh24') > 6  AND to_char(DCOLLECTIONDATE, 'hh24')< 9)"
						+ " group by  ORI_PLATE_NO, to_char(DCOLLECTIONDATE,'yyyy-MM-dd')) "
						+ " group by ORI_PLATE_NO ) TDA  "
						+ " LEFT  JOIN" // 这个地方实际需要全外连接
						+ " (select ORI_PLATE_NO,count(ORI_PLATE_NO) AS B FROM "
						+ " (SELECT ORI_PLATE_NO FROM Td_Vehicle_Record_Day_Test" + " WHERE "
						+ " DCOLLECTIONDATE between trunc(sysdate,'iw') - 7 and trunc(sysdate,'iw') - 3"
						+ " AND ORI_PLATE_NO !='--------'"
						+ " AND to_char(DCOLLECTIONDATE, 'd') !=1 AND to_char(DCOLLECTIONDATE, 'd') !=7"
						+ " AND (to_char(DCOLLECTIONDATE, 'hh24') > 9 AND to_char(DCOLLECTIONDATE, 'hh24') < 16)"
						+ " group by ORI_PLATE_NO, to_char(DCOLLECTIONDATE,'yyyy-MM-dd')) group by ORI_PLATE_NO "
						+ " ) TDB " + " ON TDA.ORI_PLATE_NO = TDB.ORI_PLATE_NO)");
		return result;
	}

	/**
	 * 将聚类后的过车信息，分批存储到AV_VEHICLE_RESULTS表
	 *
	 * @return 存储是否成功结果信息
	 */
	public int updateVehicheResult(List<Point> commuterCars) {
		daoUtil.update("declare                                                                                     "
				+ "     num   number;                                                                                "
				+ "     begin                                                                                        "
				+ "         select count(1) into num from user_tables where table_name = upper('AV_VEHICLE_RESULTS') ;"
				+ "         if num > 0 then                                                                          "
				+ "             execute immediate 'delete from AV_VEHICLE_RESULTS' ;                                   "
				+ "         end if;                                                                                  "
				+ "     end;                                                                                         ");
		Point point = new Point();
		StringBuilder sb = new StringBuilder();
		final int batchSize = 1000;
		sb.append("insert all ");
		int result = 1;
		int size=commuterCars.size();
		if(size>=1000)
		{
			for (int i = 1; i < size; i++) {
				point = commuterCars.get(i);
				sb.append(" into AV_VEHICLE_RESULTS values");
				sb.append("(");
				sb.append("'" + point.getEntityID() + "',");
				sb.append(point.getPeakFrequency() + ",");
				sb.append(point.getUnpeakFrequency() + ",");
				sb.append(point.getOnRoadTime());
				sb.append(")");
				if (i % batchSize == 0) {
					sb.append("select 1 from dual");
					result = daoUtil.update(sb.toString());
					sb.delete(10, sb.length());
				}
			}
		}else
		{
			for (int i = 1; i < size; i++) {
				point = commuterCars.get(i);
				sb.append(" into AV_VEHICLE_RESULTS values");
				sb.append("(");
				sb.append("'" + point.getEntityID() + "',");
				sb.append(point.getPeakFrequency() + ",");
				sb.append(point.getUnpeakFrequency() + ",");
				sb.append(point.getOnRoadTime());
				sb.append(")");				
			}
			sb.append("select 1 from dual");
			result = daoUtil.update(sb.toString());
		}
		return result;
	}
	/**
	 * 插入昨天的出行天数占过去七天的车辆数量及占比
	 * @return 存储是否成功结果信息
	 */
	public int updateVehicleDayRatio(VehicleDayRatio vehicleDayRatio) {
		String sql="insert into AV_VEHICLE_DAY_RATIO_RESULTS(vehicleDay,vehicleDayRatio,vehicleNumber) values('"+vehicleDayRatio.getVehicleDay()+"',"+vehicleDayRatio.getVehicleDayRatio()+","+vehicleDayRatio.getVehicleNumber()+")";
		int result = daoUtil.update(sql);
		return result;
	}
}
