/**
 * 
 */
package com.hisense.algorithm.traveldays;

import java.io.IOException;

/**
 * @author machao
 * @date 2018年11月7日
 * 车辆出行天数统计测试入口
 */
public class Test {
public static void main(String[] args) throws IOException {
	
	String[] arg = {"{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"database\":\"GXDB\",\"database_address\":\"20.2.11.9\",\"port\":\"1521\",\"account\":\"JHGX\",\"password\":\"JHGX111\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"}},\"operational_param\":{\"model\":\"CongestionIdentification\",\"roadid\":\"u3333\"},\"output\":{\"AV_VEHICLE_RESULTS\":{\"database\":\"GXDB\",\"database_address\":\"20.2.11.9\",\"port\":\"1521\",\"account\":\"JHGX\",\"password\":\"JHGX111\",\"tablename\":\"AV_VEHICLE_RESULTS\"},\"TM_VEHICLE_SCORES\":{\"database\":\"GXDB\",\"database_address\":\"20.2.11.9\",\"port\":\"1521\",\"account\":\"JHGX\",\"password\":\"JHGX111\",\"tablename\":\"TM_VEHICLE_SCORES\"},\"AV_VEHICLE_DAY_RATIO_RESULTS\":{\"database\":\"GXDB\",\"database_address\":\"20.2.11.9\",\"port\":\"1521\",\"account\":\"JHGX\",\"password\":\"JHGX111\",\"tablename\":\"AV_VEHICLE_DAY_RATIO_RESULTS\"}},\"spark_info\":{\"master\":\"local\"}}"};
	System.out.println(arg[0]);
	VehicleTravelDays.main(arg);
}
}
