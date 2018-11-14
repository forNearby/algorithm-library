/**
 * 
 */
package com.hisense.algorithm.commuter;

import java.io.IOException;

/**
 * @author machao
 * @date 2018年11月5日
 */
public class Test {
public static void main(String[] args) throws IOException {	
	String[] arg = {"{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"}},\"operational_param\":{\"model\":\"CongestionIdentification\",\"roadid\":\"u3333\"},\"output\":{\"AV_VEHICLE_RESULTS\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"AV_VEHICLE_RESULTS\"},\"TM_VEHICLE_SCORES\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TM_VEHICLE_SCORES\"},\"AV_VEHICLE_DAY_RATIO_RESULTS\":{\"account\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"user\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"AV_VEHICLE_DAY_RATIO_RESULTS\"}},\"spark_info\":{\"master\":\"local\"}}"};
	System.out.println(arg[0]);
	JudgeVehicle.main(arg);
}
}
