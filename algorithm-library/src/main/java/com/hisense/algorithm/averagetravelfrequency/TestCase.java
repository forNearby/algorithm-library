package com.hisense.algorithm.averagetravelfrequency;

import java.math.BigDecimal;
import java.util.Calendar;

public class TestCase {
	public static void main(String[] args) {
		//String json ="{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"},\"TS_DEVICE\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_DEVICE\"}},\"operational_param\":{\"areaid\":\"41\",},\"output\":{\"AV_AVERAGE_TRAVEL_FREQUENCY\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"AV_AVERAGE_TRAVEL_FREQUENCY\"}},}";
		String json = "{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"},\"TS_FBD_INFO\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_FBD_INFO\"}},\"operational_param\":{\"fdb\":\"FBD_01C_09\"},\"output\":{\"TS_FBD_INFO\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_FBD_INFO\"}},}";
		json = new String(java.util.Base64.getEncoder().encode(json.getBytes()));
		System.out.println(json);
		json = new String(java.util.Base64.getDecoder().decode(json));
		System.out.println(json);
	}
	
}
