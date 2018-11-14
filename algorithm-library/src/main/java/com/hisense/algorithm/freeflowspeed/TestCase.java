package com.hisense.algorithm.freeflowspeed;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TestCase {

	public static void main(String[] args) {
		String s = "{\"datasource\":{\"TD_VEHICLE_RECORD_DAY\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TD_VEHICLE_RECORD_DAY\"},\"TS_FBD_INFO\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_FBD_INFO\"}},\"operational_param\":{\"fdb\":\"FBD_01C_09\"},\"output\":{\"TS_FBD_INFO\":{\"account\":\"gxdb\",\"database_address\":\"20.2.15.18\",\"user\":\"JHGX\",\"password\":\"JHGX001\",\"tablename\":\"TS_FBD_INFO\"}},\"spark_info\":{\"master\":\"local\"}}";
		JSONObject parseObject = JSON.parseObject(s);
		JSONObject jsonObject = parseObject.getJSONObject("datasource");
		JSONObject jsonObject2 = jsonObject.getJSONObject("TD_VEHICLE_RECORD_DAY");
		String string = jsonObject2.getString("database_address");
		System.out.println(string);
		
		String string2 = parseObject.getJSONObject("datasource").getJSONObject("TS_FBD_INFO").getString("database_address");
		System.out.println(string2);
		
		
		String string3 = parseObject.getJSONObject("operational_param").getString("fdb");
		System.out.println(string3);
		
	}
}
