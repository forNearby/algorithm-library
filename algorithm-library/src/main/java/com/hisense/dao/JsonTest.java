/**
 *
 */
package com.hisense.dao;

import java.io.IOException;

import com.alibaba.fastjson.JSONObject;
import com.hisense.util.IoUtil;


/**
 * @author machao
 * @date 2018年11月2日
 */
public class JsonTest {
    public static void main(String[] args) throws IOException {
        String jSONString = IoUtil.readFile("src/com/hisense/util/data.json", "UTF-8");
        JSONObject jsonObject = JSONObject.parseObject(jSONString);
        JSONObject datasource = jsonObject.getJSONObject("datasource");
        JSONObject CarInfo = datasource.getJSONObject("car_info");
        JSONObject RoadInfo = datasource.getJSONObject("road_info");
        JSONObject OperationalParam = jsonObject.getJSONObject("operational_param");
        JSONObject output = jsonObject.getJSONObject("output");
        JSONObject ResultInfo = output.getJSONObject("result_info");
        JSONObject SparkInfo = jsonObject.getJSONObject("spark_info");
        System.out.println(ResultInfo.toString());
    }
}
