package com.hisense.algorithm.congestionidentification;

import com.hisense.dao.DataBaseEntity;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Author: maxiao
 * @Date: 2018/11/5 14:09
 */
public class CdTest {
    public static void main(String[] args) throws IOException {
        String jSONString = new String(java.util.Base64.getDecoder().decode(args[0]));
//        String jSONString = args[0];
        Logger logger = Logger.getLogger(CdTest.class);
        DataBaseEntity dBEntity = new DataBaseEntity();
        dBEntity.getInfo(jSONString, "road_info", "result_info");

        TrafficTime test = new TrafficTime(dBEntity);
        long startTime = System.currentTimeMillis();
        List<JamRoadTime> mark = test.jungleTrafficTime(30);
        if (mark.isEmpty()) {
            System.out.println("没有匹配数据");
            logger.info("没有匹配数据");
        } else {
            test.insertDB(mark, 1);
        }
        List<JamRoadTime> mark2 = test.manyDay(30);
        if (mark2.isEmpty()) {
            System.out.println("没有匹配数据");
            logger.info("没有匹配数据");
        } else {
            test.insertDB(mark2, 2);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间： " + (endTime - startTime) + "ms");
    }
}
