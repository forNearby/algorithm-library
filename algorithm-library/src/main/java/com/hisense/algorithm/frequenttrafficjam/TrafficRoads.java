package com.hisense.algorithm.frequenttrafficjam;

import com.hisense.dao.DataBaseEntity;
import com.hisense.service.FrequentTrafficJamService;
import org.apache.log4j.Logger;

/**
 * @author maxiao
 * @date 2018年10月31日
 */
public class TrafficRoads {
    private FrequentTrafficJamService service;
    private static Logger logger = Logger.getLogger(TrafficRoads.class);

    public TrafficRoads(DataBaseEntity dataBaseEntity) {
        this.service = new FrequentTrafficJamService(dataBaseEntity);
    }


    /**
     * 工作日常发堵点
     */
    public void workdayService() {
        int result = service.getWorkday();
        if (result == 1) {
            System.out.println("工作日常发堵点判断完成");
            logger.info("工作日常发堵点判断完成");
        }
    }

    /**
     * 非工作日常发堵点
     */
    public void weekdayService() {
        int result = service.getWeekday();
        if (result == 1) {
            System.out.println("非工作日常发堵点判断完成");
            logger.info("非工作日常发堵点判断完成");
        }
    }

    /**
     * 工作日早高峰常发堵点
     */
    public void workdayMorningPeakService() {
        int result = service.getWorkdayMorningPeak();
        if (result == 1) {
            System.out.println("工作日早高峰常发堵点判断完成");
            logger.info("工作日早高峰常发堵点判断完成");
        }
    }

    /**
     * 工作日晚高峰常发堵点
     */
    public void workdayEveningPeakService() {
        int result = service.getWorkdayEveningPeak();
        if (result == 1) {
            System.out.println("工作日晚高峰常发堵点判断完成");
            logger.info("工作日晚高峰常发堵点判断完成");
        }
    }

    /**
     * 判断周一到周天每天哪一些路口拥堵
     */
    public void whichDayTrafficJam() {
        int result = 0;
        for (int i = 1; i < 6; i++) {
            result = service.getDayWorkdayData(i);
            result = service.getPeakWorkdayData(i);
        }
        for (int i = 6; i < 8; i++) {
            result = service.getDayWorkdayData(i);
        }
        if (result == 1) {
            System.out.println("具体天常发堵点判断完成");
            logger.info("具体天常发堵点判断完成");
        }
    }

}
