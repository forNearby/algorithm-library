package com.hisense.test;

import org.apache.log4j.Logger;

/**
 * @author machao
 * @date 2018年10月30日
 */
public class LogTest {
    private static Logger logger = Logger.getLogger(LogTest.class);

    public static void main(String[] args) throws Exception {

        // 记录debug级别的信息
        logger.debug("This is debug message.");
        // 记录info级别的信息
        logger.info("This is info message.");
        // 记录error级别的信息
        logger.error("This is error message.");
    }
}
