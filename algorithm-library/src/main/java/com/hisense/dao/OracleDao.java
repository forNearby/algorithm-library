package com.hisense.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author machao
 * @date 2018年10月30日
 */
public class OracleDao {
    public Dataset<Row> daoAdapt(String dbtable, String dbView, String sql) {
        String master = "local";
        String appName = "SparkJdbc";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores", "1")
                //spark默认支持一个表20个左右的字段
                .config("spark.debug.maxToStringFields", "100")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .format("jdbc")
                //.option("url", "jdbc:oracle:thin:@//192.168.0.108:1521/hiatmp")
                .option("url", "jdbc:oracle:thin:@//20.2.15.18:1521/gxdb")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                //spark最多支持一次从数据库读出217万条左右的数据
                .option("dbtable", dbtable)
                //.option("user", "jhgx")
                .option("user", "JHGX")
                //.option("password", "jhgx")
                .option("password", "JHGX001")
                .load();
        dataset.createOrReplaceTempView(dbView);
        Dataset<Row> result = spark.sql(sql);
        return result;
    }

    public Dataset<Row> daoAdapt(String dbtable) {
        String master = "local";
        String appName = "SparkJdbc";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores", "1")
                .config("spark.debug.maxToStringFields", "100")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .format("jdbc")
                //.option("url", "jdbc:oracle:thin:@//192.168.0.108:1521/hiatmp")
                .option("url", "jdbc:oracle:thin:@//20.2.15.18:1521/gxdb")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("dbtable", dbtable)
                //.option("user", "jhgx")
                .option("user", "JHGX")
                //.option("password", "jhgx")
                .option("password", "JHGX001")
                .load();
        return dataset;
    }
    
    /**
     * 重载
     */
    public Dataset<Row> daoAdapt(String dbtable,String master1,String url,String user,String password) {
        String master = master1;
        String appName = "SparkJdbc";
        SparkSession spark = SparkSession
                .builder()
                //.master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores", "1")
                .config("spark.debug.maxToStringFields", "100")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .format("jdbc")
                //.option("url", "jdbc:oracle:thin:@//192.168.0.108:1521/hiatmp")
                .option("url", url)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("dbtable", dbtable)
                //.option("user", "jhgx")
                .option("user", user)
                //.option("password", "jhgx")
                .option("password", password)
                .load();
        return dataset;
    }

    /**
     * @param dbtable  从数据库查的表
     * @param dbView   放到内存中的表明
     * @param sql      从内存中操作表
     * @param ip       服务器IP地址
     * @param port     网络端口
     * @param account  数据库名
     * @param user     用户名
     * @param password 密码
     * @return 从数据库读取的结果
     */
    public Dataset<Row> daoAdaptByDataBaseInfo(String dbtable, String dbView, String sql, String ip, String port, String account, String user, String password, String master) {
        String portOri = "1521";
        if (port != null) {
            portOri = port;
        }
//        String master = "local";
        String appName = "SparkJdbc";
        SparkSession spark = SparkSession
                .builder()
               // .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores", "1")
                //spark默认支持一个表20个左右的字段
                .config("spark.debug.maxToStringFields", "100")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//" + ip + ":" + portOri + "/" + account)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                //spark最多支持一次从数据库读出217万条左右的数据
                .option("dbtable", dbtable)
                .option("user", user)
                .option("password", password)
                .load();
        dataset.createOrReplaceTempView(dbView);
        Dataset<Row> result = spark.sql(sql);
        return result;
    }

    public Dataset<Row> daoAdaptByDataBaseInfo(String dbtable, String ip, String port, String dataBaseName, String user, String password) {
        String portOri = "1521";
        if (port != null) {
            portOri = port;
        }
        String master = "local";
        String appName = "SparkJdbc";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores", "1")
                .config("spark.debug.maxToStringFields", "100")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//" + ip + ":" + portOri + "/" + dataBaseName)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("dbtable", dbtable)
                .option("user", user)
                .option("password", password)
                .load();
        return dataset;
    }
    public Dataset<Row> daoAdaptByDataBaseInfo(String dbtable, String ip, String port, String dataBaseName, String user, String password, String master) {
        String portOri = "1521";
        if (port != null) {
            portOri = port;
        }
        //String master = "local";
        String appName = "SparkJdbc";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores", "1")
                .config("spark.debug.maxToStringFields", "100")
                .getOrCreate();
        Dataset<Row> dataset = spark.read()
                .format("jdbc")
                .option("url", "jdbc:oracle:thin:@//" + ip + ":" + portOri + "/" + dataBaseName)
                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .option("dbtable", dbtable)
                .option("user", user)
                .option("password", password)
                .load();
        return dataset;
    }
    
}
