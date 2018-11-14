package com.hisense.service;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.dao.DaoUtil;
import com.hisense.dao.DataBaseEntity;

public class IllegalEventService {
	private DaoUtil daoUtil = new DaoUtil();
	
    /**
     * 在违法数据表中获取违法信息并统计
     * @param  dataBaseEntity           数据源信息-违法信息表
     * @return Dataset<Row>
     * 获取成都市过往10分钟内去重后的车辆数
     * */
    public Dataset<Row> getIllegalEventCharaBySql(DataBaseEntity dataBaseEntity){
	    Date currentTime = new Date();  
	    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");  
	    String dateString = formatter.format(currentTime);  
    	//历史数据
    	String sql="(select dt, count(PICTIME1) as num from "
    			+"(select TO_DATE('2018-10-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS')- (level-1)/24 dt " 
    			+ "from dual connect by level <= 24*7) d left outer join "+dataBaseEntity.getTableName()+" i "
    					+"on 1=1 and "
    					+"i.PICTIME1<dt and i.PICTIME1> d.dt-1/24  "
    					+"group by d.dt order by d.dt) dbtable";
    	Dataset<Row> dataset = daoUtil.getTableByDataBaseInfo(sql, dataBaseEntity.getDatabaseAddress(), dataBaseEntity.getPort(), dataBaseEntity.getAccount(),dataBaseEntity.getUser(),dataBaseEntity.getPassword(),dataBaseEntity.getMaster());
    	return dataset;
    }
    
    /**
     * 将count中的数据插入到ILLEGVAL_COUNT_TEST
     * @param count  			需插入到表中的数据
     * @param dataBaseEntity	数据表信息
     * 插入数据
     */
    public void writeIllegalEventCharaBySql(double[] count,DataBaseEntity dataBaseEntity){
    	String sql="INSERT ALL ";
    	for(int i=0;i<24;i++){
    		if(i<9){
    			sql=sql+" into "+dataBaseEntity.getOutPutTablename()+" (begintime,endtime,ILLEGALAVE) "
    					+"values ('0"+i+":00:00','0"+(i+1)+":00:00',"+count[i]+")";
    			
    		}else if(i==9){
    			sql=sql+" into "+dataBaseEntity.getOutPutTablename()+" (begintime,endtime,ILLEGALAVE) "
    					+"values ('09:00:00','10:00:00',"+count[i]+") ";
    		}else{
    			sql=sql+" into "+dataBaseEntity.getOutPutTablename()+" (begintime,endtime,ILLEGALAVE) "
    					+"values ('"+i+":00:00','"+(i+1)+":00:00',"+count[i]+") ";
    		}
    	}
    	sql=sql+"select 1 from dual";
    	daoUtil.update(sql, dataBaseEntity.getOutPutDatabaseAddress(), dataBaseEntity.getOutPutAccount(), dataBaseEntity.getOutPutUser(), dataBaseEntity.getOutPutPassword(),dataBaseEntity.getOutPutPort());
    }
}





