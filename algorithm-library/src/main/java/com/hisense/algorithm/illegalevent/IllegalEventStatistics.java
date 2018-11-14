package com.hisense.algorithm.illegalevent;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hisense.algorithm.vehiclesonroad.VehiclesOnRoad;
import com.hisense.dao.DataBaseEntity;
import com.hisense.service.IllegalEventService;
import com.hisense.util.IoUtil;

/**
 * 
 * @author weichengjie
 * @date   2018年11月6日
 */
public class IllegalEventStatistics {
	private static Logger logger = Logger.getLogger(VehiclesOnRoad.class);
	private String logInfo=null;
	
	/**
	 * 主函数
	 * @param  args   		传送json串
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String jSONString=new String(java.util.Base64.getDecoder().decode(args[0]));
		//String jSONString = IoUtil.readFile("src/com/hisense/util/data.json", "UTF-8");
		//String jSONString =args[0];
		//String jSONString="{\"datasource\":{\"TD_ILLEGALEVENT\":{\"database\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"port\":\"1521\",\"account\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"TD_ILLEGALEVENT\"}},\"output\":{\"AV_ILLEGVAL_STATISTICS\":{\"database\":\"hiatmp\",\"database_address\":\"192.168.0.108\",\"port\":\"1521\",\"account\":\"jhgx\",\"password\":\"jhgx\",\"tablename\":\"AV_ILLEGVAL_STATISTICS\"}},\"spark_info\":{\"master\":\"local\"}}";
		DataBaseEntity vehicleEntity=new DataBaseEntity();
		vehicleEntity.getInfo(jSONString,"TD_ILLEGALEVENT","AV_ILLEGVAL_STATISTICS");
		        
		long startTime = System.currentTimeMillis(); 
		IllegalEventStatistics i=new IllegalEventStatistics();
		i.getMouthIllegalEvent(vehicleEntity);
		long endTime = System.currentTimeMillis(); 
		
		System.out.println("程序运行时间：" + (endTime - startTime) + "ms");		
	}
	
	/**
	 * 查询过往一周内违法数据，统计每小时平均违法数并插入到表ILLEGVAL_COUNT_TEST
	 * @param dataBaseEntity datesource实体类
	 */
	public void getMouthIllegalEvent(DataBaseEntity dataBaseEntity){
		double[] tempArray=new double[24];
		int tempi=0;
		IllegalEventService illegalEventService=new IllegalEventService();
		Dataset<Row> dateSet =illegalEventService.getIllegalEventCharaBySql(dataBaseEntity);
		//dateSet.show();
		List<Row> list=dateSet.collectAsList();
		for(int i=0;i<list.size();i++){
			//System.out.println(list.get(i).getDecimal(1).intValue());
			tempi=i%24;
			tempArray[tempi]=tempArray[tempi]+list.get(i).getDecimal(1).doubleValue();
		}
		for(int i=0;i<tempArray.length;i++){
			tempArray[i]=tempArray[i]/7;
		}
		illegalEventService.writeIllegalEventCharaBySql(tempArray,dataBaseEntity);
		for(int i=0;i<tempArray.length;i++){
			logInfo=i+"--"+(i+1)+"点的平均违法数量为："+String.format("%.2f", tempArray[i]);
			logger.info(logInfo);
			System.out.println(logInfo);
		}		
	}
}
