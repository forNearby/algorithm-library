package com.hisense.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class IoUtil {
	private static Logger logger = Logger.getLogger(IoUtil.class);
	//按字符读取文件
	public static String readFileByChar(String filePath){        
        char[] buffer=new char[1024];
        StringBuffer stringBuffer=new StringBuffer();
        int count=0;
        File file=new File(filePath);
		if(file.isFile()&&file.exists()){
			try{
                FileReader inFile=new FileReader(file);
                while(-1!=(count=inFile.read(buffer))){
                        stringBuffer.append(new String(buffer,0,count));
                }
                inFile.close();
                
        }catch(IOException ex){
                ex.printStackTrace();
        }	
		}else {
			logger.info("数据不存在!!");
		}      
        
        return stringBuffer.toString();
}
	//按行读取文件，拼接存储
	public static String readFile(String filePath,String encode) throws IOException{
		if(encode==null||encode==""){
			encode="GBK";
		}
		String txt=null;
		String out="";
		File file=new File(filePath);
		if(file.isFile()&&file.exists()){
			InputStreamReader read=new InputStreamReader(new FileInputStream(file),encode);
			BufferedReader bufferedReader=new BufferedReader(read);
			try {
				while((txt=bufferedReader.readLine())!=null){
					out+=txt.toString();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			read.close();
		}else
		{
			logger.info("数据不存在!");
		}
		return out;
	}
	//按行读取文件,按行存入储集合
		public static List<String> readFile2(String filePath,String encode) throws IOException{
			if(encode==null||encode==""){
				encode="GBK";
			}
			String txt=null;
			List<String> out=new ArrayList<>();
			File file=new File(filePath);
			if(file.isFile()&&file.exists()){
				InputStreamReader read=new InputStreamReader(new FileInputStream(file),encode);
				BufferedReader bufferedReader=new BufferedReader(read);
				try {
					while((txt=bufferedReader.readLine())!=null){
						out.add(txt.toString());
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				read.close();
			}else
			{
				logger.info("额，，不存在啊!!");
			}
			return out;
		}
	//输出文件    Additional代表是否输出到同一个文件
	public static void outputFile(String filePath ,String content,String name,Boolean Additional) throws IOException{
		FileOutputStream fs = new FileOutputStream(new File(filePath+name+".txt"),Additional);
		PrintStream p = new PrintStream(fs);
		p.println(content);
		p.close();
		
	}
	public static void main(String[] args) {
		String filePath="E:/article.txt";
		try {
			readFile(filePath,"GBK");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
