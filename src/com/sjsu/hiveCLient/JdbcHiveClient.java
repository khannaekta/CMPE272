package com.sjsu.hiveCLient;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JdbcHiveClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	//  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Connection conHive;
	private static Connection con;
	private static final String QRY_TOT = "select count(*) from sfpd";
	private static final String QRY_LOC = "select Y,X, count(*) as CNT  from sfpd group by Y,X sort by CNT desc limit 20";
	private static final String QRY_MONTH = "select (substr(DateofIncident,0,2)) , count(*) as CNT from sfpd group by (substr(DateofIncident,0,2)) order by cnt desc";
	private static final String QRY_HOUR = "SELECT substr(time,0,2), COUNT(*) AS CNT FROM SFPD GROUP BY substr(time,0,2) SORT BY CNT DESC";
	private static final String QRY_DAY = "select DayOfWeek, count(*) as CNT from sfpd group by DayOfWeek";
	private static final String QRY_YEAR = "select (substr(DateofIncident,7,4)) AS YR, count(*) as CNT from sfpd group by (substr(DateofIncident,7,4)) order by YR";
	private static final String QRY_CAT = "select category, count(*) as CNT  from sfpd group by category sort by CNT desc limit 25";
	private static final String QRY_LOCHR = "select address, substr(Time,0,2) , count(*) as CNT from sfpd group by address, substr(time,0,2) order by cnt desc limit 50";
	private static final String QRY_MYSQL = "SELECT * FROM DATAEXTRACT WHERE ID = ?";
	private static final String QRY_MYSQL_STATS = "SELECT * FROM STATS";
	private static final String INSERT_QRY = "INSERT INTO DATAEXTRACT";
	private static final String VAL = " VALUES ";
	private static final String OPEN_BRAC  = "(";
	private static final String CLOSE_BRAC  = ")";
	private static final String QUES = "?";
	private static final String COMMA = ",";
	private static final String INSERT_DATA = "INSERT INTO DATAEXTRACT VALUES (?,?,?)";
	private static final String QRY_STATS = "select category , substr(DateofIncident,4,2) as month, substr(DateofIncident,7,4) as yr, substr(time,0,2) as hr, y,x,address, count(*) as cnt from sfpd group by category , substr(DateofIncident,4,2), substr(DateofIncident,7,4), substr(time,0,2), x, y,address order by cnt desc, yr desc limit 1000";
	private static final String INSERT_STATS = "INSERT INTO STATS VALUES (?,?,?,?,?,?,?,?);";
	private static final String DELETE_QRY = "DELETE FROM DATAEXTRACT";
	private static final String DELETE_STATS = "DELETE FROM STATS";
	private static final String QRY_TRF = "select count(*) from sfpd where description like '%TRAFFIC%'";
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static final String DB_URL = "jdbc:mysql://";
	private static String DB_HOST = "localhost";
	private static String DB_NAME = "hive";
	private static String DB_USER = "root";
	private static String DB_PASS = "Score";
	private static String QUERY_HIVE = "False";
	private static String HIVE_SERVER = "192.168.221.136";
	private static String HIVE_USER = "cloudera";
	private static String HIVE_PASS = "cloudera";
	private static final String filename ="config.properties";
	private static String JSON_FILE_PATH = "C:\\mywork\\eclipsews\\sim\\CMPE272Hive\\";
	
	public JdbcHiveClient(){
		init();
	}
	
	private void init() {
		// TODO Auto-generated method stub

		InputStream input = null;
		String path = System.getenv("HIVE_CONF");
		Properties prop = new Properties();

		try {
			input = new FileInputStream(path+filename);
			// load a properties file
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		// TODO Auto-generated method stub
		DB_HOST=prop.getProperty("DB_HOST");
		DB_NAME=prop.getProperty("DB_NAME");
		DB_USER=prop.getProperty("DB_USER");
		DB_PASS=prop.getProperty("DB_PASS");
		QUERY_HIVE=prop.getProperty("QUERY_HIVE");
		HIVE_SERVER=prop.getProperty("HIVE_SERVER");
		HIVE_USER=prop.getProperty("HIVE_USER");
		HIVE_PASS=prop.getProperty("HIVE_PASS");
		JSON_FILE_PATH=prop.getProperty("JSON_FILE_PATH");
	
	}

	private void initDBConnection()
	{
		try 
		{
			String flag = QUERY_HIVE ;
			Class.forName(JDBC_DRIVER);
			con = DriverManager.getConnection(DB_URL+DB_HOST+"/"+DB_NAME,DB_USER,DB_PASS);
			if (flag.equalsIgnoreCase("true")){
				try {
					Class.forName(driverName);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(1);
				}
				conHive = DriverManager.getConnection("jdbc:hive2://"+HIVE_SERVER+":10000/default", HIVE_USER, HIVE_PASS);
				System.out.println("ConnHive"+conHive.toString());
				PreparedStatement stmt = con.prepareStatement(DELETE_QRY);
				stmt.executeUpdate();
				stmt.close();
				stmt = con.prepareStatement(DELETE_STATS);
				stmt.executeUpdate();
				stmt.close();
				runHiveQuery(QRY_TOT,"QRY_TOT");
				runHiveQuery(QRY_LOC,"QRY_LOC");
				runHiveQuery(QRY_DAY,"QRY_DAY");
				runHiveQuery(QRY_HOUR,"QRY_HOUR");
				runHiveQuery(QRY_MONTH,"QRY_MONTH");
				runHiveQuery(QRY_YEAR,"QRY_YEAR");
				runHiveQuery(QRY_TRF,"QRY_TRF");
				runHiveQuery(QRY_CAT,"QRY_CAT");
				runHiveQuery(QRY_LOCHR,"QRY_LOCHR");
				runHiveQuery(QRY_STATS, "STATS");
				conHive.close();
			}
		} 
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (SQLException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void runHiveQuery(String query,String id) {
		// TODO Auto-generated method stub
		Statement stmt = null;
		PreparedStatement st = null;
		ResultSet res = null;
		ResultSetMetaData rsmd = null;
		System.out.println("Hive Query -->" + query);

		try {
			stmt = conHive.createStatement();
			res = stmt.executeQuery(query);
			ResultSet temp = res;
			rsmd = res.getMetaData();
			int columns = rsmd.getColumnCount();
			int i = 1;
			String sql="";
			if (!id.equals("STATS")){
				sql = sql + INSERT_QRY + OPEN_BRAC + "ID" + COMMA;
				while(i<columns){
					sql = sql + "val" + i++ + COMMA;
				}
				sql = sql + "val" + i++ + CLOSE_BRAC + " VALUES" + OPEN_BRAC + QUES + COMMA;
				i =1 ;
				while(i<columns){
					sql = sql + QUES + COMMA;
					i++;
				}
				sql = sql + QUES  + CLOSE_BRAC;
				i =1 ;
				st= con.prepareStatement(sql);
				System.out.println("SQL --> "+ sql);
			}
			else 
				st= con.prepareStatement(INSERT_STATS);
			
			while(res.next()){
				i=1;
				int j=1;
				if (!id.equals("STATS")){
					st.setString(i,id);
					i++;
				}
				while(j<=columns){
					st.setString(i, res.getString(j));
					j++; i++;
				}
				st.executeUpdate();
			}
			System.out.println("Statement "+ st.toString() );
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				st.close();
				res.close();
				stmt.close();
			}catch(SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}

	public JSONObject queryDB(String sql, String key)//, String rowName , String colName)
	{
		System.out.println("Running: " + sql);
		JSONObject jsonobj = new JSONObject();
		JSONArray jarray = new JSONArray();
		ResultSet res = null;
		try{
			PreparedStatement stmt = con.prepareStatement(sql);
			int flag=1,i;
			if(!key.equals("STATS")){
				stmt.setString(1, key);
				flag=2;
			}
			res = stmt.executeQuery();
			ResultSetMetaData rsmd = res.getMetaData();
			int columns = rsmd.getColumnCount();
			while (res.next()) {
				JSONObject row = new JSONObject();
				i = flag;
				while(i <= columns){
					if( res.getString(i) == null)
						break;
					row.put(rsmd.getColumnName(i), res.getString(i));
//					System.out.println(res.getString(i) + "\t" + res.getString(3));
					i++;
				}
				
				jarray.put(row);
			}
			res.close();
			jsonobj.put("returndata", jarray);
			return jsonobj;

		}catch (JSONException e){
			e.printStackTrace();
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	public ResultSet querySQL(String sql)
	{
		System.out.println("Running: " + sql);
		ResultSet res = null;
		try{
			PreparedStatement stmt = con.prepareStatement(sql);
			res = stmt.executeQuery();
			return res;

		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
	
	private void writeJsonToFile(JSONObject obj, String fileName){
		FileWriter file = null ;
		try {
			file = new FileWriter(JSON_FILE_PATH +fileName);
		
			file.write(obj.toString());
			System.out.println("Successfully Copied JSON Object to File...");
		}catch (Exception e){
			e.printStackTrace();
		}
		finally{
			try {
				file.flush();
				file.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws SQLException {
		JdbcHiveClient obj = new JdbcHiveClient();
		obj.initDBConnection();
		JSONObject jsonobj = new JSONObject();
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_TOT");//,"Longitude","Latitude");
		obj.writeJsonToFile(jsonobj,"QRY_TOT.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_LOC");//,"Longitude","Latitude");
		obj.writeJsonToFile(jsonobj,"QRY_LOC.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_MONTH");//,"Month","Count");
		obj.writeJsonToFile(jsonobj,"QRY_MONTH.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_HOUR");//,"Hour","Count");
		obj.writeJsonToFile(jsonobj,"QRY_HOUR.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_DAY");//,"Day","Count");
		obj.writeJsonToFile(jsonobj,"QRY_DAY.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_YEAR");//,"Year","Count");
		obj.writeJsonToFile(jsonobj,"QRY_YEAR.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_TRF");//,"Year","Count");
		obj.writeJsonToFile(jsonobj,"QRY_TRF.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_CAT");//,"Category","Count");
		obj.writeJsonToFile(jsonobj,"QRY_CAT.txt");
		jsonobj = obj.queryDB(QRY_MYSQL,"QRY_LOCHR");//,"Year","Count");
		obj.writeJsonToFile(jsonobj,"QRY_LOCHR.txt");
		jsonobj = obj.queryDB(QRY_MYSQL_STATS,"STATS");//,"","");
		obj.writeJsonToFile(jsonobj,"STATS.txt");
		ResultSet rs = obj.querySQL("Select latitude,longitude from ukdata where Road_Surface_Conditions = '1'");
		while (rs.next()){
			System.out.println("Latitude("+ rs.getString(1)+"), Longitude("+rs.getString(2)+")");
		}
	}

}
