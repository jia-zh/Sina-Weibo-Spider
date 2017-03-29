package cn.edu.bistu.spider.dbutil;

import java.sql.*;

/**
 * MySQL数据库连接工具
 * @author Jia Zheng
 */
public class MySQLConnManager {
	public final static String DRIVER = "com.mysql.jdbc.Driver"; // 数据库驱动  
    public final static String URL = "jdbc:mysql://localhost:3306/sina?useSSL=false";  //useUnicode=true&characterEncoding=UTF-8&
    public final static String DBUSER = "root";   // 数据库用户名  
    public final static String DBPASS = "123456"; // 数据库密码  
  
    /* 
     * 得到数据库连接   
     * @throws Exception  
     * @return 数据库连接对象 
     */  
    public static Connection creatConnection() {  
        Connection dbConnection = null;  
        try {  
            // 把JDBC驱动类装载入Java虚拟机中  
            Class.forName(DRIVER);  
            // 加载驱动，并与数据库建立连接  
            dbConnection = DriverManager.getConnection(URL,DBUSER, DBPASS);  
            dbConnection.prepareStatement("set names utf8mb4").executeQuery();
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
        return dbConnection;  
    }  
  
    public static void closeConnection(Connection dbConnection) {  
        try {  
            if (dbConnection != null && !dbConnection.isClosed()) {  
                dbConnection.close();  
            }  
        } catch (SQLException sqlEx) {  
            sqlEx.printStackTrace();  
        }  
    }  
    public static void closeResultSet(ResultSet res){  
        try{  
            if(res != null){  
                res.close();  
                res = null;  
            }  
        }catch(SQLException e){  
            e.printStackTrace();  
        }  
    }  
    public static void closeStatement(PreparedStatement pStatement){  
        try{  
            if(pStatement != null){  
                pStatement.close();  
                pStatement = null;  
            }  
        }catch(SQLException e){  
            e.printStackTrace();  
        }  
    }  
}
