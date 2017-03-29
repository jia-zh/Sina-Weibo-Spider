package cn.edu.bistu.spider.dbutil;
import java.sql.Connection;  
import java.sql.PreparedStatement;  
import java.sql.ResultSet;  
import java.sql.SQLException;
import java.sql.Statement;

import cn.edu.bistu.spider.model.UserModel;
import cn.edu.bistu.spider.model.WeiboModel;  
 
/**
 * 数据库相关操作
 * @author Jia Zheng
 */
public class MySQLDBHelper { 
    
	/**执行不带参数的数据库查询操作
	 * @param conn Connection对象
	 * @param sql 查询的sql语句
	 * @return 查询结果ResulSet
	 */
	public static ResultSet ExecuteQuery(Connection conn,String sql) {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
		}catch (SQLException e) {			
			System.out.println("数据库查询出错！" + e.toString());
		}
		return rs;
	}

	/**执行带参数的数据库查询操作
	 * @param conn Connection对象
	 * @param sql 查询的sql语句
	 * @param params 参数数组
	 * @return 查询结果ResulSet
	 */
	public static ResultSet ExecuteQuery(Connection conn,String sql,Object[]params) {
		PreparedStatement pstmt=null;
		ResultSet rs = null;
		try{
			pstmt=conn.prepareStatement(sql);
			for(int i=0;i<params.length;i++)
			{
				pstmt.setObject(i+1,params[i]);
			}
			rs=pstmt.executeQuery();
		}catch(SQLException e){
			System.out.println("数据库查询出错！" + e.toString());
		}
		return rs;
	}
	
	/**执行不带参数的数据库更新操作
	 * @param conn Connection对象
	 * @param sql 更新的sql语句
	 * @return 更新操作影响的行数
	 */
	public static int ExecuteUpdate(Connection conn,String sql)  {
		Statement stmt = null;
		int count = 0;
		try {
			stmt = conn.createStatement();
			count = stmt.executeUpdate(sql);
		}catch (SQLException e) {
			System.out.println("数据库更新出错！" + e.toString());
		}
		return count;
	}
	
	/**执行带参数的数据库更新操作
	 * @param conn Connection对象
	 * @param sql 更新的sql语句
	 * @param params 参数数组
	 * @return 更新操作影响的行数
	 */
	public static int ExecuteUpdate(Connection conn,String sql,Object[]params) {
		PreparedStatement pstmt=null;
		int count = 0;
		try {
			pstmt=conn.prepareStatement(sql);
			for(int i=0;i<params.length;i++)
			{
				pstmt.setObject(i+1,params[i]);
			}
			count = pstmt.executeUpdate();
		}catch (SQLException e) {
			System.out.println("数据库更新出错！" + e.toString());
		}
		return count;
	}
	
    /**
     * 插入基本信息
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserUserBasicInfor(Connection connection,UserModel userModel) {
    	int inserts = 0;
		String selectSql = "select * from user where id = ?";
		Object[] selectPars = { userModel.getId()}; 
        try {
			ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (!selects.next()) {
				String insertSql = "insert into user(id,nickname,birthday,gender,marriage,address,signature,homeUrl,profile) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
				Object[] insertPars = { userModel.getId(),
						                userModel.getNickname(),
						                userModel.getBirthday(),
						                userModel.getGender(),
						                userModel.getMarriage(),
						                userModel.getAddress(),
						                userModel.getSignature(),
						                userModel.getHomeUrl(),
						                userModel.getProfile()};
				inserts = ExecuteUpdate(connection,insertSql,insertPars);
				System.out.println("插入1个用户！");
			}
			else {
				String updateSql = "update user set nickname = ?,birthday= ?, gender= ?, marriage= ?, address= ?, signature= ?, homeUrl= ?, profile= ? where id = ?";
				Object[] updatePars = { userModel.getNickname(),
						                userModel.getBirthday(),
						                userModel.getGender(),
						                userModel.getMarriage(),
						                userModel.getAddress(),
						                userModel.getSignature(),
						                userModel.getHomeUrl(),
						                userModel.getProfile(),
						                userModel.getId()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("更新1个用户！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
    
    /**
     * 插入标签信息
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserUserTagsInfor(Connection connection,UserModel userModel) {
    	int inserts = 0;
		String selectSql = "select * from user where id = ?";
		Object[] selectPars = { userModel.getId()}; 
        try {
			ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (selects.next()) {
				String updateSql = "update user set tags = ? where id = ?";
				Object[] updatePars = { userModel.getTags(),
										userModel.getId()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("更新1个用户！");
			}
			else{
				String updateSql = "insert into user (id, tags) values (?, ?)";
				Object[] updatePars = { userModel.getId(),
						                userModel.getTags()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("插入1个用户！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
    
    /**
     * 插入微博数目、关注数目、粉丝数目
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserUserNumInfor(Connection connection,UserModel userModel) {
    	int inserts = 0;
		String selectSql = "select * from user where id = ?";
		Object[] selectPars = { userModel.getId()}; 
        try {
			ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (selects.next() ) {
				String updateSql = "update user set weibo_num = ?, follows_num = ?, fans_num = ? where id = ?";
				Object[] updatePars = { userModel.getWeiboNum(),
						                userModel.getFollowsNum(),
						                userModel.getFansNum(),
										userModel.getId()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("更新1个用户！");
			}
			else{
				String updateSql = "insert into user (id, weibo_num, follows_num,fans_num) values (?, ?, ?, ?)";
				Object[] updatePars = { userModel.getId(),
										userModel.getWeiboNum(),
						                userModel.getFollowsNum(),
						                userModel.getFansNum()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("插入1个用户！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
    
    /**
     * 插入粉丝
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserUserFansInfor(Connection connection,UserModel userModel) {
    	int inserts = 0;
		String selectSql = "select * from user where id = ?";
		Object[] selectPars = { userModel.getId()}; 
        try {
        	ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (selects.next()) {
				String selectSqls = "select fans from user where id = ?";
				ResultSet selectFans = ExecuteQuery(connection,selectSqls,selectPars);
				if (selectFans.next()) {
					String fans = selectFans.getString(1);
					if(fans == null || fans == ""){
						String updateSql = "update user set fans = ? where id = ?";
						Object[] updatePars = { userModel.getFans(),
												userModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);
						System.out.println("更新1个用户！");
					}
					if(!fans.contains(userModel.getFans())){
						String newFans = fans + "," + userModel.getFans();
						String updateSql = "update user set fans = ? where id = ?";
						Object[] updatePars = { newFans,
												userModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);						
					}
				}
				else {
					String updateSql = "update user set fans = ? where id = ?";
					Object[] updatePars = { userModel.getFans(),
											userModel.getId()};
					inserts = ExecuteUpdate(connection,updateSql,updatePars);
					System.out.println("更新1个用户！");
				}
			}
			else{
				String updateSql = "insert into user (id, fans) values (?, ?)";
				Object[] updatePars = { userModel.getId(),
										userModel.getFans()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("插入1个用户！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
    
    /**
     * 插入关注者
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserUserFollowsInfor(Connection connection,UserModel userModel) {
    	int inserts = 0;
		String selectSql = "select * from user where id = ?";
		Object[] selectPars = { userModel.getId()}; 
        try {
        	ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (selects.next()) {
				String selectSqls = "select follows from user where id = ?";
				ResultSet selectFollows = ExecuteQuery(connection,selectSqls,selectPars);
				if (selectFollows.next()) {
					String follows = selectFollows.getString(1);
					if(follows == null || follows == ""){
						String updateSql = "update user set follows = ? where id = ?";
						Object[] updatePars = { userModel.getFollows(),
												userModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);						
					}
					else if(!follows.contains(userModel.getFollows())){
						String newFollows = follows + "," + userModel.getFollows();
						String updateSql = "update user set follows = ? where id = ?";
						Object[] updatePars = { newFollows,
												userModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);
					}
				}
				else{
					String updateSql = "update user set follows = ? where id = ?";
					Object[] updatePars = { userModel.getFollows(),
											userModel.getId()};
					inserts = ExecuteUpdate(connection,updateSql,updatePars);	
				}
				System.out.println("更新1个用户！");
			}
			else{
				String updateSql = "insert into user (id, follows) values (?, ?)";
				Object[] updatePars = { userModel.getId(),
										userModel.getFollows()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("插入1个用户！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
        
    /**
     * 插入微博
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserWeiboInfor(Connection connection,WeiboModel weiboModel) {
    	int inserts = 0;
		String selectSql = "select * from weibo where id = ?";
		Object[] selectPars = { weiboModel.getId()}; 
        try {
			ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (!selects.next()) {
				String insertSql = "insert into weibo values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				Object[] insertPars = { weiboModel.getId(),
						                weiboModel.getUserid(),
						                weiboModel.getContent(),
						                weiboModel.getLikes(),
						                weiboModel.getTransfers(),
						                weiboModel.getComments(),
						                weiboModel.getTime(),
						                weiboModel.getPlatform(),
						                weiboModel.getRepostusers(),
						                weiboModel.getCommentusers()};
				inserts = ExecuteUpdate(connection,insertSql,insertPars);
				System.out.println("插入1条微博！");
			}
			else {
				String updateSql = "update weibo set userid =?,content =?,likes=?,transfers=?,comments=?,time=?,platform=? where id = ?";
				Object[] insertPars = { weiboModel.getUserid(),
						                weiboModel.getContent(),
						                weiboModel.getLikes(),
						                weiboModel.getTransfers(),
						                weiboModel.getComments(),
						                weiboModel.getTime(),
						                weiboModel.getPlatform(),
						                weiboModel.getId()};
				inserts = ExecuteUpdate(connection,updateSql,insertPars);
				System.out.println("更新1条微博！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
    
    /**
     * 插入转发者
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserWeiboRepostUsers(Connection connection,WeiboModel weiboModel) {
    	int inserts = 0;
		String selectSql = "select * from weibo where id = ?";
		Object[] selectPars = { weiboModel.getId()}; 
        try {
        	ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (selects.next()) {
				String selectSqls = "select repostusers from weibo where id = ?";
				ResultSet selectRepostUsers = ExecuteQuery(connection,selectSqls,selectPars);
				if (selectRepostUsers.next()) {
					String repostUsers = selectRepostUsers.getString(1);
						if(repostUsers == null || repostUsers == ""){
						String updateSql = "update weibo set repostusers = ? where id = ?";
						Object[] updatePars = { weiboModel.getRepostusers(),
												weiboModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);
						System.out.println("更新1条微博！");
					}else if(!repostUsers.contains(weiboModel.getRepostusers())){
						String newRepostUsers = repostUsers + "," + weiboModel.getRepostusers();
						String updateSql = "update weibo set repostusers = ? where id = ?";
						Object[] updatePars = { newRepostUsers,
												weiboModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);
					}
				}
				else{
					String updateSql = "update weibo set repostusers = ? where id = ?";
					Object[] updatePars = { weiboModel.getRepostusers(),
											weiboModel.getId()};
					inserts = ExecuteUpdate(connection,updateSql,updatePars);
					System.out.println("更新1条微博！");
				}
			}
			else{
				String updateSql = "insert into weibo (id, repostusers) values (?, ?)";
				Object[] updatePars = { weiboModel.getId(),
						                weiboModel.getRepostusers()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("插入1条微博！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
    
    /**
     * 插入评论者
     * @param connection
     * @param userModel
     * @return
     */
    public static int inserWeiboCommentUsers(Connection connection,WeiboModel weiboModel) {
    	int inserts = 0;
		String selectSql = "select * from weibo where id = ?";
		Object[] selectPars = { weiboModel.getId()}; 
        try {
        	ResultSet selects = ExecuteQuery(connection,selectSql,selectPars);
			if (selects.next()) {
				String selectSqls = "select commentusers from weibo where id = ?";
				ResultSet selectCommentUsers = ExecuteQuery(connection,selectSqls,selectPars);
				if (selectCommentUsers.next()) {
					String commentUsers = selectCommentUsers.getString(1);
					if(commentUsers == null || commentUsers == ""){
						String updateSql = "update weibo set commentusers = ? where id = ?";
						Object[] updatePars = { weiboModel.getCommentusers(),
												weiboModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);
						System.out.println("更新1条微博！");
					}
					else if(!commentUsers.contains(weiboModel.getCommentusers())){
						String newCommentUsers = commentUsers + "," + weiboModel.getCommentusers();
						String updateSql = "update weibo set commentusers = ? where id = ?";
						Object[] updatePars = { newCommentUsers,
												weiboModel.getId()};
						inserts = ExecuteUpdate(connection,updateSql,updatePars);
					}
				}
				else{
					String updateSql = "update weibo set commentusers = ? where id = ?";
					Object[] updatePars = { weiboModel.getCommentusers(),
											weiboModel.getId()};
					inserts = ExecuteUpdate(connection,updateSql,updatePars);
					System.out.println("更新1条微博！");
				}
			}
			else{
				String updateSql = "insert into weibo (id, commentusers) values (?, ?)";
				Object[] updatePars = { weiboModel.getId(),
										weiboModel.getCommentusers()};
				inserts = ExecuteUpdate(connection,updateSql,updatePars);
				System.out.println("插入1条微博！");
			}
		} catch (Exception sqlE) {
			// TODO Auto-generated catch block
			System.out.println("查询出错"); 
		}    	
    	return inserts;		
	}
}  