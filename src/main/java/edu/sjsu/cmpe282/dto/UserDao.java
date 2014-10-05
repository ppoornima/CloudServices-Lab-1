package edu.sjsu.cmpe282.dto;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.json.JSONObject;

import edu.sjsu.cmpe282.domain.User;

public class UserDao {

	public static final int STATUS_SUCCESS_CODE = 200;
	public static final String STATUS_SUCCESS_MESSAGE = "success";
	public static final int STATUS_ERROR_CODE =500;
	public static final String STATUS_ERROR_MESSAGE="error";

	Connection conn = null;
	Statement stmt = null;

	// Constructure with JDBC connection
	public UserDao()
	{
		try{
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//conn = DriverManager.getConnection("jdbc:mysql://localhost/cloudservices","root","root");

			conn = DriverManager.getConnection("jdbc:mysql://cloudservices.cvz5dtczqgms.us-west-1.rds.amazonaws.com:3306/user","root","cloudservices"); 
			System.out.println("COnn:"+conn);
		}
		catch (SQLException e) {
			e.printStackTrace();

		}
	}


	public String getCurrentDateTime()
	{		  
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		java.util.Date date = new java.util.Date();
		System.out.println("Current Date Time : " + dateFormat.format(date));
		return dateFormat.format(date);
	}

	public String addUser(User user)
	{
		JSONObject response = new JSONObject();
		try {

			System.out.println(user.getEmail()+"\t"+user.getFirstName()+"\t"+user.getLastName()+"\t"+ user.getPasswd()+"\t");
			stmt = conn.createStatement();
			user.setLastLoginTime(getCurrentDateTime());
			String query = "INSERT INTO `cloudservices`.`user` (`email`, `firstname`, `lastname`, `password`,`lastloggedintime`) VALUES ('" + user.getEmail() + "', '" + user.getFirstName() + "', '" +user.getLastName()  + "', '" + user.getPasswd() + "','"+user.getLastLoginTime()+"');";
			stmt.executeUpdate(query);
			response.put("statusCode",STATUS_SUCCESS_CODE);
			response.put("statusMessage", STATUS_SUCCESS_MESSAGE);
			response.put("email", user.getEmail());
			response.put("firstName",user.getFirstName());
			//response.put("lastLoginTime", lastLoginTime);			

		} catch (SQLException e) {

			response.put("statusCode",STATUS_ERROR_CODE);
			response.put("statusMessage", STATUS_ERROR_MESSAGE);
			
		}
		
		return response.toString();
	}

public JSONObject updateTime(String email)
{
	JSONObject response = new JSONObject();
	try {
		stmt = conn.createStatement();

		String query = "update `cloudservices`.`user` set lastloggedintime = '"+getCurrentDateTime()+"'where email ='"+email+"';";
		stmt.executeUpdate(query);
	
	} catch (SQLException e) {
		
		e.printStackTrace();
		response.put("statusCode",STATUS_ERROR_CODE);
		response.put("statusMessage", STATUS_ERROR_MESSAGE+"\t Error updating last logged in time!");
		return response;
	
	}

	response.put("statusCode",STATUS_SUCCESS_CODE);
	response.put("statusMessage", STATUS_SUCCESS_MESSAGE+"\t last logged in time updated");
	
return response;
}

	public String checkUser(User user){
		ResultSet rs;
		String origPasswd = null;
		String firstName = null;
		String lastLoginTime = null ;
		JSONObject response = new JSONObject();

		try {
			stmt = conn.createStatement();
			String query = "Select * from cloudservices.user where email = '"+user.getEmail()+"';";
			rs = stmt.executeQuery(query);
			rs.next();
			origPasswd = rs.getString("password");
			firstName =  rs.getString("firstName");
			lastLoginTime =  rs.getString("lastloggedintime");
			response = updateTime(user.getEmail());
			System.out.println("Password from db : "+ origPasswd );
			System.out.println("Password entered : "+user.getPasswd());

		} catch (SQLException e) {
			
			response.put("statusCode",STATUS_ERROR_CODE);
			response.put("statusMessage", STATUS_ERROR_MESSAGE);
			return response.toString();
		}
		if(user.getPasswd().equals(origPasswd))
		{

			response.put("statusCode",STATUS_SUCCESS_CODE);
			response.put("statusMessage", STATUS_SUCCESS_MESSAGE);
			response.put("email", user.getEmail());
			response.put("firstName",firstName);
			response.put("lastLoginTime", lastLoginTime);


		}
		else
		{

			response.put("statusCode",STATUS_ERROR_CODE);
			response.put("statusMessage", STATUS_ERROR_MESSAGE);

		}

		return response.toString();
		//Response.status(201).entity("User Created : \n"+ user.getFirstName()).build();
	}

	
	
	public String getCatalogs()
	{
		
		return "";
		
		
	}
	
	
	private static void CreateClient()
	{
	  
	}  
	
	public String getProducts()
	{
		
		
		return"";
		
	}

	public static void main(String[] args)
	{



			 User u = new User();
		 ///u.setFirstName("pooja");
		// u.setLastName("prasad");
		 u.setEmail("pooja@gmail.com");
		 u.setPasswd("1234");
		 
		 new UserDao().checkUser(u);

		//System.out.println("Record inserted :" + dao.addUser(u));*/







	}

}
