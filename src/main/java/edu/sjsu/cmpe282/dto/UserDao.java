package edu.sjsu.cmpe282.dto;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.fasterxml.jackson.databind.introspect.WithMember;

import edu.sjsu.cmpe282.domain.User;

public class UserDao {

	public static final int STATUS_SUCCESS_CODE = 200;
	public static final String STATUS_SUCCESS_MESSAGE = "success";
	public static final int STATUS_ERROR_CODE =500;
	public static final String STATUS_ERROR_MESSAGE="error";

	Connection conn = null;
	Statement stmt = null;
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());

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
			return response.toString();

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
			return response.toString();

		}

		return response.toString();

	}



	public String getCatalogs()
	{

		return "";


	}


	public String getProductByID(String productID)
	{
		awsAuthentication();

		JSONObject response = new JSONObject();
		JSONObject product = getProductByIDFromDynamoDB(productID);

		response.put("statusCode",STATUS_SUCCESS_CODE);
		response.put("statusMessage", STATUS_SUCCESS_MESSAGE);
		response.put("catalog",product);
		System.out.println("RESPONSE"+ response.toString());

		return response.toString();

	}

	public JSONObject getProductByIDFromDynamoDB(String productID)
	{

		//	JSONObject product = new JSONObject();


		String tableName = "products";
		HashMap<String, Condition> filter = new HashMap<String, Condition>();

		Condition hashKeyCondition = new Condition().withComparisonOperator(
				ComparisonOperator.EQ.toString()).withAttributeValueList(new AttributeValue().withN(productID));

		filter.put("productID", hashKeyCondition);

		QueryRequest queryRequest = new QueryRequest().withTableName(tableName).withKeyConditions(filter);

		QueryResult result = client.query(queryRequest);
		System.out.println("Query Result:" + result);

		JSONObject p = new JSONObject();

		List<Map<String, AttributeValue>> items = result.getItems();
		for (Map<?, ?> item : items) {
			Set s = item.keySet();  
			Iterator i  = s.iterator(); 

			while(i.hasNext()) {

				String key =  (String) i.next();
				String value = item.get(key).toString();
				String actualValue = (value.substring(3,(value.length())-2));
				p.put(key, actualValue);
			}

		}


		return p;


	}



	public String getProducts(String categoryID)
	{

		JSONObject response = new JSONObject();
		try
		{
			awsAuthentication();
			JSONArray product = new JSONArray();
			product = getProductsfromDynamoDB(categoryID);

			response.put("statusCode",STATUS_SUCCESS_CODE);
			response.put("statusMessage", STATUS_SUCCESS_MESSAGE);
			response.put("dataProducts",product);

			System.out.println("response "+ response.toString());
		}
		catch (AmazonServiceException ase) {
			System.err.println(ase.getMessage());

			response.put("statusCode",STATUS_ERROR_CODE);
			response.put("statusMessage", STATUS_ERROR_MESSAGE);
			return response.toString();

		} 

		return response.toString();

	}

	public void awsAuthentication()
	{

		AWSCredentials credentials = new BasicAWSCredentials("AKIAI3QN42KZIHNSDIFA","do3c/rNdJnDW/rlCCSKzT5NvAyzKnw7qeCPvSSYL");

		client = new AmazonDynamoDBClient(credentials);
		Region usWest2 = Region.getRegion(Regions.US_WEST_1);
		client.setRegion(usWest2);

	}

	public JSONArray getProductsfromDynamoDB(String categoryID)
	{
		String tableName = "products";

		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();

		Condition hashKeyCondition = new Condition().withComparisonOperator(
				ComparisonOperator.EQ.toString()).withAttributeValueList(new AttributeValue().withS(categoryID));

		scanFilter.put("categoryID", hashKeyCondition);
		ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
		ScanResult scanResult = client.scan(scanRequest);



		List<Map<String, AttributeValue>> items = scanResult.getItems();
		JSONArray array = new JSONArray();
		for (Map<?, ?> item : items) {
			Set s = item.keySet();  
			Iterator i  = s.iterator(); 
			JSONObject p = new JSONObject();

			while(i.hasNext()) {

				String key =  (String) i.next();
				String value = item.get(key).toString();
				String actualValue = (value.substring(3,(value.length())-2));
				p.put(key, actualValue);
			}
			array.put(p);
		}
		return array;

	}


	public String getCatalog()
	{

		JSONObject response = new JSONObject();
		try{
			awsAuthentication();
			JSONArray product = new JSONArray();
			product = getCatalogfromDynamoDB();

			response.put("statusCode",STATUS_SUCCESS_CODE);
			response.put("statusMessage", STATUS_SUCCESS_MESSAGE);
			response.put("catalog",product);

			System.out.println("response "+ response.toString());
		}
		catch (AmazonServiceException ase) {
			System.err.println(ase.getMessage());

			response.put("statusCode",STATUS_ERROR_CODE);
			response.put("statusMessage", STATUS_ERROR_MESSAGE);
			return response.toString();

		} 

		return response.toString();

	}

	public JSONArray getCatalogfromDynamoDB()
	{
		String tableName = "catalog";

		ScanRequest scanRequest = new ScanRequest(tableName);
		ScanResult scanResult = client.scan(scanRequest);


		List<Map<String, AttributeValue>> items = scanResult.getItems();
		int n=1;
		JSONArray array = new JSONArray();
		for (Map<?, ?> item : items) {
			Set s = item.keySet();  
			Iterator i  = s.iterator(); 
			JSONObject p = new JSONObject();

			while(i.hasNext()) {

				String key =  (String) i.next();
				String value = item.get(key).toString();
				String actualValue = (value.substring(3,(value.length())-2));
				p.put(key, actualValue);
			}
			array.put(p);
		}
		return array;

	}


	public String getCategory(String catalogID)
	{

		JSONObject response = new JSONObject();

		try{
			awsAuthentication();
			JSONArray product = new JSONArray();
			product = getCategoryfromDynamoDB(catalogID);

			response.put("statusCode",STATUS_SUCCESS_CODE);
			response.put("statusMessage", STATUS_SUCCESS_MESSAGE);
			response.put("category",product);

			System.out.println("response "+ response.toString());
		} catch (AmazonServiceException ase) {
			System.err.println(ase.getMessage());

			response.put("statusCode",STATUS_ERROR_CODE);
			response.put("statusMessage", STATUS_ERROR_MESSAGE + ase.getMessage() );
			//System.out.println("RESPONSE--ERROR"+ response.toString());
			return response.toString();

		} 

		return response.toString();

	}

	private JSONArray getCategoryfromDynamoDB(String catalogID) {
		String tableName = "category";

		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();

		Condition hashKeyCondition = new Condition().withComparisonOperator(
				ComparisonOperator.EQ.toString()).withAttributeValueList(new AttributeValue().withS(catalogID));

		scanFilter.put("catalogID", hashKeyCondition);
		ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
		ScanResult scanResult = client.scan(scanRequest);

		List<Map<String, AttributeValue>> items = scanResult.getItems();
		JSONArray array = new JSONArray();
		for (Map<?, ?> item : items) {
			Set s = item.keySet();  
			Iterator i  = s.iterator(); 
			JSONObject p = new JSONObject();

			while(i.hasNext()) {

				String key =  (String) i.next();
				String value = item.get(key).toString();
				String actualValue = (value.substring(3,(value.length())-2));
				p.put(key, actualValue);
			}
			array.put(p);
		}
		return array;

	}



	public String addToCart(String email, String productID, String quantity)
	{
		awsAuthentication();

		String tableName = "cart";
		Map<String, AttributeValue> cart = new HashMap<String, AttributeValue>();

		String timestamp= getCurrentDateTime();

		PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(cart);
		cart.put("emailID", new AttributeValue().withS(email));
		cart.put("productID", new AttributeValue().withS(productID));
		cart.put("timestamp", new AttributeValue().withS(timestamp)); 
		cart.put("quantity", new AttributeValue().withN(quantity));

		client.putItem(itemRequest);
		cart.clear();
		System.out.println("Item inserted into DB - cart table");
		return "";


	}

	public String removeFromCart(String emailID,String productID)
	{
		awsAuthentication();

		String tableName = "cart";
		JSONObject response = new JSONObject();

		  Map<String, AttributeValue> attributeList = new HashMap<String, AttributeValue>();
		  attributeList.put("emailID", new AttributeValue().withS(emailID));
		  attributeList.put("productID", new AttributeValue().withS(productID));
		  
		        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
		            String attributeName = item.getKey();
		            AttributeValue value = item.getValue();
		            System.out.println("AttributeName"+ attributeName+"\t value "+value);
		        }
		        

		DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
		.withTableName(tableName).withKey(attributeList);

		DeleteItemResult deleteItemResult = client.deleteItem(deleteItemRequest);
		System.out.println("DELETE ITEM RESULT"+ deleteItemResult);


		response.put("statusCode",STATUS_SUCCESS_CODE);
		response.put("statusMessage", STATUS_SUCCESS_MESSAGE);

		return response.toString();
	}

	public String updateCart(String emailID, String productID,String quantity )
	{
		awsAuthentication();
		String tableName = "cart";
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();
		String timestamp= getCurrentDateTime();
		

		  Map<String, AttributeValue> attributeList = new HashMap<String, AttributeValue>();
		  attributeList.put("emailID", new AttributeValue().withS(emailID));
		  attributeList.put("productID", new AttributeValue().withS(productID));

/*		Map<String, Condition> keyConditions = new HashMap<String, Condition>();

		Condition hashKeyCondition = new Condition()
		.withAttributeValueList(new AttributeValue().withS(emailID));
		keyConditions.put("emailID", hashKeyCondition);

		Condition rangeKeyCondition = new Condition()
		.withAttributeValueList(new AttributeValue().withS(productID));
		keyConditions.put("productID", rangeKeyCondition);*/

/*
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("emailID", new AttributeValue().withN(emailID));*/
		/*
	*/	updateItems.put("emailID", new AttributeValueUpdate().withAction(AttributeAction.ADD)
			.withValue(new AttributeValue().withS(emailID)));

		updateItems.put("productID", new AttributeValueUpdate().withAction(AttributeAction.ADD)
				.withValue(new AttributeValue().withS(productID)));


		updateItems.put("quantity", new AttributeValueUpdate().withAction(AttributeAction.ADD)
				.withValue(new AttributeValue().withS(quantity)));


		updateItems.put("timestamp", new AttributeValueUpdate().withAction(AttributeAction.ADD)
				.withValue(new AttributeValue().withS(timestamp)));

		/*// Add two new authors to the list.
		updateItems.put("Authors", 
		  new AttributeValueUpdate()
		    .withAction(AttributeAction.ADD)
		    .withValue(new AttributeValue().withSS("AuthorYY", "AuthorZZ")));*/
		/*
		// Reduce the price. To add or subtract a value,
		// use ADD with a positive or negative number.
		updateItems.put("Price", 
		  new AttributeValueUpdate()
		    .withAction(AttributeAction.ADD)
		    .withValue(new AttributeValue().withN("-1")));

		// Delete the ISBN attribute.
		updateItems.put("ISBN", 
		  new AttributeValueUpdate()
		    .withAction(AttributeAction.DELETE));*/

		UpdateItemRequest updateItemRequest = new UpdateItemRequest()
		.withTableName(tableName)
		.withKey(attributeList)
		.withReturnValues(ReturnValue.UPDATED_NEW)
		.withAttributeUpdates(updateItems);

		
		
		UpdateItemResult result = client.updateItem(updateItemRequest);
		System.out.println("Result of update operation : " +result);

		return"";

	}
	
	

	public static void main(String[] args)
	{
		User u = new User();
		u.setEmail("pooja@gmail.com");
		u.setPasswd("1234");

		//	new UserDao().checkUser(u);

		//new UserDao().getProducts("100");
		// new UserDao().getCategory("1001");
		//	
	//	new UserDao().addToCart("poornima@gmail.com", "003","1");
	//	new UserDao().addToCart("pooja@gmail.com", "002", "2");
		// new UserDao().addToCart("poornima@gmail.com", "003","3");
		//new UserDao().getProductByID("001");
		// new UserDao().removeFromCart("pooja@gmail.com", "002");
	//	new UserDao().updateCart("pooja@gmail.com", "001", "3");
	//	new UserDao().updateCart("poornima@gmail.com", "002", "2");

		new UserDao().updateCart("poorni@gmail.com", "002","2");


	}

}
