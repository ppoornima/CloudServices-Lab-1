package edu.sjsu.cmpe282.api.resources;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

public class GettingStartedLoadTables {
	static AmazonDynamoDBClient client;
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static String productCatalogTableName = "product";
   
    
    public static void main(String[] args) throws Exception {
        createClient();

        try {

            uploadSampleProducts(productCatalogTableName);
            

        } catch (AmazonServiceException ase) {
            System.err.println("Data load script failed.");
        }
    }
    
    private static void createClient() throws Exception {
//        AWSCredentials credentials = new PropertiesCredentials(
//                GettingStartedLoadTables.class.getResourceAsStream("AwsCredentials.properties"));
    	// client = new AmazonDynamoDBClient(credentials);
    	
        AWSCredentials credentials = new BasicAWSCredentials("AKIAI3QN42KZIHNSDIFA","do3c/rNdJnDW/rlCCSKzT5NvAyzKnw7qeCPvSSYL");

        client = new AmazonDynamoDBClient(credentials);
        client.setRegion(Region.getRegion(Regions.US_WEST_1));      
        
       
    }

    private static void uploadSampleProducts(String tableName) {
        
        try {
            // Add books.
            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            item.put("ProductID", new AttributeValue().withN("101"));
            item.put("Title", new AttributeValue().withS("Book 101 Title"));
            item.put("ISBN", new AttributeValue().withS("111-1111111111"));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1")));
            item.put("Price", new AttributeValue().withN("2"));
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 0.5"));
            item.put("PageCount", new AttributeValue().withN("500"));
            item.put("InPublication", new AttributeValue().withN("1"));
            item.put("ProductCategory", new AttributeValue().withS("Book"));
            
            PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();
            
            item.put("ProductID", new AttributeValue().withN("102"));
            item.put("Title", new AttributeValue().withS("Book 102 Title"));
            item.put("ISBN", new AttributeValue().withS("222-2222222222"));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2")));
            item.put("Price", new AttributeValue().withN("20"));
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 0.8"));
            item.put("PageCount", new AttributeValue().withN("600"));
            item.put("InPublication", new AttributeValue().withN("1"));
            item.put("ProductCategory", new AttributeValue().withS("Book"));
            
            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();
            
            item.put("ProductID", new AttributeValue().withN("103"));
            item.put("Title", new AttributeValue().withS("Book 103 Title"));
            item.put("ISBN", new AttributeValue().withS("333-3333333333"));
            item.put("Authors", new AttributeValue().withSS(Arrays.asList("Author1", "Author2")));
            // Intentional. Later we run scan to find price error. Find items > 1000 in price.            
            item.put("Price", new AttributeValue().withN("2000")); 
            item.put("Dimensions", new AttributeValue().withS("8.5 x 11.0 x 1.5"));
            item.put("PageCount", new AttributeValue().withN("600"));
            item.put("InPublication", new AttributeValue().withN("0"));
            item.put("ProductCategory", new AttributeValue().withS("Book"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            // Add bikes.
            item.put("ProductID", new AttributeValue().withN("201"));
            item.put("Title", new AttributeValue().withS("18-Bike-201")); // Size, followed by some title.
            item.put("Description", new AttributeValue().withS("201 Description"));
            item.put("BicycleType", new AttributeValue().withS("Road"));
            item.put("Brand", new AttributeValue().withS("Mountain A")); // Trek, Specialized.
            item.put("Price", new AttributeValue().withN("100"));
            item.put("Gender", new AttributeValue().withS("M")); // Men's
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("ProductID", new AttributeValue().withN("202"));
            item.put("Title", new AttributeValue().withS("21-Bike-202")); 
            item.put("Description", new AttributeValue().withS("202 Description"));
            item.put("BicycleType", new AttributeValue().withS("Road"));
            item.put("Brand", new AttributeValue().withS("Brand-Company A"));
            item.put("Price", new AttributeValue().withN("200"));
            item.put("Gender", new AttributeValue().withS("M"));
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Green", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));
            
            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("ProductID", new AttributeValue().withN("203"));
            item.put("Title", new AttributeValue().withS("19-Bike-203")); 
            item.put("Description", new AttributeValue().withS("203 Description"));
            item.put("BicycleType", new AttributeValue().withS("Road"));
            item.put("Brand", new AttributeValue().withS("Brand-Company B"));
            item.put("Price", new AttributeValue().withN("300"));
            item.put("Gender", new AttributeValue().withS("W")); // Women's
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red", "Green", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("ProductID", new AttributeValue().withN("204"));
            item.put("Title", new AttributeValue().withS("18-Bike-204")); 
            item.put("Description", new AttributeValue().withS("204 Description"));
            item.put("BicycleType", new AttributeValue().withS("Mountain"));
            item.put("Brand", new AttributeValue().withS("Brand-Company B"));
            item.put("Price", new AttributeValue().withN("400"));
            item.put("Gender", new AttributeValue().withS("W"));
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);
            item.clear();

            item.put("ProductID", new AttributeValue().withN("205"));
            item.put("Title", new AttributeValue().withS("20-Bike-205")); 
            item.put("Description", new AttributeValue().withS("205 Description"));
            item.put("BicycleType", new AttributeValue().withS("Hybrid"));
            item.put("Brand", new AttributeValue().withS("Brand-Company C"));
            item.put("Price", new AttributeValue().withN("500"));
            item.put("Gender", new AttributeValue().withS("B")); // Boy's
            item.put("Color", new AttributeValue().withSS(Arrays.asList("Red", "Black")));
            item.put("ProductCategory", new AttributeValue().withS("Bicycle"));
            
            itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
            client.putItem(itemRequest);

                
        }   catch (AmazonServiceException ase) {
            System.err.println("Failed to create item in " + tableName + " " + ase);
        } 

    }


}
