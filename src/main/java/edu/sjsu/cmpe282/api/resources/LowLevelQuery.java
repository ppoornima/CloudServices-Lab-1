package edu.sjsu.cmpe282.api.resources;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

public class LowLevelQuery {
	
	static AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider());
    static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static void main(String[] args) throws Exception {
        try {
            
            AWSCredentials credentials = new BasicAWSCredentials("AKIAI3QN42KZIHNSDIFA","do3c/rNdJnDW/rlCCSKzT5NvAyzKnw7qeCPvSSYL");

            client = new AmazonDynamoDBClient(credentials);
            Region usWest2 = Region.getRegion(Regions.US_WEST_1);
            client.setRegion(usWest2);
            
        String forumName = "Amazon DynamoDB";
        String threadSubject = "DynamoDB Thread 1";

        // Get an item.
        getBook("205", "product");
        
        // Query replies posted in the past 15 days for a forum thread.
        //findRepliesInLast15DaysWithConfig("Reply", forumName, threadSubject);
        }  
        catch (AmazonServiceException ase) {
            System.err.println(ase.getMessage());
        }  
    }

    
    private static void getBook(String id, String tableName) {
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("ProductID", new AttributeValue().withN(id));
        //com.amazonaws.AmazonServiceException: 1 validation error detected: Value null at 'key' failed to satisfy constraint: Member must not be null (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: ValidationException; Request ID: 4F2UKOOSCAI755IIC8TCFOA9SBVV4KQNSO5AEMVJF66Q9ASUAAJG)
        GetItemRequest getItemRequest = new GetItemRequest()
            .withTableName(tableName)
           .withKey(key)
           .withAttributesToGet(Arrays.asList("ProductID", "Price", "Title"));
        
        GetItemResult result = client.getItem(getItemRequest);

        // Check the response.
        System.out.println("Printing item after retrieving it....");
        printItem(result.getItem());            
    }

  
    private static void printItem(Map<String, AttributeValue> attributeList) {
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            System.out.println(attributeName + " "
                    + (value.getS() == null ? "" : "S=[" + value.getS() + "]")
                    + (value.getN() == null ? "" : "N=[" + value.getN() + "]")
                    + (value.getB() == null ? "" : "B=[" + value.getB() + "] ")
                    + (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
                    + (value.getNS() == null ? "" : "NS=[" + value.getNS() + "]")
                    + (value.getBS() == null ? "" : "BS=[" + value.getBS() + "] \n"));
        }
    }
	    
}
