package telran.aws;


import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

public class KinesisHandler implements RequestHandler<KinesisEvent, String> {
	private static final JSONParser parser = new JSONParser();
	
	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		LambdaLogger logger = context.getLogger();
		Map<String, String> enviromentVarsMap = new HashMap<>();
		enviromentVarsMap.put("ID_KEY", System.getenv("ID_KEY"));
		enviromentVarsMap.put("VALUE_KEY", System.getenv("VALUE_KEY"));
		enviromentVarsMap.put("TIMESTAMP_KEY", System.getenv("TIMESTAMP_KEY"));
		enviromentVarsMap.put("TABLE_NAME",System.getenv("TABLE_NAME"));
		input.getRecords().stream().map(
				r -> new String(r.getKinesis().getData().array()))
			.forEach(data -> consumeAndRecord(data, logger, enviromentVarsMap));
		return null;
	}

	private void consumeAndRecord(String data, LambdaLogger logger, Map<String, String> enviromentVarsMap) {
		try {
			int startIndex = data.indexOf("{");
	        if (startIndex != -1) {
	            data = data.substring(startIndex);
	        }
			JSONObject jsonObject = (JSONObject) parser.parse(data);
			long idString = (Long) jsonObject.get(enviromentVarsMap.get("ID_KEY"));
			double valueString = (Double) jsonObject.get(enviromentVarsMap.get("VALUE_KEY"));
			long timeString = (Long) jsonObject.get(enviromentVarsMap.get("TIMESTAMP_KEY"));
			AmazonDynamoDB clientAmazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
		            .withRegion(Regions.US_EAST_1)
		            .build();
			DynamoDB dynamoDB = new DynamoDB(clientAmazonDynamoDB);
			Table table = dynamoDB.getTable(enviromentVarsMap.get("TABLE_NAME"));
			Item item = new Item()
					.withPrimaryKey(enviromentVarsMap.get("ID_KEY"), idString)
					.withDouble(enviromentVarsMap.get("VALUE_KEY"), valueString)
					.withLong(enviromentVarsMap.get("TIMESTAMP_KEY"), timeString);
			table.putItem(item);
			logger.log("id average value" + data + " time " + timeString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
