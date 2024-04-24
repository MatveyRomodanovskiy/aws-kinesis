package telran.aws;
import java.lang.System.Logger;

import javax.lang.model.element.VariableElement;
import javax.swing.text.html.HTMLEditorKit.Parser;

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
		var logger = context.getLogger();
		input.getRecords().stream().map(
				r -> new String(r.getKinesis().getData().array()))
			.forEach(data -> consumeAndRecord(data, logger));
		return null;
	}

	private void consumeAndRecord(String data, LambdaLogger logger) {
		try {
			int startIndex = data.indexOf("{");
	        if (startIndex != -1) {
	            data = data.substring(startIndex);
	        }
			JSONObject jsonObject = (JSONObject) parser.parse(data);
			long idString = (Long) jsonObject.get("id");
			double valueString = (Double) jsonObject.get("value");
			long timeString = (Long) jsonObject.get("timestamp");
			AmazonDynamoDB clientAmazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
		            .withRegion(Regions.US_EAST_1)
		            .build();
			DynamoDB dynamoDB = new DynamoDB(clientAmazonDynamoDB);
			Table table = dynamoDB.getTable("avg-values");
			Item item = new Item()
					.withPrimaryKey("id", idString)
					.withDouble("value", valueString)
					.withLong("timestamp", timeString);
			table.putItem(item);
			logger.log("id average value" + data + " time " + timeString);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
