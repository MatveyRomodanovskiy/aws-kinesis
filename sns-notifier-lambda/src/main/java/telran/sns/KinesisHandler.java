package telran.sns;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;



public class KinesisHandler implements RequestHandler<KinesisEvent, String> {
	private static final JSONParser parser = new JSONParser();

	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		var logger = context.getLogger();
		input.getRecords().stream().map(
				r -> new String(r.getKinesis().getData().array()))
			.forEach(string -> sendSns(string, logger));
		return null;
	}

	private void sendSns(String data, LambdaLogger logger) {
			/*
			* Build environment vars
			*/
			String topicArnString = System.getenv("ARN_KEY") + ":" + System.getenv("TOPIC_NAME");
			String idName = System.getenv("ID_KEY");
			String deviationName = System.getenv("DEVIATION_NAME");
			String valueName = System.getenv("VALUE_NAME");
			String timeStampName = System.getenv("TIMESTAMP_NAME");
			String mainSubjectName = System.getenv("SUBJECT_NAME");
			
			AmazonSNS client = AmazonSNSClient.builder().withRegion(Regions.US_EAST_1)
					.build();
		try {
			int startIndex = data.indexOf("{");
	        if (startIndex != -1) {
	            data = data.substring(startIndex);
	        }
			JSONObject jsonObject = (JSONObject) parser.parse(data);
			long sensorId = (Long) jsonObject.get(idName);
			double deviation = (Double) jsonObject.get(deviationName);
			double value = (Double) jsonObject.get(valueName);
			long timestamp = (Long) jsonObject.get(timeStampName);
			String subject = mainSubjectName + " " +  sensorId;
			String message = String.format("Deviation of sensor:%d is %f, of value: %f, timestamp %s", sensorId, deviation, value, timestamp + "");
//			client.publish(topicArnString, message, subject);
			logger.log("send deviation data:" + message);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			}
	}


