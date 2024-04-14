package telran.probes;



import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import telran.probes.dto.DeviationData;

@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class AwsSnsNotifierAppl {
	@Value("${arn_address}")
	String arnAddress;
	@Value("${topicName}")
	String topicName;
	@Value("${subjectName}")
	String subjectName;
	@Value("${regiontName}")
	String region;
	public static void main(String[] args) {
		SpringApplication.run(AwsSnsNotifierAppl.class, args);		
	}
	@Bean
	Consumer<DeviationData> emailNotifierConsumer() {
		return this::sendingSns;
	}
	void sendingSns(DeviationData deviation) {
		log.debug("received deviation data: {}",deviation);
		long sensorId = deviation.id();
		AmazonSNS client = AmazonSNSClient.builder().withRegion(region)
				.build();
		String topicArnString = (arnAddress + ":" + topicName);
		String message = String.format("Deviation of sensor:%d is %f, of value: %f, timestamp %s", sensorId, deviation.deviation(), deviation.value(), deviation.timestamp() + "");
		log.debug("Config arnAddr: {}, message: {}, subj: {}", topicArnString, message, topicName);
		client.publish(topicArnString, message, subjectName);
		log.debug("send deviation data: {}", message);
        AmazonDynamoDB clientAmazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withRegion(region)
            .build();	
		DynamoDB dynamoDB = new DynamoDB(clientAmazonDynamoDB);
		Table table = dynamoDB.getTable(topicName);
		Item item = new Item()
				.withPrimaryKey("id",sensorId)
				.withDouble("deviation", deviation.deviation())
				.withDouble("value", deviation.value())
				.withLong("timestamp", deviation.timestamp());
		table.putItem(item);
	}	
	
	
}