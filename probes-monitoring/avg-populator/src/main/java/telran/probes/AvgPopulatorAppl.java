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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import telran.probes.dto.ProbeData;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class AvgPopulatorAppl {
@Value("${avgTabletName}")
String avgTableName;
@Value("${regiontName}")
String region;
	public static void main(String[] args) {
		SpringApplication.run(AvgPopulatorAppl.class, args);

	}
	@Bean
	Consumer<ProbeData> avgPopulatorConsumer() {
		return this::probeDataPopulation;
	}
	
	void probeDataPopulation(ProbeData probeData) {
		log.debug("received probeData: {}", probeData);
		AmazonDynamoDB clientAmazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
	            .withRegion(region)
	            .build();
		DynamoDB dynamoDB = new DynamoDB(clientAmazonDynamoDB);
		Table table = dynamoDB.getTable(avgTableName);
		Item item = new Item()
				.withPrimaryKey("id", probeData.id())
				.withDouble("value", probeData.value())
				.withLong("timestamp", probeData.timestamp());
		table.putItem(item);
		log.debug("Avg value {} has been saved to Database", probeData);
	}

}
