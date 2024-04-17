package telran.probes.service;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.amazonaws.regions.Regions;

import ch.qos.logback.core.joran.conditional.IfAction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsResponse;
import telran.probes.dto.Range;
import telran.probes.dto.SensorUpdateData;

@Service
@RequiredArgsConstructor
@Slf4j
public class RangeProviderClientServiceImpl implements RangeProviderClientService {
final RestTemplate restTemplate;
final ServiceConfiguration serviceConfiguration;
HashMap<Long, Range> sensorData = new HashMap<Long, Range>();
@Value("${logGroupName}")
String logGroupName;
@Value("${logStreamName}")
String logStreamName;
@Value("${regiontName}")
String regionName;
	@Override
	public Range getRange(long sensorId) {
		Range range = null;
		if (!sensorData.containsKey(sensorId)) {
			range = serviceRequest(sensorId);
			if (range != null) {
				if (!range.equals(new Range(MIN_DEFAULT_VALUE, MAX_DEFAULT_VALUE))) {
				sensorData.put(sensorId, range);
			}			
		} 
		}
		else {
			range = sensorData.get(sensorId);
		}
			
			
		return range;
	}
	
	private Range serviceRequest(long sensorId) {
		Range range = null;
		ResponseEntity<?> responseEntity ;
		try {
			responseEntity = restTemplate.exchange(getUrl(sensorId), HttpMethod.GET, null, Range.class);
			if (responseEntity.getStatusCode().isError()) {
				throw new Exception(responseEntity.getBody().toString());
			}
			range = (Range) responseEntity.getBody();
			log.debug("range value {}", range);
		} catch (Exception e) {
			log.error("error at service request: {}", e.getMessage());
			sendCloudWatchMetric(e.getMessage());
			log.warn("throw error to AWS CloudWatch {}");
			
		} 
		return range;
		
	}
	private void sendCloudWatchMetric(String errorMessage) {
		  	// Create client Amazon CloudWatch
		Region region = Region.of(regionName);
        	// Create metrics with error message
		CloudWatchLogsClient logsClient = CloudWatchLogsClient.builder()
	            .region(region)
	            .build();
        MetricDatum metricDatum = MetricDatum.builder()
                .metricName("ErrorCount")
                .dimensions(Dimension.builder().name("ErrorType").value(errorMessage).build())
                .unit(StandardUnit.NONE)
                .value(1.0) // 1 error
                .build();
        String message = metricDatum.toString(); 
        putLogEvent(logsClient, logGroupName, logStreamName, message);
        // Create request to send message to CloudWatch
    }
		
	

	private String getUrl(long sensorId) {
		String url = String.format("http://%s:%d%s/%d",
				serviceConfiguration.getHost(),
				serviceConfiguration.getPort(),
				serviceConfiguration.getPath(),
				sensorId);
		log.debug("url created is {}", url);
		return url;
	}
	
	private static void putLogEvent(CloudWatchLogsClient logsClient, String logGroupName, String logStreamName, String message) {
        long timestamp = Instant.now().toEpochMilli();
        InputLogEvent logEvent = InputLogEvent.builder()
            .timestamp(timestamp)
            .message(message)
            .build();
        PutLogEventsRequest request = PutLogEventsRequest.builder()
            .logGroupName(logGroupName)
            .logStreamName(logStreamName)
            .logEvents(Collections.singletonList(logEvent))
            .build();
        PutLogEventsResponse response = logsClient.putLogEvents(request);
        log.warn("Response: {}", response);
    }

	
	@Bean
	Consumer<SensorUpdateData> updateRangeConsumer() {
		return this::updateProcessing;
	}

	private void updateProcessing(SensorUpdateData updateData) {	
		if(updateData.range()!= null && sensorData.containsKey(updateData.id())) {
			sensorData.put(updateData.id(), updateData.range());
		}
	}
}
