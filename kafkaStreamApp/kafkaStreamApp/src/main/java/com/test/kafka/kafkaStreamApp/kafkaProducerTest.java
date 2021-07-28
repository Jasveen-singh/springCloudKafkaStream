package com.test.kafka.kafkaStreamApp;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class kafkaProducerTest {
	private static String TOPIC = "";
	private static String BOOTSTRAP_SERVERS = "";
	public static int count = 0;

	private static Producer<Long, JSONObject> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
		//props.setProperty(ProducerConfig.,grp_id); 
		return new KafkaProducer<>(props);
	}

	static void runProducer(final int sendMessageCount) throws Exception {
		final Producer<Long, JSONObject> producer = createProducer();

		try {
			JSONObject kafkaPushDataSMS = createMsg();
			for(int i=0; i<1; i++) 
			{
				if(kafkaPushDataSMS.has("specific field") ) {}
				else  {
					ProducerRecord<Long, JSONObject> record = new ProducerRecord<>(TOPIC, 
							kafkaPushDataSMS);

					RecordMetadata metadata = producer.send(record).get();
					count++;
					System.out.println(count);
				}

			}

		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static void main(String... args) throws Exception {
		loadKafkaTopicJson();
		for (int i = 0; i < 1; i++) {
			runProducer(1);
		}
	}

	@SuppressWarnings("deprecation")
	static JSONObject createMsg() throws JSONException {
		//load msg from input.json

		String kafkaPushData = null;
		JsonObject kafkaPushDataJson = new JsonObject();
		JSONObject kafkaPushDataJSONObj = null;
		try {
			Gson gson = new Gson();
			Object obj =  gson.fromJson(new FileReader("input.json"),Object.class);
			kafkaPushData = gson.toJson(obj,Object.class);
			kafkaPushDataJson = JsonParser.parseString(kafkaPushData).getAsJsonObject();
			System.out.println(kafkaPushData.toString());
			kafkaPushDataJSONObj = new JSONObject(kafkaPushDataJson.toString());

		} catch (Exception e) {
			e.printStackTrace();
		}
		return kafkaPushDataJSONObj;
	}

	public static void loadKafkaTopicJson() {
		Map<String, String> kafkaConfig = new HashMap<String, String>();
		ObjectMapper mapper = new ObjectMapper();
		TypeReference<Map<String, String>> typeRef = new TypeReference<Map<String, String>>() {
		};
		InputStream inputStream ;
		try  {
			inputStream = new FileInputStream("kafka_config.json");
			kafkaConfig = mapper.readValue(inputStream, typeRef);
			BOOTSTRAP_SERVERS= kafkaConfig.get("kafka_dns");
			TOPIC=kafkaConfig.get("kafka_topic");
			System.out.println("##############%%%%% TOPIC IS ::"+TOPIC);
			inputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
