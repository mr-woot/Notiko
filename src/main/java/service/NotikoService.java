package service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import config.StreamsConfig;
import domain.Notiko;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import util.ConfigBundle;
import util.CustomDeserializer;
import util.CustomSerializer;
import util.GSON;

import java.util.Properties;

/**
 * Project: kstreamsdemo
 * Contributed By: Tushar Mudgal
 * On: 29/01/20 | 4:59 PM
 */
public class NotikoService {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static Gson gson = GSON.getGson();

    public void run() {
        Properties streamsProps = StreamsConfig.getStreamProps();
        // custom json serdes
        final Serde<String> stringSerde = Serdes.String();
        final CustomSerializer<JsonNode> jsonSerializer = new CustomSerializer<>();
        final CustomDeserializer<JsonNode> jsonDeserializer = new CustomDeserializer<>(JsonNode.class);
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final CustomSerializer<Notiko> notikoSerializer = new CustomSerializer<>();
        final CustomDeserializer<Notiko> notikoDeserializer = new CustomDeserializer<>(Notiko.class);
        final Serde<Notiko> notikoSerde = Serdes.serdeFrom(notikoSerializer, notikoDeserializer);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> messagePayload = builder
                .stream(ConfigBundle.getValue("KSTREAM_TOPIC_INSERT_TYPE"),
                        Consumed.with(stringSerde, jsonSerde));

        // ## create new payload
        KStream<String, Notiko> fMap = messagePayload
                .mapValues(value -> {
                    Notiko p = null;
                    try {
                         p = Notiko.createNotikoJsonNode(value);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    System.out.println(gson.toJson(p));
                    return p;
                });

        KTable<String, Notiko> table = fMap
                .selectKey((key, value) -> value.getMobileNumber() + "+" + value.getSiteId())
                // very important to mention custom serde in each and every step
                .groupByKey(Grouped.with(stringSerde, notikoSerde))
                // here also very important to mention custom serde in each and every step
                // materialized with method parses using custom serde
                .reduce((value1, value2) -> value2, Materialized.with(stringSerde, notikoSerde));

        table
                // convert table to stream
                .toStream()
                // and finally sends the records to topic with *correct serdes*
                .to(ConfigBundle.getValue("KTABLE_TOPIC_UPDATE_TYPE"),
                        Produced.with(stringSerde, notikoSerde));

        // finish topology
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
