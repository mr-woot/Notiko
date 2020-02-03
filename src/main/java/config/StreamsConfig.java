package config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import util.ConfigBundle;

import java.util.Properties;

/**
 * Project: kstreamsdemo
 * Contributed By: Tushar Mudgal
 * On: 29/01/20 | 5:00 PM
 */
public class StreamsConfig {
    public static Properties getStreamProps() {
        Properties props = new Properties();
        props.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigBundle.getValue("BOOTSTRAP_SERVERS_CONFIG"));
        props.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG,
                ConfigBundle.getValue("APPLICATION_ID_CONFIG"));
        props.put(org.apache.kafka.streams.StreamsConfig.CLIENT_ID_CONFIG,
                ConfigBundle.getValue("CLIENT_ID_CONFIG"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
//                Serdes.String().getClass());
//        props.put(org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
//                Serdes.String().getClass());
        props.put(org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE);
        // ## disable cache to demonstrate all the steps involved in the transformation
        // - not recommended in production environment
        props.put(org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return props;
    }
}
