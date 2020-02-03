package util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.Map;

/**
 * Project: kstreamsdemo
 * Contributed By: Tushar Mudgal
 * On: 29/01/20 | 11:29 PM
 */
@Log4j
public class CustomSerializer<T> implements Serializer<T> {
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String topic, T t) {
        try {
            return mapper.writeValueAsBytes(t);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            log.error("Exception occured while serializing message :: " +
                    t + " with exception " + Arrays.toString(e.getStackTrace()));
        }
        return null;
    }

    @Override
    public void close() {
    }
}
