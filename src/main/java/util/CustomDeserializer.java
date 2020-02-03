package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Project: kstreamsdemo
 * Contributed By: Tushar Mudgal
 * On: 29/01/20 | 11:28 PM
 */
@Log4j
public class CustomDeserializer<T> implements Deserializer<T> {

    private ObjectMapper mapper = new ObjectMapper();
    private Class<T> deserializedClass;

    public CustomDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    public CustomDeserializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> map, boolean b) {
        if (deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return mapper.readValue(bytes, deserializedClass);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Exception || CORRUPT MESSAGE while deserializing:: " + new String(bytes) + " with exception "
                    + Arrays.toString(e.getStackTrace()));
        }
        return null;
    }

    @Override
    public void close() {

    }
}

