package util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Project: kafkastreams
 * Contributed By: Tushar Mudgal
 * On: 27/01/20 | 3:25 PM
 */
public class GSON {
    private static Gson gson;

    public static Gson getGson() {
        if (gson == null) {
            return new GsonBuilder().serializeNulls().create();
        }
        return gson;
    }
}
