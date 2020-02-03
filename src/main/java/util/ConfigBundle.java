package util;

import java.util.ResourceBundle;

/**
 * Project: kafkastreams
 * Contributed By: Tushar Mudgal
 * On: 27/01/20 | 3:20 PM
 */
public class ConfigBundle {
    private static ResourceBundle resourceBundle;

    private static ResourceBundle getInstance() {
        if (ConfigBundle.resourceBundle == null) {
            ConfigBundle.resourceBundle = ResourceBundle.getBundle("config");
        }
        return ConfigBundle.resourceBundle;
    }

    public static String getValue(final String key) {
        return getInstance().getString(key);
    }
}