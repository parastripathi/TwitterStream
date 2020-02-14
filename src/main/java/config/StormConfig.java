package config;

import java.util.Properties;

public class StormConfig {

    private static StormConfig stormConfig = null;
    Properties configFile;

    public StormConfig() {
        configFile = new java.util.Properties();
        try {
            configFile.load(this.getClass().getClassLoader().
                    getResourceAsStream("config.cfg"));
        } catch (Exception eta) {
            eta.printStackTrace();
        }
    }

    public static StormConfig getInstance() {
        if (null == stormConfig) {
            stormConfig = new StormConfig();
        }
        return stormConfig;
    }

    public String getProperty(String key) {
        String value = this.configFile.getProperty(key);
        return value;
    }


}
