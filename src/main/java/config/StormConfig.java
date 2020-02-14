package config;

import java.util.Properties;

public class StormConfig {

    public static final String CONFIG_NAME = "config";
    private static StormConfig stormConfig = null;
    private Properties configFile;

    private StormConfig() {
        configFile = new java.util.Properties();
        try {
            configFile.load(this.getClass().getClassLoader().
                    getResourceAsStream(CONFIG_NAME + ".cfg"));
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
        return this.configFile.getProperty(key);
    }


}
