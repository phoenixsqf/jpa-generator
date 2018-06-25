package util;

import entity.ConfigProperties;
import lombok.Getter;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigUtil {

    private static ConfigProperties configProperties = new ConfigProperties();

    private static ConfigProperties readProperties() throws Exception {
        Properties properties = new Properties();
        InputStream in = ConfigUtil.class.getResourceAsStream("/config.properties");
        properties.load(in);
        configProperties.setDriverClassName(properties.getProperty("driverClassName"));
        configProperties.setSchemaName(properties.getProperty("schemaName"));
        configProperties.setTableName(properties.getProperty("tableName"));
        configProperties.setUrl(properties.getProperty("url"));
        configProperties.setUsername(properties.getProperty("username"));
        configProperties.setPassword(properties.getProperty("password"));
        configProperties.setPackageName(properties.getProperty("packageName"));
        configProperties.setOutput(properties.getProperty("output"));
        return configProperties;
    }

    public static ConfigProperties init() throws Exception {
        return readProperties();
    }
}
