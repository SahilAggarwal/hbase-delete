package config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;


public class PurgerConfig {

    public static final String RETENTION_KEY = "retention";
    public static String UID_TABLE_KEY = "tsd.storage.hbase.uid_table";
    public static String BATCH_SIZE_KEY = "uid.fetch.batch.size";
    public static String ZK_QUORUM_KEY = "hbase.zookeeper.quorum";
    public static String ZK_NODE_KEY = "zookeeper.znode.parent";
    public static String DATA_TABLE_KEY = "tsd.storage.hbase.data_table";

    private static final int DEFAULT_BATCH_SIZE = 10;

    private String configFile;
    private Properties properties;

    public PurgerConfig(String configFile) throws IOException {
        this.configFile = configFile;
        this.init();
    }

    public void init() throws IOException {
        properties = new Properties();

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configFile);

        if(inputStream != null) {
            properties.load(inputStream);

            if(!properties.containsKey(RETENTION_KEY)) {
                throw new InvalidPropertiesFormatException("Required field " + RETENTION_KEY + " not found");
            }

            if(!properties.containsKey(UID_TABLE_KEY)) {
                throw new InvalidPropertiesFormatException("Required field " + UID_TABLE_KEY + " not found");
            }

            if(!properties.containsKey(ZK_QUORUM_KEY)) {
                throw new InvalidPropertiesFormatException("Required field " + ZK_QUORUM_KEY + " not found");
            }

            if(!properties.containsKey(ZK_NODE_KEY)) {
                throw new InvalidPropertiesFormatException("Required field " + ZK_NODE_KEY + " not found");
            }

            if(!properties.containsKey(DATA_TABLE_KEY)) {
                throw new InvalidPropertiesFormatException("Required field " + DATA_TABLE_KEY + " not found");
            }

            if (!properties.containsKey(BATCH_SIZE_KEY)) {
                properties.setProperty(BATCH_SIZE_KEY, String.valueOf(DEFAULT_BATCH_SIZE));
            }

        } else {
            throw new FileNotFoundException("Properties file " + configFile + " not found");
        }
    }

    public Object getConfig(String key) {
        return properties.get(key);
    }

    public Configuration getHbaseConfig() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.clear();
        configuration.set(ZK_QUORUM_KEY, (String) getConfig(ZK_QUORUM_KEY));
        configuration.set(ZK_NODE_KEY, (String) getConfig(ZK_NODE_KEY));
        return configuration;
    }

}
