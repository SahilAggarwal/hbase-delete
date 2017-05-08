import config.PurgerConfig;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import service.PurgerService;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Purger {
    private static Logger log = Logger.getLogger(Purger.class);

    public static void main(String[] args) {
        try {
            PurgerConfig config = new PurgerConfig("config.properties");
            Connection connection = ConnectionFactory.createConnection(config.getHbaseConfig());
            PurgerService service = new PurgerService(connection, config);
            service.start();
        } catch (IOException | ExecutionException | InterruptedException e) {
            log.error(e);
        }
    }
}
