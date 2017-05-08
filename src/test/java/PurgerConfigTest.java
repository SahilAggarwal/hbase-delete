import config.PurgerConfig;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class PurgerConfigTest {
    @Test(expected = FileNotFoundException.class)
    public void shouldBombNoConfigFile() throws IOException {
        PurgerConfig purgerConfig = new PurgerConfig("foo.properties");
        purgerConfig.init();
    }
}
