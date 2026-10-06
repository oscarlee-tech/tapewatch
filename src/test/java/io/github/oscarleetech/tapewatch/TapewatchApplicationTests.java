package io.github.oscarleetech.tapewatch;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "recorder.prepare-on-startup=false")
class TapewatchApplicationTests {

    @Test
    void contextLoads() {
    }

}
