package io.github.oscarleetech.tapewatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TapewatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(TapewatchApplication.class, args);
    }

}
