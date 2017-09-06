package client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

/**
 * Created by immortalCockroach on 8/28/17.
 */

@SpringBootApplication
@ComponentScan("client")
@ImportResource({"classpath:applicationContext.xml"})
public class ClientStartUp extends SpringBootServletInitializer {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(ClientStartUp.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(ClientStartUp.class);
    }

}
