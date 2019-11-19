package connection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@SpringBootApplication
public class Application {

//    @Autowired
//    private AcceptRequestController acceptRequestController;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

//    @Override
//    public void run(String... args) throws Exception {
//        acceptRequestController
//    }
}
