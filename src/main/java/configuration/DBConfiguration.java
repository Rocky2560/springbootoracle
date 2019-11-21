package configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

@Configuration
@PropertySource("file:/home/tchiring/IdeaProjects/spring-boot-oracle/src/main/resources/db_config.properties")
public class DBConfiguration {

    @Autowired
    private Environment env;

    public String getDB() {
        return env.getProperty("db_url");
    }

    public String getUsername() {
        return env.getProperty("db_usr");
    }

    public String getPass() {
        return env.getProperty("db_password");
    }

}
