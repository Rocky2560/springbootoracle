package connection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

@RestController

/*
 location of external configuration file. Uncomment to use
@PropertySource("")
  */

public class AcceptRequestController {

    @Value("${spring.datasource.url}")
    private String db_url;
    @Value("${spring.datasource.username}")
    private String db_name;
//    @Value("${}")
    @Value("${spring.datasource.password}")
    private String db_pwd;

    public AcceptRequestController() {
    }

    @RequestMapping("/show")
    public void test() {

        Connection conn;
            try {
//                System.out.println(db_url);

                conn = DriverManager.getConnection(db_url, db_name, db_pwd);

                if (conn != null) {
                    System.out.println("Connected to the database!");
//                    Statement stmt=conn.createStatement();
//                    ResultSet rs=stmt.executeQuery("select * from customers");
//
//                    while(rs.next())
//                        System.out.println("id: " + rs.getString(1) + "\t name: " + rs.getString(2) + "\t city: " + rs.getString(3));
//                    conn.close();
                } else {
                    System.out.println("Failed to make connection!");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

}


//    @Autowired
//    private MyRepo repository;
//
//    @Autowired
//    private List<DailyTask> dailyt;
//
//    public AcceptRequestController(MyRepo repository,
//        List<DailyTask> dailyt){
//        super();
//        this.repository = repository;
//        this.dailyt = dailyt;
//    }
//
//    @GetMapping("/dailytask")
//    public Iterable<DailyTask> getAllDailyTask(){
//        return repository.findAll();
//    }

//    private static final Logger logger = Logger.getLogger(AcceptRequestController.class);

/*
//Auto wiring Environment variable. Uncomment to use
//    @Autowired
//    private Environment env;
// */
//
//    /*
//    These lines provide memcache access. If you want to setup memcache server for caching
//    pull these lines out of comment box.
//
//        private MemcachedClientBuilder builder = new XMemcachedClientBuilder("127.0.0.1:11211");
//        private MemcachedClient mc = builder.build();
//
//        * And add following entry to pom.xml file
//        *
//        * <dependency>
//            <groupId>com.googlecode.xmemcached</groupId>
//            <artifactId>xmemcached</artifactId>
//            <version>2.4.3</version>
//          </dependency>
//
//     */
//
//
//    public AcceptRequestController() {
//    }
//
//    /**
//     * Returns output from query string.
//     *
//     * @param api_key String api key sent by client
//     * @return output of query string
//     */
//    @PostMapping(path = "/select")
//    public String acceptSelectRequest(
//            HttpServletResponse response,
//            @RequestHeader("key") String api_key
//    ) {
//        return "";
//
//    }

//}