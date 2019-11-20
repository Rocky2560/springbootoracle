package connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

    private Logger log = LoggerFactory.getLogger(AcceptRequestController.class);

    public AcceptRequestController() {
    }

//    @PostMapping("/show")
    @RequestMapping(value = "/json", produces="application/json", consumes="application/json", method = RequestMethod.POST)
//    @Autowired
    public Map<String, Object> db_fetch(@RequestBody RequestData requestData) {
//        System.out.println(requestData.getUsername());
//        System.out.println(requestData.getPassword());
//        System.out.println(requestData.getOffset());
        int offset_value = requestData.getOffset();
        System.out.println(offset_value);
        int result_offset;
        int initial_offset = 5;
        String table_name = "movies";
        int range_count = 1000;
        Connection conn;
//        JSONObject jo = new JSONObject();
        Map<String, Object> jo = new HashMap<>();

        try {
//                System.out.println(db_url);
                conn = DriverManager.getConnection(db_url, db_name, db_pwd);

                if (conn != null) {
//                    log.info("Connected!!!");
                    System.out.println("Connected to the database!");
//                    String crow_query= "select count(*) from movies";

                    String fetch_query = "select * from "+ table_name +" offset "+ offset_value +" rows fetch next "+ range_count +" rows only";

                    Statement stmt=conn.createStatement();
                    ResultSet rs=stmt.executeQuery(fetch_query);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int num_col = rsmd.getColumnCount();

                    jo.put("table-name",table_name);
                    jo.put("count",range_count);
                    ArrayList<Map<String, Object>> ja = new ArrayList<>();
                    while(rs.next()) {
                        Map<String, Object> jo2 = new HashMap<>();
                        for (int i = 1; i <= num_col; i++) {

                            Map<String, Object> m = new HashMap<>();
                            m.put("value", rs.getObject(i));
                            m.put("type", rsmd.getColumnTypeName(i));

                            jo2.put(rsmd.getColumnName(i), m);
                        }

                        ja.add(jo2);
                    }
                    jo.put("columns", ja);

                    result_offset = offset_value + 1000;
                    jo.put("offset",result_offset);
                    conn.close();
                } else {
                    System.out.println("Failed to make connection!");
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        System.out.println(jo);
        return jo;
    }
}

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