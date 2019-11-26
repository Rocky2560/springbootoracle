package bootstrap;

import misc.Queries;
import oracle.jdbc.driver.OracleDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.*;
import requestdata.CustomerValidation;
import requestdata.FetchByDate;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


@RestController
@PropertySource("file:/etc/bigmart_data_fetch/dbconfig.properties")
@PropertySource("file:/etc/bigmart_data_fetch/application.properties")
public class AcceptRequestController {

    @Autowired
    private Environment env;

    public AcceptRequestController() {

    }


    String start_date = "";
    String end_date = "";
    String mobile = "";


    private int result_offset;
    private Map<String, String> table_info = new HashMap<>();

    private Logger log = LoggerFactory.getLogger(AcceptRequestController.class);
    private Connection conn;
    private Queries queries = new Queries();
    private String table_name = "";
    private int offset_value = -1;


    //    int range_count = 1000;
//    int range_count = 1000;
    int range_count = 1000;

    @RequestMapping(value = "/validate", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map<String, Object> validate(@RequestBody CustomerValidation customerValidation) {
        this.start_date = customerValidation.getMobile_no();
        Map<String, Object> jo = new HashMap<>();
        try {
            if (conn != null) {
                jo = validate(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = validate(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(jo);
        return jo;

    }

    private Map<String, Object> validate(Connection conn){
        Map<String, Object> jo = new HashMap<>();
        String status = "";
        String fetch_query = queries.fetchMobileRecord(mobile);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();
            ArrayList<Map<String, Object>> ja = new ArrayList<>();
            System.out.println("printing rs");
            if (num_col > 0) {
                jo.put("columns", ja);
                System.out.println("YES RS!!!!!!");
                System.out.println(rs.getObject(1));
                System.out.println(rsmd.getColumnTypeName(1));
            }
            else {
                System.out.println("NO RS!!!!");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        jo.put("status", status);
        return jo;
    }

    @RequestMapping(value = "/csv", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map<String, Object> tchiring(@RequestBody FetchByDate fetchByDate) {
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        Map<String, Object> jo = new HashMap<>();
        try {
            if (conn != null) {
                jo = fetchByDate(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchByDate(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(jo);
        return jo;

    }

    private Map<String, Object> fetchByDate(Connection conn) {
        Map<String, Object> jo = new HashMap<>();
        int total_count = 0;
        String status = "";
        String fetch_query = queries.fetchByDate(start_date, end_date);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();

            ArrayList<Map<String, Object>> ja = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> jo2 = new HashMap<>();
                for (int i = 1; i <= num_col; i++) {

                    Map<String, Object> m = new HashMap<>();
                    m.put("value", rs.getObject(i));
                    m.put("type", rsmd.getColumnTypeName(i));

                    jo2.put(rsmd.getColumnName(i).toLowerCase(), m);
                }
                ja.add(jo2);
            }
            jo.put("columns", ja);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        jo.put("status", status);
        return jo;
    }

    @RequestMapping(value = "/json", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map<String, Object> db_fetch(@RequestBody RequestData requestData) {

        System.out.println("REQUEST AYOOO!!!");
        System.out.println("table name = " + table_name);
        System.out.println("offset = " + offset_value);
        this.offset_value = requestData.getOffset();
        this.table_name = requestData.getTable_name();
//        env.getProperty("range_count");
        this.range_count = Integer.parseInt(env.getProperty("range_count"));


        Map<String, Object> jo = new HashMap<>();
        try {
            if (conn != null) {
                jo = fetchData(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchData(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(jo);
        return jo;
    }

    private Map<String, Object> fetchData(Connection conn) {
        String crow_query = queries.getCountQuery(table_name);
//        String fetch_query = queries.getFetchQuery(table_name, offset_value, range_count);
//        String fetch_query = queries.fetchTransactionRecord(table_name, offset_value, range_count);
        String fetch_query = queries.fetchByDate(start_date, end_date);
        System.out.println("fetch query = " + fetch_query);

        Map<String, Object> jo = new HashMap<>();
        int total_count = 0;
        String status = "";
        try {
            Statement stmt = conn.createStatement();
            if (table_info.containsKey(table_name)) {
                total_count = Integer.parseInt(table_info.get(table_name));
            } else {
                ResultSet rss = null;

                rss = stmt.executeQuery(crow_query);
                rss.next();
                total_count = rss.getInt(1);
                table_info.put(table_name, String.valueOf(total_count));
                rss.close();
            }

            if (offset_value >= Integer.parseInt(env.getProperty("offset_value"))) {
//            if (offset_value >= total_count) {
                status = "done";
//                conn.close();
            } else {
                status = "running";
                result_offset = offset_value + range_count;
                if (result_offset >= total_count) {
                    result_offset = total_count;
                }

                ResultSet rs = stmt.executeQuery(fetch_query);
                ResultSetMetaData rsmd = null;
                rsmd = rs.getMetaData();
                int num_col = 0;
                num_col = rsmd.getColumnCount();

                jo.put("table_name", table_name);
                ArrayList<Map<String, Object>> ja = new ArrayList<>();
                int sent_count = 0;
                while (rs.next()) {
                    Map<String, Object> jo2 = new HashMap<>();
                    for (int i = 1; i <= num_col; i++) {

                        Map<String, Object> m = new HashMap<>();
                        m.put("value", rs.getObject(i));
                        m.put("type", rsmd.getColumnTypeName(i));

                        jo2.put(rsmd.getColumnName(i).toLowerCase(), m);
                    }
                    ja.add(jo2);
                    sent_count++;
                }
//                System.out.println(sent_count);
                jo.put("count", sent_count);
                jo.put("columns", ja);

                jo.put("offset", result_offset);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        jo.put("status", status);
        return jo;
    }
}

//    private static final Logger logger = Logger.getLogger(bootstrap.AcceptRequestController.class);

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
//    public bootstrap.AcceptRequestController() {
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