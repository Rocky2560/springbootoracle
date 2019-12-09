package bootstrap;

import encryption.AES;
import misc.Queries;
import misc.Status;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.*;
import requestdata.CustomerValidation;
import requestdata.FetchByDate;
import requestdata.FetchItem;
import requestdata.FetchStore;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;


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
    String key = "";
    String date_column = "";



    private int result_offset;
    private Map<String, String> table_info = new HashMap<>();

    private Logger log = LoggerFactory.getLogger(AcceptRequestController.class);
    private Connection conn;
    private Queries queries = new Queries();
    private String table_name = "";
    int offset_value = -1;
    int range_count = 1000;

    @RequestMapping(value = "/validate", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public ArrayList validate(@RequestBody CustomerValidation customerValidation) {
        this.mobile = customerValidation.getMobile_no();
        ArrayList<Map<String,Object>> jo = new ArrayList<Map<String,Object>>();
        try {
            if (conn != null) {
                jo.add(validate(conn));
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo.add(validate(conn));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jo;
    }

    private Map<String, Object> validate(Connection conn) {
        Map<String, Object> jo = new HashMap<>();
        String fetch_query = queries.fetchMobileRecord(mobile);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            if (rs.next()) {
                jo.put("status", true);
                jo.put("code", Status.OK_QUERY);
            } else {
                jo.put("status", false);
                jo.put("code", Status.CUSTOMER_NOT_FOUND);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return jo;
    }

    @RequestMapping(value = "/csv", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public JSONArray tchiring(@RequestBody FetchByDate fetchByDate) {
        this.table_name = fetchByDate.getTable_name();
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        this.date_column = fetchByDate.getDate_column();
        JSONArray jo = new JSONArray();
//        String jo = "";
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

//        System.out.println(jo.size());
//        System.out.println(jo);
        return jo;
    }

    private JSONArray fetchByDate(Connection conn) {
        String fetch_query = queries.fetchByDate(table_name, start_date, end_date, date_column);
//        System.out.println(fetch_query);
        JSONArray ja = new JSONArray();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();

            while (rs.next()) {
                Map<String, Object> jo2 = new HashMap<>();
                for (int i = 1; i <= num_col; i++) {
                    jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                ja.put(jo2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
//        System.out.println(ja);
//        try {
//            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("count_file"),true));
//            bf.write("{\"count\":"+ ja.size() +",\"start_date\":\""+ start_date +"\",\"end_date\":\""+ end_date +"\"}\n");
//            bf.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        jo.put("status", status);
//        return AES.encrypt(ja.toString(), key);
        return ja;
    }

    @RequestMapping(value = "/enc_date_table", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String aesEncDateTable(@RequestBody FetchByDate fetchByDate){
        String key = env.getProperty("key");
        this.table_name = fetchByDate.getTable_name();
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        this.date_column = fetchByDate.getDate_column();

        JSONArray jo = new JSONArray();
//        String jo = "";
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
//        System.out.println(jo);
//        System.out.println(AES.encrypt(jo.toString(), key));
        return AES.encrypt(jo.toString(), key);
    }

    @RequestMapping(value = "/enc_table", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map aesEncTable(@RequestBody RequestData requestData) {

        this.table_name = requestData.getTable_name();
        this.offset_value = requestData.getOffset();
        Map jo = new TreeMap();
        try {
            if (conn != null) {
                jo = fetchTable(conn);

            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchTable(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jo;
    }

    private Map fetchTable(Connection conn) {
        String key = env.getProperty("key");
        Map off_map = new TreeMap();
        int count = 0;
        String status = "";
        String fetch_query = queries.fetchTable(table_name, offset_value);
        JSONArray ja = new JSONArray();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();

            while (rs.next()) {
                Map<String, Object> jo2 = new HashMap<>();
                for (int i = 1; i <= num_col; i++) {
                    jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                ja.put(jo2);
                count++;
            }
            off_map.put("offset_value", count);
            off_map.put("value", AES.encrypt(ja.toString(), key));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return off_map;
    }




    @RequestMapping(value = "/item", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map itemFetch(@RequestBody FetchItem fetchItem) {
        this.table_name = fetchItem.getTable_name();
        this.offset_value = fetchItem.getOffset_value();
        this.range_count = fetchItem.getRange_count();

        Map jo = new HashMap();
        try {
            if (conn != null) {
                jo = fetchItem(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchItem(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("count_file_item"),true));
            bf.write("{\"count\":"+ jo.size() +",\"date\":\""+ java.time.LocalDateTime.now() +"}\n");
            bf.flush();
            bf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        System.out.println(jo.size());
//        System.out.println(jo);
        return jo;
    }

    private Map fetchItem(Connection conn) {
        String key = env.getProperty("key");
        Map off_map = new TreeMap();
        int count = 0;
        String status = "";
        String fetch_query = queries.fetchTable(table_name, offset_value);
        JSONArray ja = new JSONArray();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();

            while (rs.next()) {
                Map<String, Object> jo2 = new HashMap<>();
                for (int i = 1; i <= num_col; i++) {
                    jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                ja.put(jo2);
                count++;
            }
            off_map.put("offset", count);
            off_map.put("value", AES.encrypt(ja.toString(), key));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return off_map;
    }

//    private ArrayList fetchItem(Connection conn) {
//        Map<String, Object> jo = new HashMap<>();
//        int total_count = 0;
//        String status = "";
//        String fetch_query = queries.fetchTable(table_name, offset_value);
//        ArrayList<Map<String, Object>> ja = new ArrayList<>();
//        try {
//            Statement stmt = conn.createStatement();
//            ResultSet rs = stmt.executeQuery(fetch_query);
//            ResultSetMetaData rsmd = null;
//            rsmd = rs.getMetaData();
//            int num_col = 0;
//            num_col = rsmd.getColumnCount();
//
//            while (rs.next()) {
//                Map<String, Object> jo2 = new HashMap<>();
//                for (int i = 1; i <= num_col; i++) {
//                    jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
//                }
//                ja.add(jo2);
////                System.out.println(ja);
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
////        jo.put("status", status);
//        return ja;
//    }

//    @RequestMapping(value = "/site", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
//    public ArrayList storeFetch(@RequestBody RequestData requestData) {
//        this.table_name = requestData.getTable_name();
//        this.offset_value = requestData.getOffset();
//        ArrayList jo = new ArrayList<>();
//        try {
//            if (conn != null) {
//                jo = () fetchTable(conn);
//            } else {
//                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
//                jo = (ArrayList) fetchTable(conn);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        System.out.println(env.getProperty("count_file_store"));
//
//        try {
//            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("count_file_store"),true));
//            bf.write("{\"count\":"+ jo.size() +",\"date\":\""+ java.time.LocalDateTime.now() +"}\n");
//            bf.flush();
//            bf.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
////        System.out.println(jo.size());
////        System.out.println(jo);
//        return jo;
//    }



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
        String fetch_query = queries.fetchTransactionRecord(table_name, offset_value, range_count);
//        String fetch_query = queries.fetchByDate(table_name,start_date, end_date);
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