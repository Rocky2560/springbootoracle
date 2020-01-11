package bootstrap;

import com.fasterxml.jackson.annotation.JsonAlias;
import encryption.AES;
import misc.Queries;
import misc.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.*;
import requestdata.*;

import javax.swing.plaf.nimbus.State;
import javax.transaction.TransactionRequiredException;
import javax.xml.transform.Result;
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
    private ArrayList site_code;

    public AcceptRequestController() {

    }

    String start_date = "";
    String end_date = "";
    String mobile = "";
    String key = "";
    String date_column = "";
    private int limit = 1;
    boolean encrypt = true;
    String bill_no = "";


    private int result_offset;
    private Map<String, String> table_info = new HashMap<>();

    private Logger log = Logger.getLogger(AcceptRequestController.class);
    private Connection conn;
    private Queries queries = new Queries();
    private String table_name = "";
    int offset_value = -1;
    int range_count = 1000;

    @RequestMapping(value = "/validate", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public ArrayList validate(@RequestBody CustomerValidation customerValidation) {
        this.mobile = customerValidation.getMobile_no();
        ArrayList<Map<String, Object>> jo = new ArrayList<Map<String, Object>>();
        try {
            if (conn != null) {
                jo.add(validate(conn));
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo.add(validate(conn));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Validate: ", e);
        }
        return jo;
    }

    private Map<String, Object> validate(Connection conn) {
        log.info("Fetching lpcardno of mobile number:" + mobile);
        Map<String, Object> jo = new HashMap<>();
        String fetch_query = queries.fetchMobileRecord(mobile);
        boolean status = false;
        int code = Status.CUSTOMER_NOT_FOUND;
        ArrayList lp = new ArrayList();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            ResultSetMetaData resultSetMetaData = null;
            resultSetMetaData = rs.getMetaData();
            int col_num = resultSetMetaData.getColumnCount();
            while (rs.next()) {
                lp.add(rs.getObject(1));
                jo.put(resultSetMetaData.getColumnName(2).toLowerCase(), rs.getObject(2));
                status = true;
                code = Status.OK_QUERY;
//                System.out.println(lp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Validate: ", e);
        }

        jo.put("lpcardno", lp);
        jo.put("code", code);
        jo.put("status", status);
        return jo;
    }

//    @RequestMapping(value = "/csv", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
//    public JSONArray tchiring(@RequestBody FetchByDate fetchByDate) {
//        this.table_name = fetchByDate.getTable_name();
//        this.start_date = fetchByDate.getStart_date();
//        this.end_date = fetchByDate.getEnd_date();
//        this.date_column = fetchByDate.getDate_column();
//        JSONArray jo = new JSONArray();
////        String jo = "";
//        try {
//            if (conn != null) {
//                jo = fetchByDate(conn);
//            } else {
//                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
//                jo = fetchByDate(conn);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
////        System.out.println(jo.size());
////        System.out.println(jo);
//        return jo;
//    }

    private JSONArray fetchByDate(Connection conn) {
        log.info("INFO Fetching table: " + table_name + " " + "start_date:" + start_date + " " + "end_date:" + end_date + "\n");
        String fetch_query = queries.fetchByDate(table_name, start_date, end_date, date_column);
//        System.out.println(fetch_query);
        JSONArray ja = new JSONArray();
        int off_count = 0;

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
                off_count++;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("enc_date_table: ", e);
        }

        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("sale_count"), true));
            bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
            bf.flush();
            bf.close();
        } catch (IOException e) {
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

    @RequestMapping(value = "/fetch_sale", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String aesFetchSale(@RequestBody FetchSale fetchSale) {
//        log.error("CHECK ENC_DATE_TABLE");
        String key = env.getProperty("key");
        this.start_date = fetchSale.getStart_date();
        this.end_date = fetchSale.getEnd_date();
        this.site_code = fetchSale.getSite_code();

//        System.out.println("123123123");
//        System.out.println(site_code);
//        System.out.println("123123123");

        JSONArray jo = new JSONArray();
//        String jo = "";
        try {
            if (conn != null) {
                jo = fetchSale(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchSale(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
//            log.error("enc_date_table: ", e);
        }
//        System.out.println(jo);
//        System.out.println(AES.encrypt(jo.toString(), key));
//        return AES.encrypt(jo.toString(), key);
//        System.out.println(jo);
        return AES.encrypt(jo.toString(), key);
    }

    private JSONArray fetchSale(Connection conn) {
//        log.info("INFO Fetching table: " + table_name  + " " + "start_date:" + start_date + " " + "end_date:" + end_date + "\n");
        JSONArray main_arr = new JSONArray();
        String fetch_query = queries.fetchSale(start_date, end_date, site_code);
        Map<String, Object> main_map = new TreeMap<>();
        Set<String> final_site_code = new HashSet<>();
        JSONArray ja = new JSONArray();
//        Map<String,Object> site_map = new HashMap<>();
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
                    if (rsmd.getColumnName(i).toLowerCase().equals("admsite_code")) {
                        final_site_code.add(rs.getObject(i).toString());
                    }
                }
                ja.put(jo2);
            }
            main_map.put("site_code", final_site_code);
            main_map.put("result", ja);
            main_arr.put(main_map);
//            System.out.println(main_arr);
//            site_map.put("site_exist", final_site_code);
        } catch (SQLException e) {
            e.printStackTrace();
//            log.error("enc_date_table: ", e);
        }
//        try {
//            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("sale_count"),true));
//            bf.write("{\"count\":"+ off_count +",\"date\":\""+ java.time.LocalDateTime.now() +"}\n");
//            bf.flush();
//            bf.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

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
//        return ja;
        return main_arr;
    }

    @RequestMapping(value = "/enc_date_table", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String aesEncDateTable(@RequestBody FetchByDate fetchByDate) {
//        log.error("CHECK ENC_DATE_TABLE");
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
            log.error("enc_date_table: ", e);
        }
//        System.out.println(jo);
//        System.out.println(AES.encrypt(jo.toString(), key));
        return AES.encrypt(jo.toString(), key);
    }

    @RequestMapping(value = "/sales_history", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String salesHistory(@RequestBody FetchByDate fetchByDate) {
//        log.error("CHECK ENC_DATE_TABLE");
        String key = env.getProperty("key");
        this.table_name = fetchByDate.getTable_name();
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        JSONArray jo = new JSONArray();
//        String jo = "";
        try {
            if (conn != null) {
                jo = fetchSalesHistory(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchSalesHistory(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("enc_date_table: ", e);
        }
//        System.out.println(jo);
        if (!encrypt) {
            return String.valueOf(jo);
        } else {
            return AES.encrypt(jo.toString(), key);

        }
    }

    private JSONArray fetchSalesHistory(Connection conn) {
//        log.info("INFO Fetching table: " + table_name + " " + "start_date:" + start_date + " " + "end_date:" + end_date + "\n");
        String fetch_bill_info = queries.fetchBillInfo(start_date,end_date);
        String fetch_product_info = queries.fetchProductInfo(bill_no);
//        System.out.println(fetch_query);
        JSONArray ja = new JSONArray();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_bill_info);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();
            Statement prd_stmt = conn.createStatement();

            while (rs.next()) {
                Map<String, Object> jo2 = new TreeMap<>();
                for (int i = 1; i <= num_col; i++) {
                    jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
                    System.out.println(rsmd.getColumnName(i));
                    if (rsmd.getColumnName(i).contains("BILLNO")){
                        bill_no = (String) rs.getObject(i);

                        ResultSet rs_product = prd_stmt.executeQuery(fetch_product_info);
                        ResultSetMetaData rsmd_product = null;
                        rsmd_product = rs_product.getMetaData();
                        int prod_num_col = 0;
                        prod_num_col = rsmd_product.getColumnCount();
                        while (rs_product.next()){
                            for (int j = 1; j <= prod_num_col; j++){
                                System.out.println(rs.getObject(j));
                            }
                        }

                    }
                }
                ja.put(jo2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("enc_date_table: ", e);
        }
        return ja;
    }

    @RequestMapping(value = "/mongo", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String test_fetch(@RequestBody FetchByDate fetchByDate){
//        log.error("CHECK ENC_DATE_TABLE");
        String key = env.getProperty("key");
        this.table_name = fetchByDate.getTable_name();
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        this.date_column = fetchByDate.getDate_column();
        this.limit = fetchByDate.getLimit();
        this.encrypt = fetchByDate.isEncrypt();

        JSONArray jo = new JSONArray();
//        String jo = "";
        try {
            if (conn != null) {
                jo = fetchForMongo(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchForMongo(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("enc_date_table: ", e);
        }
//        System.out.println(jo);
//        System.out.println(AES.encrypt(jo.toString(), key));
        if (!encrypt){
            return String.valueOf(jo);
        }
        else {
            return AES.encrypt(jo.toString(), key);

        }
    }

    private JSONArray fetchForMongo(Connection conn) {
//        log.info("INFO Fetching table: " + table_name + " " + "start_date:" + start_date + " " + "end_date:" + end_date + "\n");
        String fetch_query = queries.fetchForMongo(table_name, start_date, end_date, date_column, limit);
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
            log.error("enc_date_table: ", e);
        }

//        try {
//            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("sale_count"), true));
//            bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
//            bf.flush();
//            bf.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return ja;
    }

    @RequestMapping(value = "/enc_table", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map aesEncTable(@RequestBody RequestData requestData) {
        log.info("INFO ENC_TABLE");
//        log.error("CHECK ENC_TABLE");
        this.table_name = requestData.getTable_name();
        this.offset_value = requestData.getOffset_value();
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
            log.error("enc_table: ", e);
        }
        return jo;
    }



    private Map<String, Object> fetchTable(Connection conn) {
        log.info("INFO Fetching table: " + table_name  + " " + "offset_value:" + offset_value + "\n");
        String key = env.getProperty("key");
        int off_count = 0;
        Map<String, Object> off_map = new TreeMap<>();
//        String status = "";
        String crow_query = queries.getCountQuery(table_name);
        String fetch_query = queries.fetchTable(table_name,offset_value);
        JSONArray ja = new JSONArray();
        int total_count = 0;
//        try {
//            Statement statement = conn.createStatement();
//            ResultSet resultSet = statement.executeQuery(crow_query);
//            resultSet.next();
//            total_count = resultSet.getInt(1);
////            System.out.println(total_count);
//            if (offset_value >= total_count){
//                off_map.put("status", "check offset");
//            }
//            else {
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
                        off_count++;
                    }
                    off_count = off_count + offset_value;
//                    System.out.println(off_count);
                    off_map.put("offset_value", off_count);
                    off_map.put("value", AES.encrypt(ja.toString(), key));
                } catch (SQLException e) {
                    e.printStackTrace();
                    log.error("enc_table: ", e);
                }
                if (table_name.toLowerCase().equals("mmpl.v_ekb_cust")) {
                    try {
                        BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("cust_count"), true));
                        bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                        bf.flush();
                        bf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                else if (table_name.toLowerCase().equals("mmpl.v_ekb_site")){
                    try {
                        BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("site_count"), true));
                        bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                        bf.flush();
                        bf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        return off_map;
        }

    @RequestMapping(value = "/item", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map<String, Object> itemFetch(@RequestBody FetchItem fetchItem) {
        log.info("Fetching item table");
//        log.error("CHECK ITEM");
        this.table_name = fetchItem.getTable_name();
        this.offset_value = fetchItem.getOffset_value();
        Map<String, Object> jo = new TreeMap<>();
        try {
            if (conn != null) {
                jo = fetchItem(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchItem(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Item: ", e);
        }

//        try {
//            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("count_file_item"),true));
//            bf.write("{\"count\":"+ jo.size() +",\"date\":\""+ java.time.LocalDateTime.now() +"}\n");
//            bf.flush();
//            bf.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return jo;
    }

    private Map<String, Object> fetchItem(Connection conn) {
        log.info("INFO Fetching table: " + table_name  + " " + "offset_value:" + offset_value + "\n");
        String key = env.getProperty("key");
        int off_count = 0;
        Map<String, Object> of_map = new TreeMap<>();
//        String status = "";
        String crow_query = queries.getCountQuery(table_name);
        String fetch_query = queries.fetchItemTable(offset_value);
        JSONArray ja = new JSONArray();
        int total_count = 0;
//        try {
//            Statement statement = conn.createStatement();
//                ResultSet resultSet = statement.executeQuery(crow_query);
//                resultSet.next();
//                total_count = resultSet.getInt(1);
////            System.out.println(total_count);
//                if (offset_value >= total_count){
//                    of_map.put("status", "check offset");
//                }
//                else {
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
                            off_count++;
                        }
                        off_count = off_count + offset_value;
                        of_map.put("offset_value", off_count);
                        of_map.put("value", AES.encrypt(ja.toString(), key));
                    } catch (SQLException e) {
                        e.printStackTrace();
                        log.error("Item: ", e);
                    }
        try {
            BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("item_count"),true));
            bf.write("{\"count\":"+ off_count +",\"date\":\""+ java.time.LocalDateTime.now() +"}\n");
            bf.flush();
            bf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
        return of_map;
    }

    @RequestMapping(value = "/json", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public Map<String, Object> db_fetch(@RequestBody RequestData requestData) {
        System.out.println("REQUEST AYOOO!!!");
        System.out.println("table name = " + table_name);
        System.out.println("offset = " + offset_value);
        this.offset_value = requestData.getOffset_value();
        this.table_name = requestData.getTable_name();
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