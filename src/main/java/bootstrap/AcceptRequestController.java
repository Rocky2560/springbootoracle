package bootstrap;

import encryption.AES;
import misc.Queries;
import misc.Status;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.*;
import requestdata.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
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
    String lpcardno = "";

    private int result_offset;
    private Map<String, String> table_info = new HashMap<>();

    private Logger log = Logger.getLogger(AcceptRequestController.class);
    private Connection conn;
    private Queries queries = new Queries();
    private String table_name = "";
    int offset_value = -1;
    int range_count = 1000;
    private int total_count = 0;
    private int table_key = 0;

    @RequestMapping(value = "/get_count", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
    public ArrayList<Map<String, Object>> validate(@RequestBody FetchByDate fetchByDate) {
        this.table_key = fetchByDate.getTable_key();
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        ArrayList<Map<String, Object>> jo = new ArrayList<Map<String, Object>>();
        try {
            if (conn != null) {
                jo.add(fetch_count(conn));
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo.add(fetch_count(conn));
            }
        } catch (Exception e) {
            e.printStackTrace();
//            log.error("Validate: " + e.getMessage());
        }
        System.out.println(jo);
        return jo;
    }

    private Map<String, Object> fetch_count(Connection conn) {
        Map<String, Object> jo = new TreeMap<>();
        String fetch_query = "";

        if (table_key == 1){
            table_name = env.getProperty("1");
            fetch_query = queries.QACountQuery(table_name,start_date,end_date);
        } else if (table_key == 2){
            table_name = env.getProperty("2");
            fetch_query = queries.QACountQuery(table_name,start_date,end_date);
        } else if (table_key == 3){
            table_name = env.getProperty("3");
            fetch_query = queries.QACountQuerySite(table_name);
        } else {
            jo.put("Wrong DB KEY", null);
        }
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_query);
            while (rs.next()) {
                if (rs.getObject(1) != null) {
                    jo.put("Count", rs.getObject(1));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
//            log.error("Validate: " + e);
        }
        return jo;
    }


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
            log.error("Validate: " + e.getMessage());
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
                if (rs.getObject(1)!= null) {
                    lp.add(rs.getObject(1));
                }
                jo.put(resultSetMetaData.getColumnName(2).toLowerCase(), rs.getObject(2));
                status = true;
                code = Status.OK_QUERY;
//                System.out.println(lp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Validate: " + e);
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
            log.error("enc_date_table: " + e);
        }
        if (env.getProperty("debug").equals("false")) {
            try {
                BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("sale_count"), true));
                bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                bf.flush();
                bf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
//    public JSONArray aesFetchSale(@RequestBody FetchSale fetchSale) {
//        log.error("CHECK ENC_DATE_TABLE");
        String key = env.getProperty("key");
        this.start_date = fetchSale.getStart_date();
        this.end_date = fetchSale.getEnd_date();
        this.site_code = fetchSale.getSite_code();

//        System.out.println("123123123");
//        System.out.println(site_code);
//        System.out.println("123123123");

//        JSONArray jo = new JSONArray();
        JSONArray jo = new JSONArray();
        Map<String, Object> map = new HashMap<>();
//        String jo = "";
        try {
            if (conn != null) {
                jo = fetchSale(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchSale(conn);
            }
        } catch (Exception e) {
            map.put("error_msg", Status.CONNECTION_EXCEPTION_MSG);
            jo.put(map);
            e.printStackTrace();
            log.error("fetch_sale: " + e.getMessage());
//            log.error("enc_date_table: ", e);
        }
//        System.out.println(jo);
        return AES.encrypt(jo.toString(), key);
//        return jo;
    }

    private JSONArray fetchSale(Connection conn) {
//        log.info("INFO Fetching table: " + table_name  + " " + "start_date:" + start_date + " " + "end_date:" + end_date + "\n");
        JSONArray main_arr = new JSONArray();
        String fetch_query = queries.fetchSale(start_date, end_date, site_code);
        Map<String, Object> main_map = new TreeMap<>();
        Set<Integer> final_site_code = new TreeSet<>();
        JSONArray ja = new JSONArray();
//        Map<String,Object> site_map = new HashMap<>();
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
                    if (rsmd.getColumnName(i).toLowerCase().equals("admsite_code")) {
                        final_site_code.add(rs.getInt(i));
//                        System.out.println(rs.getObject(i));
                    }
                }
                ja.put(jo2);
                off_count++;
            }
//            System.out.println(final_site_code);
            main_map.put("site_code", final_site_code);
            main_map.put("result", ja);
            main_arr.put(main_map);
//            System.out.println(main_arr);
//            site_map.put("site_exist", final_site_code);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("fetch_sale: " + e.getMessage());
        }
        try {
            if (env.getProperty("debug").equals("true")) {
                BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("sale_site_count"), true));
                bf.write("{\"start_date\": " + start_date + ",\"end_date\": " + end_date + ",\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                bf.flush();
                bf.close();
            }
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
        Map<String, Object> map = new TreeMap<>();
//        String jo = "";
        try {
            if (conn != null) {
                jo = fetchByDate(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchByDate(conn);
            }
        } catch (Exception e) {
            map.put("error_msg", Status.CONNECTION_EXCEPTION_MSG);
            jo.put(map);
            e.printStackTrace();
            log.error("enc_date_table: " + e.getMessage());
        }
//        System.out.println(jo);
//        System.out.println(AES.encrypt(jo.toString(), key));
        return AES.encrypt(jo.toString(), key);
    }

    @RequestMapping(value = "/sale_h", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String salesHistory(@RequestBody FetchByDate fetchByDate) {
//        log.error("CHECK ENC_DATE_TABLE");
        String key = env.getProperty("key");
        this.table_name = fetchByDate.getTable_name();
        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        this.lpcardno = fetchByDate.getLpcardno();
        this.limit = fetchByDate.getLimit();

//        System.out.println(lpcardno);
        Map<String, Object> map = new HashMap<>();
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
            map.put("error_msg", Status.CONNECTION_EXCEPTION_MSG);
            jo.put(map);
            e.printStackTrace();
            log.error("sale_h: " + e.getMessage());
        }
//        System.out.println(jo);

        return AES.encrypt(jo.toString(), key);
    }

    private JSONArray fetchSalesHistory(Connection conn) {
//        log.info("INFO Fetching table: " + table_name + " " + "start_date:" + start_date + " " + "end_date:" + end_date + "\n");
        String fetch_bill_info = queries.fetchBillInfo(start_date, end_date, lpcardno, limit, offset_value);
//        System.out.println(fetch_bill_info);
        JSONArray ja = new JSONArray();
        Map<String, Object> prod_map = new TreeMap<>();
        JSONArray prod_array = new JSONArray();
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(fetch_bill_info);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();
            String items = "";

            while (rs.next()) {
                Map<String, Object> jo2 = new TreeMap<>();
                for (int i = 1; i <= num_col; i++) {
                    jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
                }
                ja.put(jo2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("sales_h: " + e.getMessage());
        }
//        System.out.println(ja);
        return ja;
    }

    @RequestMapping(value = "/mongo", produces = "text/plain", consumes = "application/json", method = RequestMethod.POST)
    public String test_fetch(@RequestBody FetchByDate fetchByDate) {
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
        if (!encrypt) {
            return String.valueOf(jo);
        } else {
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
            log.error("enc_date_table: " + e);
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
            jo.put("error_msg", Status.CONNECTION_EXCEPTION_MSG);
            e.printStackTrace();
            log.error("enc_table: " + e.getMessage());
        }
        return jo;
    }

    private Map<String, Object> fetchTable(Connection conn) {
        log.info("INFO Fetching table: " + table_name + " " + "offset_value:" + offset_value + "\n");
        String key = env.getProperty("key");
        int off_count = 0;
        Map<String, Object> off_map = new TreeMap<>();
//        String status = "";
        String crow_query = queries.getCountQuery(table_name);
        String fetch_query = queries.fetchTable(table_name, offset_value);
        JSONArray ja = new JSONArray();
        int total_count = 0;
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
            log.error("enc_table: " + e.getMessage());
        }
        if (env.getProperty("debug").equals("false")) {
            try {
                if (table_name.toLowerCase().equals("mmpl.v_ekb_cust")) {
                    try {
                        BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("cust_count"), true));
                        bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                        bf.flush();
                        bf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else if (table_name.toLowerCase().equals("mmpl.v_ekb_site")) {
                    try {
                        BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("site_count"), true));
                        bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                        bf.flush();
                        bf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
            jo.put("error_msg", Status.CONNECTION_EXCEPTION_MSG);
            e.printStackTrace();
            log.error("Item: " + e.getMessage());
        }
        return jo;
    }

    private Map<String, Object> fetchItem(Connection conn) {
        log.info("INFO Fetching table: " + table_name + " " + "offset_value:" + offset_value + "\n");
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
            log.error("Item: " + e.getMessage());
        }
        if (env.getProperty("debug").equals("false")) {
            try {
                BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("item_count"), true));
                bf.write("{\"count\":" + off_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                bf.flush();
                bf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return of_map;
    }

    @RequestMapping(value = "/sales_history", produces = "application/json", consumes = "application/json", method = RequestMethod.POST)
//    public Map<String, Object> db_fetch(@RequestBody FetchByDate fetchByDate) {
    public String db_fetch(@RequestBody FetchByDate fetchByDate) {
//    public String db_fetch(@RequestBody FetchByDate fetchByDate) {
        String key = env.getProperty("key");
//        System.out.println("REQUEST AYOOO!!!");
//        System.out.println("table name = " + table_name);
//        System.out.println("offset = " + offset_value);

        this.start_date = fetchByDate.getStart_date();
        this.end_date = fetchByDate.getEnd_date();
        this.limit = fetchByDate.getLimit();
        this.offset_value = fetchByDate.getOffset_value();
        this.lpcardno = fetchByDate.getLpcardno();

//        Map<String, Object> jo = new HashMap<>();
        JSONObject jo = new JSONObject();
        try {
            if (conn != null) {
                jo = fetchData(conn);
            } else {
                conn = DriverManager.getConnection(Objects.requireNonNull(env.getProperty("db_url")), env.getProperty("db_usr"), env.getProperty("db_password"));
                jo = fetchData(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            jo.put("error_msg", Status.CONNECTION_EXCEPTION_MSG);
            log.error("sales_history: " + e.getMessage());
        }
//        System.out.println(jo);
        return AES.encrypt(jo.toString(), key);
    }

    private JSONObject fetchData(Connection conn) {
        String crow_query = queries.fetchBillInfoCount(start_date, end_date, lpcardno);
//        String fetch_query = queries.getFetchQuery(table_name, offset_value, range_count);
//        String fetch_query = queries.fetchTransactionRecord(table_name, offset_value, range_count);
//        String fetch_query = queries.fetchByDate(table_name,start_date, end_date);
        String fetch_bill_info = queries.fetchBillInfo(start_date, end_date, lpcardno, limit, offset_value);
        int check_offset = 0;
        int sent_count = 0;
        int total_count = 0;
//        Map<String, Object> jo = new HashMap<>();
        JSONObject jo = new JSONObject();
        String status = "";
        try {
            Statement stmt = conn.createStatement();
//            if (table_info.containsKey("mmpl.V_EKB_CUST_SALE")) {
////                total_count = Integer.parseInt(table_info.get("mmpl.V_EKB_CUST_SALE"));
//            } else {
            ResultSet rss = null;
            rss = stmt.executeQuery(crow_query);
            rss.next();
            total_count = rss.getInt(1);
//            System.out.println(total_count);
            table_info.put("mmpl.V_EKB_CUST_SALE", String.valueOf(total_count));
            rss.close();
//            System.out.println(total_count);
//            }
//            if (offset_value >= Integer.parseInt(env.getProperty("offset_value"))) {
//            if (offset_value >= total_count) {
//                status = "done";
//                conn.close();
//            } else {
//                status = "running";
//                result_offset = offset_value + limit;
//                if (limit >= total_count) {
//                    limit = total_count;
//                }

//            System.out.println(fetch_bill_info);
            ResultSet rs = stmt.executeQuery(fetch_bill_info);
            ResultSetMetaData rsmd = null;
            rsmd = rs.getMetaData();
            int num_col = 0;
            num_col = rsmd.getColumnCount();

            jo.put("table_name", "mmpl.V_EKB_CUST_SALE");
//                ArrayList<Map<String, Object>> ja = new ArrayList<>();
            JSONArray ja = new JSONArray();
            while (rs.next()) {
                Map<String, Object> jo2 = new HashMap<>();
                for (int i = 1; i <= num_col; i++) {

//                        Map<String, Object> m = new HashMap<>();
//                        m.put("value", rs.getObject(i));
//                        m.put("type", rsmd.getColumnTypeName(i));

//                        jo2.put(rsmd.getColumnName(i).toLowerCase(), m);

                    if( rsmd.getColumnName(i).toLowerCase().equals("items")) {
                        Clob clob = rs.getClob(rsmd.getColumnName(i));
                        Reader r = clob.getCharacterStream();
                        StringBuilder buffer = new StringBuilder();
                        int ch;
                        while ((ch = r.read())!=-1) {
                            buffer.append((char) ch);
                        }
//                        System.out.println(buffer);
                        jo2.put(rsmd.getColumnName(i).toLowerCase(), buffer);
                    }
                    else {
                        jo2.put(rsmd.getColumnName(i).toLowerCase(), rs.getObject(i));
                    }
                }
                ja.put(jo2);
                sent_count++;
            }

            check_offset = offset_value + limit;
            if (check_offset >= total_count) {
                result_offset = offset_value + sent_count;
                status = "done";
            } else {
                result_offset = offset_value + sent_count;
                status = "running";
            }
//            System.out.println("TOTAL_COUNT = " + total_count);
//            System.out.println("RESULT_OFFSET = "+ result_offset);
//            System.out.println("SENT_COUNT = "+ sent_count);

//                System.out.println(sent_count);
            jo.put("count", sent_count);
            jo.put("columns", ja);

            jo.put("offset_value", result_offset);
//            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            log.error("sales_history: " + e.getMessage());
        }
        jo.put("status", status);

        if (env.getProperty("debug").equals("false")) {
            try {
                BufferedWriter bf = new BufferedWriter(new FileWriter(env.getProperty("sales_history_count"), true));
                bf.write("{\"start_date\": " + start_date + ",\"end_date\": " + end_date + ",\"offset_value:\": " + offset_value + ",\"limit\": " + limit + ",\"count\":" + sent_count + ",\"date\":\"" + java.time.LocalDateTime.now() + "}\n");
                bf.flush();
                bf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("INFO Fetching table: mmpl.V_EKB_CUST_SALE;" + " start_date:" + start_date + ";" + " end_date:" + end_date + " offset_value:" + offset_value + ";" + " limit:" + limit  + "\n");


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