package misc;

public class Queries {
    public String getCountQuery(String table_name){
        return "select count(*) from " + table_name + "";
    }

    public String getFetchQuery(String table_name,int offset_value,int range_count){
        return "select LPCARDNO from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
//        return "select * from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
    }

    public String fetchTransactionRecord (String table_name,int offset_value,int range_count){
        return "select * from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
    }


    public String fetchByDate (String table_name, String start_date, String end_date, String date_column){
        return "select * from " + table_name +" where " + date_column + " > '" + start_date + "' and " + date_column + " < '" + end_date + "'";
//        return "select * from " + table_name + " offset 1 rows fetch next 2 rows only";
    }

    public String fetchMobileRecord (String mobile_no){
        return "select * from mmpl.V_EKB_CUST where MOBILE ='" + mobile_no + "'";
    }

//    public String fetchItem (String table_name, Integer offset_value, Integer range_count){
    public String fetchItemTable (String table_name, int offset_value){
        return "select site_code, address, contact_person, ctname, email1, email2, latitude, longitude, mobile, oph1, oph2, oph3, shrtname, site_name from mmpl.V_ITEM";
//        return "select * from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only where lev1grpname = 'DAIRY'";
//        return "select * from " + table_name + " where lev1grpname = 'DAIRY'";
//        return "select * from " + table_name + " where lev1grpname = 'FMCG FOOD' and lev2grpname = 'NOODLES & SOUP' and icode = 'BM7211'";
    }

    public String fetchTable (String table_name, int offset_value){
        return "select * from " + table_name + " offset " + offset_value + " rows";
    }
}
