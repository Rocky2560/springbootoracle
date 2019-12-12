package misc;

public class Queries {
    public String getCountQuery(String table_name){
        return "select count(*) from " + table_name + "";
    }

    public String getFetchQuery(String table_name,int offset_value,int range_count){
        return "select LPCARDNO from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
    }

    public String fetchTransactionRecord (String table_name,int offset_value,int range_count){
        return "select * from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
    }

    public String fetchByDate (String table_name, String start_date, String end_date, String date_column){
        return "select * from " + table_name +" where " + date_column + " > '" + start_date + "' and " + date_column + " < '" + end_date + "'";
    }

    public String fetchMobileRecord (String mobile_no){
        return "select * from mmpl.V_EKB_CUST where MOBILE ='" + mobile_no + "'";
    }

    public String fetchItemTable (int offset_value){
        return "select LEV1GRPNAME, LEV2GRPNAME, GRPNAME, CNAME1, CNAME2, CNAME3, CNAME4, CNAME5, CNAME6, ICODE from mmpl.V_ITEM offset " + offset_value + " rows";
    }

    public String fetchTable (String table_name, int offset_value){
        return "select * from " + table_name + " offset " + offset_value + " rows";
    }
}
