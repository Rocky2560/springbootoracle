package misc;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class Queries {


    public String getCountQuery(String table_name){
        return "select count(*) from " + table_name + "";
    }

    public String getFetchQuery(String table_name,int offset_value,int range_count){
        return "select LPCARDNO, NAME from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
    }


    public String fetchBillInfoCount(String start_date, String end_date, String lpcardno,int  limit, int offset_value){
        return "select count(count(billno)) from mmpl.V_EKB_CUST_SALE where billdate >= '"+ start_date +"' and billdate < '"+ end_date +"' and lpcardno IN "+lpcardno+"";
    }

    public String fetchBillInfo(String start_date, String end_date, String lpcardno,int  limit, int offset_value){
        return "with temp as (select billno, admsite_code, listagg(cat1 || ':' || icode || ':' || netamt || ':' || saleqty || ':' || (saleqty*netamt), ';') within group (order by billno) \"ITEMS\" from mmpl.V_EKB_CUST_SALE where billdate>='"+ start_date +"' and billdate<'"+ end_date+"' and lpcardno IN "+lpcardno+" group by billno, admsite_code) select c.billno, c.admsite_code, b.lpcardno, b.billdate, b.netsales, c.items from mmpl.V_EKB_POSBILL b join temp c on c.billno=b.billno and c.admsite_code=b.site_code offset "+offset_value+" rows fetch next "+ limit +" rows only";
    }
    public String fetchProductInfo(String bill_no){
//        return "select BILLNO, BILLDATE, ADMSITE_CODE, TOATALDISCOUNTAMT, LPCARDNO, CAT1, ICODE, SALEQTY from mmpl.V_EKB_CUST_SALE where billdate >='"+start_date+"' and billdate < '"+end_date+"'" ;
//        return "select DISTINCT(BILLNO), BILLDATE, ADMSITE_CODE, TOATALDISCOUNTAMT, LPCARDNO,  from mmpl.V_EKB_CUST_SALE where billdate >='"+start_date+"' and billdate < '"+end_date+"'" ;
        return "select CAT1, ICODE, SALEQTY from mmpl.V_EKB_CUST_SALE where billno = '"+bill_no+"'";
    }


    public String fetchSale (String start_date, String end_date, ArrayList<String> site_code){
//        String temp = StringUtils.join(site_code, "\', \'");
//        String temp2 = StringUtils.wrap(temp, "\'");
        return "select * from mmpl.V_EKB_CUST_SALE where billdate >= '"+ start_date +"' and billdate < '"+ end_date + "' and admsite_code in (" + StringUtils.join(site_code, ',') + ")";
//        return "select * from mmpl.V_EKB_CUST_SALE where billdate >= '"+ start_date +"' and billdate < '"+ end_date + "' and admsite_code in (" + temp2 + ")";
    }

    public String fetchTransactionRecord (String table_name,int offset_value,int range_count){
        return "select * from " + table_name + " offset " + offset_value + " rows fetch next " + range_count + " rows only";
    }

    public String fetchByDate (String table_name, String start_date, String end_date, String date_column){
        return "select * from " + table_name +" where " + date_column + " > '" + start_date + "' and " + date_column + " < '" + end_date + "'";
    }

    public String fetchForMongo (String table_name, String start_date, String end_date, String date_column, int limit){
        return "select * from " + table_name +" where " + date_column + " > '" + start_date + "' and " + date_column + " < '" + end_date + "' offset 0 rows fetch next " + limit + " rows only";
    }

    public String fetchMobileRecord (String mobile_no){
        return "select LPCARDNO, NAME from mmpl.V_EKB_CUST where MOBILE ='" + mobile_no + "'" + "ORDER BY NAME DESC offset 0 rows";
    }

    public String fetchItemTable (int offset_value){
        return "select LEV1GRPNAME, LEV2GRPNAME, GRPNAME, CNAME1, CNAME2, CNAME3, CNAME4, CNAME5, CNAME6, ICODE from mmpl.V_ITEM offset " + offset_value + " rows";
    }

    public String fetchTable (String table_name, int offset_value){
        return "select * from " + table_name + " offset " + offset_value + " rows";
    }
}
