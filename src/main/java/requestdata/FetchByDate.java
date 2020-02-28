package requestdata;

import java.util.ArrayList;

public class FetchByDate {
    int table_key;

    public int getTable_key() {
        return table_key;
    }

    public void setTable_key(int table_key) {
        this.table_key = table_key;
    }

    String start_date;
    String end_date;
    String table_name;
    String date_column;
    int limit;
    boolean encrypt;
    int offset_value;

    public int getOffset_value() {
        return offset_value;
    }

    public void setOffset_value(int offset_value) {
        this.offset_value = offset_value;
    }

    String lpcardno;

    public String getLpcardno() {
        return lpcardno;
    }

    public void setLpcardno(String lpcardno) {
        this.lpcardno = lpcardno;
    }

    public boolean isEncrypt() {
        return encrypt;
    }

    public void setEncrypt(boolean encrypt) {
        this.encrypt = encrypt;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getDate_column() {return date_column;}

    public void setDate_column(String date_column) {this.date_column = date_column;}

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getStart_date() {
        return start_date;
    }

    public void setStart_date(String start_date) {
        this.start_date = start_date;
    }

    public String getEnd_date() {
        return end_date;
    }

    public void setEnd_date(String end_date) {
        this.end_date = end_date;
    }
}
