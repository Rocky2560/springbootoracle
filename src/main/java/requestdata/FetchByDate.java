package requestdata;

public class FetchByDate {
    String start_date;
    String end_date;
    String table_name;
    String date_column;

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
