package requestdata;


import java.util.ArrayList;

public class FetchSale {
    String start_date;
    String end_date;
    ArrayList<String> site_code;

    public ArrayList<String> getSite_code() {
        return site_code;
    }

    public void setSite_code(ArrayList site_code){
        this.site_code = site_code;
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
