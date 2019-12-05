package requestdata;

public class FetchItem {
    String table_name;
    int offset_value;
    int range_count;

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public int getOffset_value() {
        return offset_value;
    }

    public void setOffset_value(int offset_value) {
        this.offset_value = offset_value;
    }

    public int getRange_count() {
        return range_count;
    }

    public void setRange_count(int range_count) {
        this.range_count = range_count;
    }
}
