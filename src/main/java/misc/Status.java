package misc;

public class Status {
   public static final int OK_QUERY = 200;
   public static final int CUSTOMER_NOT_FOUND = 205;
   public static final int CONNECTION_EXCEPTION = 300;
   public static final String CONNECTION_EXCEPTION_MSG = "Database Connection Failed! Status:" + Status.CONNECTION_EXCEPTION;
   public static final String DB_MAP_NOT_FOUND = "Table Key Not Found!" + Status.DB_MAP_NOT_FOUND;

}