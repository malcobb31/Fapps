package rest_api_v3;

public class rest_api_PrintUtilMsg {

    public void info(String message) {
        System.out.println("[INFO] " + message);
    }

    public void warn(String message) {
        System.out.println("[WARN] " + message);
    }

    public void success(String message) {
        System.out.println("[SUCCESS] " + message);
    }

    public void printError(String message) {
        System.err.println("[ERROR] " + message);
    }

    public void start(String message) {
        System.out.println("===== " + message + " =====");
    }
}
