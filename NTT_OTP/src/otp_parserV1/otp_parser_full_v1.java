package otp_parserV1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class otp_parser_full_v1 {

    static String DB_URL = "jdbc:mysql://127.0.0.1:3306/urlScan";
    static String DB_USER = "root";
    static String DB_PASS = "admin@321";

    static String CAPTCHA_API = "http://10.10.2.131/urlScan/fcm_test.php?fileName=";

    public static void main(String[] args) {
        try {

            // ---------------- REQUIRED PARAM ----------------
            if (args.length < 1) {
                System.out.println("ERROR: order_id parameter missing!");
                System.out.println("Usage: java -jar otp_parser.jar <order_id> [captchaMaxAttempts captchaWaitSeconds otpMaxAttempts otpWaitSeconds]");
                return;
            }

            String order_id = args[0]; 

            int captchaMaxAttempts = 20;
            int captchaWaitSeconds = 5;

            int otpMaxAttempts = 5;
            int otpWaitSeconds = 20;

            if (args.length >= 5) {
                captchaMaxAttempts = Integer.parseInt(args[1]);
                captchaWaitSeconds = Integer.parseInt(args[2]);
                otpMaxAttempts = Integer.parseInt(args[3]);
                otpWaitSeconds = Integer.parseInt(args[4]);
            }

            System.out.println("==============================================");
            System.out.println("CAPTCHA + OTP TEST PROCESS STARTED");
            System.out.println("Order ID            = " + order_id);
            System.out.println("Captcha Max Attempts = " + captchaMaxAttempts);
            System.out.println("Captcha Wait Seconds = " + captchaWaitSeconds);
            System.out.println("OTP Max Attempts     = " + otpMaxAttempts);
            System.out.println("OTP Wait Seconds     = " + otpWaitSeconds);
            System.out.println("==============================================");

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
            String currentTime = LocalDateTime.now().format(formatter);
            String captchaFileName = "captcha_20260116_204924_.png";
;

            System.out.println("Captcha File Name = " + captchaFileName);

            // 1) HIT CAPTCHA API
            callCaptchaAPI(captchaFileName);

            // 2) WAIT CAPTCHA
            CaptchaResult captchaResult = waitForCaptchaFromDB(captchaMaxAttempts, captchaWaitSeconds);
            if (captchaResult == null) {
                System.out.println(" CAPTCHA NOT FOUND IN DB. EXITING...");
                return;
            }

            System.out.println(" CAPTCHA RECEIVED FROM DB = " + captchaResult.captchaCode);
            System.out.println(" CAPTCHA CREATED_AT       = " + captchaResult.createdAt);

            // 3) WAIT OTP (ONLY AFTER CAPTCHA CREATED_AT)
            String otpValue = waitForOTPFromDB(otpMaxAttempts, otpWaitSeconds, captchaResult.createdAt);
            if (otpValue == null) {
                System.out.println(" OTP NOT FOUND IN DB. EXITING...");
                return;
            }

            System.out.println(" OTP RECEIVED FROM DB = " + otpValue);

            System.out.println("/home/pview/apache_pview/htdocs/urlScan/uploads/sahyog_mail_attachments/OCBMC_NTT.xls");

        } catch (Exception e) {
            System.out.println(" ERROR in process: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void callCaptchaAPI(String captchaFileName) {
        try {
            String fullUrl = CAPTCHA_API + captchaFileName;

            System.out.println(" Calling Captcha API...");
            System.out.println("URL = " + fullUrl);

            URL url = new URL(fullUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(10000);

            int responseCode = conn.getResponseCode();
            System.out.println("Captcha API Response Code = " + responseCode);

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            System.out.println("Captcha API Response = " + response);

        } catch (Exception e) {
            System.out.println(" Captcha API Call Failed: " + e.getMessage());
        }
    }

    public static CaptchaResult waitForCaptchaFromDB(int maxAttempts, int waitSeconds) {

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            System.out.println(" Checking CAPTCHA in DB... Attempt " + attempt + "/" + maxAttempts);

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                String selectQuery =
                        "SELECT sno, captcha_code, created_at FROM captcha_table " +
                                "WHERE is_used=0 ORDER BY sno DESC LIMIT 1";

                PreparedStatement ps = conn.prepareStatement(selectQuery);
                ResultSet rs = ps.executeQuery();

                if (rs.next()) {
                    int sno = rs.getInt("sno");
                    String captcha = rs.getString("captcha_code");
                    Timestamp createdAt = rs.getTimestamp("created_at");

                    String updateQuery =
                            "UPDATE captcha_table SET is_used=1 WHERE sno=?";
                    PreparedStatement ps2 = conn.prepareStatement(updateQuery);
                    ps2.setInt(1, sno);
                    ps2.executeUpdate();

                    System.out.println(" CAPTCHA FOUND: " + captcha + " (sno=" + sno + ")");
                    return new CaptchaResult(captcha, createdAt);
                }

            } catch (Exception e) {
                System.out.println(" DB Error (Captcha): " + e.getMessage());
            }

            sleepSeconds(waitSeconds);
        }

        System.out.println(" NO CAPTCHA RECEIVED AFTER MAX ATTEMPTS!");
        return null;
    }

    public static String waitForOTPFromDB(int maxAttempts, int waitSeconds, Timestamp captchaCreatedAt) {

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            System.out.println(" Checking OTP in DB... Attempt " + attempt + "/" + maxAttempts);

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                String selectQuery =
                        "SELECT sno, otp, created_at FROM otp_table " +
                                "WHERE created_at >= ? ORDER BY sno DESC LIMIT 1";

                PreparedStatement ps = conn.prepareStatement(selectQuery);
                ps.setTimestamp(1, captchaCreatedAt);

                ResultSet rs = ps.executeQuery();

                if (rs.next()) {
                    int sno = rs.getInt("sno");
                    String otp = rs.getString("otp");
                    Timestamp otpCreatedAt = rs.getTimestamp("created_at");

                    System.out.println(" OTP FOUND: " + otp + " (sno=" + sno + ", created_at=" + otpCreatedAt + ")");
                    return otp;
                }

            } catch (Exception e) {
                System.out.println(" DB Error (OTP): " + e.getMessage());
            }

            sleepSeconds(waitSeconds);
        }

        System.out.println(" NO OTP RECEIVED AFTER MAX ATTEMPTS!");
        return null;
    }

    public static void sleepSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException ignored) {
        }
    }

    static class CaptchaResult {
        String captchaCode;
        Timestamp createdAt;

        CaptchaResult(String captchaCode, Timestamp createdAt) {
            this.captchaCode = captchaCode;
            this.createdAt = createdAt;
        }
    }
}
