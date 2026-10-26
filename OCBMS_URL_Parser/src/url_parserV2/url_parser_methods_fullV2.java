package url_parserV2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class url_parser_methods_fullV2 {

    // ---------------- DB CONFIG ----------------
    static String DB_URL = "jdbc:mysql://127.0.0.1:3306/urlScan";
    static String DB_USER = "root";
    static String DB_PASS = "Admin@321";

    // ---------------- API CONFIG ----------------
    static String CAPTCHA_API = "http://10.10.2.131/urlScan/fcm_test.php?fileName=";

    public static void main(String[] args) {
        try {

            // ---------------- VARIABLE THRESHOLDS (DEFAULTS) ----------------
            int captchaMaxAttempts = 1;
            int captchaWaitSeconds = 5;

            int otpMaxAttempts =1;
            int otpWaitSeconds = 15;

            // ---------------- OPTIONAL ARGUMENTS ----------------
            // java CaptchaOtpTestProcess 30 3 20 10
            // means: captchaAttempts=30 captchaWait=3 otpAttempts=20 otpWait=10
            if (args.length >= 4) {
                captchaMaxAttempts = Integer.parseInt(args[0]);
                captchaWaitSeconds = Integer.parseInt(args[1]);
                otpMaxAttempts = Integer.parseInt(args[2]);
                otpWaitSeconds = Integer.parseInt(args[3]);
            }

            System.out.println("==============================================");
            System.out.println("CAPTCHA + OTP TEST PROCESS STARTED");
            System.out.println("Captcha Max Attempts = " + captchaMaxAttempts);
            System.out.println("Captcha Wait Seconds = " + captchaWaitSeconds);
            System.out.println("OTP Max Attempts     = " + otpMaxAttempts);
            System.out.println("OTP Wait Seconds     = " + otpWaitSeconds);
            System.out.println("==============================================");

            // 1) Create captcha filename
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
            String currentTime = LocalDateTime.now().format(formatter);
            String captchaFileName = "captcha_" + currentTime + "_.png";

            System.out.println("Captcha File Name = " + captchaFileName);

            // 2) Call Captcha API
            callCaptchaAPI(captchaFileName);

            // 3) Wait for Captcha in DB
            String captchaValue = waitForCaptchaFromDB(captchaMaxAttempts, captchaWaitSeconds);
            if (captchaValue == null) {
                System.out.println(" CAPTCHA NOT FOUND IN DB. EXITING...");
                return;
            }
            System.out.println(" CAPTCHA RECEIVED FROM DB = " + captchaValue);

            // 4) Wait for OTP in DB
            String otpValue = waitForOTPFromDB(otpMaxAttempts, otpWaitSeconds);
            if (otpValue == null) {
                System.out.println(" OTP NOT FOUND IN DB. EXITING...");
                return;
            }
            System.out.println(" OTP RECEIVED FROM DB = " + otpValue);

            System.out.println("==============================================");
            System.out.println("PROCESS COMPLETED SUCCESSFULLY");
            System.out.println("==============================================");

        } catch (Exception e) {
            System.out.println(" ERROR in process: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ---------------- CAPTCHA API CALL ----------------
    public static void callCaptchaAPI(String captchaFileName) {
        try {
            String fullUrl = CAPTCHA_API + captchaFileName;

            System.out.println(""
            		+ " Calling Captcha API...");
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

    // ---------------- WAIT CAPTCHA FROM DB ----------------
    public static String waitForCaptchaFromDB(int maxAttempts, int waitSeconds) {

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            System.out.println(" Checking CAPTCHA in DB... Attempt " + attempt + "/" + maxAttempts);

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                String selectQuery =
                        "SELECT sno, captcha_text FROM captcha_table " +
                        "WHERE captcha_read_status='P' ORDER BY sno DESC LIMIT 1";

                PreparedStatement ps = conn.prepareStatement(selectQuery);
                ResultSet rs = ps.executeQuery();

                if (rs.next()) {
                    int sno = rs.getInt("sno");
                    String captcha = rs.getString("captcha_text");

                    String updateQuery =
                            "UPDATE captcha_table SET captcha_read_status='D' WHERE sno=?";
                    PreparedStatement ps2 = conn.prepareStatement(updateQuery);
                    ps2.setInt(1, sno);
                    ps2.executeUpdate();

                    System.out.println(" CAPTCHA FOUND: " + captcha + " (sno=" + sno + ")");
                    return captcha;
                }

            } catch (Exception e) {
                System.out.println(" DB Error (Captcha): " + e.getMessage());
            }

            try {
                Thread.sleep(waitSeconds * 1000L);
            } catch (InterruptedException ignored) {
            }
        }

        System.out.println(" NO CAPTCHA RECEIVED AFTER MAX ATTEMPTS!");
        return null;
    }

    // ---------------- WAIT OTP FROM DB ----------------
    public static String waitForOTPFromDB(int maxAttempts, int waitSeconds) {

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            System.out.println(" Checking OTP in DB... Attempt " + attempt + "/" + maxAttempts);

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

                String selectQuery =
                        "SELECT sno, OTP FROM otp_table WHERE otp_read_status='P' ORDER BY sno DESC LIMIT 1";

                PreparedStatement ps = conn.prepareStatement(selectQuery);
                ResultSet rs = ps.executeQuery();

                if (rs.next()) {
                    int sno = rs.getInt("sno");
                    String otp = rs.getString("OTP");

                    String updateQuery =
                            "UPDATE otp_table SET otp_read_status='D' WHERE sno=?";
                    PreparedStatement ps2 = conn.prepareStatement(updateQuery);
                    ps2.setInt(1, sno);
                    ps2.executeUpdate();

                    System.out.println(" OTP FOUND: " + otp + " (sno=" + sno + ")");
                    return otp;
                }

            } catch (Exception e) {
                System.out.println(" DB Error (OTP): " + e.getMessage());
            }

            try {
                Thread.sleep(waitSeconds * 1000L);
            } catch (InterruptedException ignored) {
            }
        }

        System.out.println(" NO OTP RECEIVED AFTER MAX ATTEMPTS!");
        return null;
    }
}
