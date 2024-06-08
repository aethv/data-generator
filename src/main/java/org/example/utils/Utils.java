package org.example.utils;

import java.util.Scanner;

public class Utils {

    private static final Scanner scanner = new Scanner(System.in);

    private Utils() {
    }

    public static String getUserInput(String message) {
        message += ": ";
        System.out.print(message);
        return scanner.nextLine();
    }

    public static String getUserInput(String message, String defString) {
        if (defString != null && !defString.isEmpty()) {
            message = message + " (default: " + defString + ")";
        }
        var msg = getUserInput(message);
        if (msg.isEmpty()) {
            return defString;
        }
        return msg;
    }

    public static int getUserInputInteger(String message, Integer defaultValue) {
        if (defaultValue != null) {
            message = message + " (default: " + defaultValue + ")";
        }
        message += ": ";
        System.out.print(message);
        String input = scanner.nextLine();
        if (input.isEmpty()) {
            return defaultValue;
        }
        if (input.matches("\\d+")) {
            return Integer.parseInt(input);
        } else {
            System.err.println("Invalid input");
            return -1;
        }
    }

    public static int getUserInputInteger(String message) {
        return getUserInputInteger(message, null);
    }
}
