package org.example;

import org.example.generator.IGenerator;
import org.example.generator.UserGenerator;
import org.example.utils.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class App {

    private static final int BATCH_SIZE = 1000;

    public App() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Error loading MySQL JDBC Driver");
        }
        System.out.println("--------------------");
        System.out.println("---Data Generator---");
        System.out.println("--------------------");
    }

    public static void main(String[] args) {
        App app = new App();

        System.out.println("This will create 3 table Users, Books, Orders, please ensure they are not existed before");
        Connection connection = app.getConnectionFromInput();
        app.startApp(connection);
    }

    public void startApp(Connection connection) {
        int selection = getSelection();
        if (selection == 1) {
            startGeneration(connection, new UserGenerator(), "Users");
        } else if (selection == 2) {
//            startGeneration(connection, new BookGenerator());
        } else if (selection == 3) {
//            startGeneration(connection, new OrderGenerator());
        } else if (selection == 4) {
            System.out.println("Exiting...");
            System.exit(0);
        } else {
            System.out.println("Invalid selection. Please try again.");
        }
        startApp(connection);
    }

    private void startGeneration(Connection connection, IGenerator generator, String generationType) {
        var data = generator.generate();
        var insertStatement = generator.getInsertStatement();
        try {
            var preparedStatement = connection.prepareStatement(insertStatement);
            connection.setAutoCommit(false);
            var count = 0;
            var totalRecord = 0;
            for (var list : data) {
                for (int i = 0; i < list.size(); i++) {
                    preparedStatement.setObject(i + 1, list.get(i));
                }
                preparedStatement.addBatch();
                if (++count % BATCH_SIZE == 0) {
                    preparedStatement.executeBatch();
                    connection.commit();
                }
                totalRecord++;
            }
            preparedStatement.executeBatch();
            connection.commit();
            System.out.println(generationType + " generated successfully! (" + totalRecord + " records)\n\n");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Error generating data of " + generationType + ": " + e.getMessage());
        }
    }

    private int getSelection() {
        System.out.println("Select an action:");
        System.out.println("1. Generate Users");
        System.out.println("2. Generate Books");
        System.out.println("3. Generate Orders");
        System.out.println("4. Exit");
        int selected = Utils.getUserInputInteger("Enter your selection", 4);
        if (selected < 1 || selected > 4) {
            System.err.println("Invalid input");
            return getSelection();
        }
        return selected;
    }

    private Connection getConnectionFromInput() {
        try {
            String url = Utils.getUserInput("Enter JDC url", "jdbc:mysql://localhost:3308/ecom");
            String username = Utils.getUserInput("Enter username", "root");
            String password = Utils.getUserInput("Enter password", "123456");
            Connection connection = DriverManager.getConnection(url, username, password);
            Statement statement = connection.createStatement();
            statement.executeQuery("SELECT VERSION()");
            statement.close();
            System.out.println("Connected to database!");
            return connection;
        } catch (Exception e) {
            throw new RuntimeException("Error connecting to database: " + e.getMessage());
        }
    }
}
