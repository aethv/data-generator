package org.example.generator;

import org.example.utils.Utils;

import java.util.List;

public interface IGenerator {

    default List<List<String>> generate() {
        readUserInput();
        return buildData();
    }

    void readUserInput();

    List<List<String>> buildData();

    String getInsertStatement();

    default String[] getInputData(String message) {
        String input = Utils.getUserInput(message);
        return input.split(",");
    }
}
