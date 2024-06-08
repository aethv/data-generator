package org.example.generator;

import org.example.utils.Utils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class UserGenerator implements IGenerator {

    private String[] firstNames;
    private String[] lastNames;
    private String[] countryCodes;
    private String[] industryNames;
    private String[] educations;

    public UserGenerator() {
    }

    @Override
    public void readUserInput() {
        System.out.println("Enter list data with comma separated. For example: data1,data2,data3,...");
        firstNames = getInputData("1/5 Enter FirstName List");
        lastNames = getInputData("2/5 Enter LastName List");
        countryCodes = getInputData("3/5 Enter Country Code List (2 character)");
        industryNames = getInputData("4/5 Enter Industry Name List");
        educations = getInputData("5/5 Enter Education List");

        int maxCount = Math.max(firstNames.length, 1)
                * Math.max(lastNames.length, 1)
                * Math.max(countryCodes.length, 1)
                * Math.max(industryNames.length, 1)
                * Math.max(educations.length, 1);
        System.out.println("Total records to be generated: " + maxCount);
        Utils.getUserInput("Press Enter to continue");
    }

    @Override
    public List<List<String>> buildData() {
        // merge country and industry into country_industry
        var countryIndustry = Arrays.stream(countryCodes)
                .map(countryCode -> Arrays.stream(industryNames)
                        .map(industryName -> Map.of(
                                "country", countryCode,
                                "industry", industryName))
                        .toList())
                .flatMap(List::stream)
                .toList();

        // merge country_industry with education into country_industry_education
        var countryIndustryEducation = countryIndustry.stream()
                .map(map -> Arrays.stream(educations)
                        .map(education -> Map.of(
                                "country", map.get("country"),
                                "industry", map.get("industry"),
                                "education", education))
                        .toList())
                .flatMap(List::stream)
                .toList();

        // merge country_industry_education with last name
        var countryIndustryEducationLastName = countryIndustryEducation.stream()
                .map(map -> Arrays.stream(lastNames)
                        .map(lastName -> Map.of(
                                "country", map.get("country"),
                                "industry", map.get("industry"),
                                "education", map.get("education"),
                                "lastName", lastName))
                        .toList())
                .flatMap(List::stream)
                .toList();

        // merge country_industry_education_lastName with first name
        var before = Instant.now().getEpochSecond();
        List<List<String>> result = null;
        int total = firstNames.length * countryIndustryEducationLastName.size();
        if (total < 10_000_000) {
            AtomicInteger counter = new AtomicInteger(0);
            result = countryIndustryEducationLastName.stream()
                    .parallel()
                    .map(map -> Arrays.stream(firstNames)
                            .map(firstName -> {
                                System.out.println("Generated: " + counter.incrementAndGet() + "/" + total);
                                return List.of(
                                        createUserName(firstName, map.get("lastName"), map.get("education"),
                                                map.get("industry"), map.get("country")),
                                        firstName,
                                        map.get("lastName"),
                                        map.get("industry"),
                                        map.get("education"),
                                        map.get("country")
                                );
                            })
                            .toList()
                    )
                    .flatMap(List::stream)
                    .toList();
        } else {
            result = buildFirstNameASync(countryIndustryEducationLastName, total);
        }

        System.out.println("generated " + (Instant.now().getEpochSecond() - before) + " seconds");
        return result;
    }

    private List<List<String>> buildFirstNameASync(List<Map<String, String>> countryIndustryEducationLastName, int total) {
        try {
            AtomicInteger counter = new AtomicInteger(0);
            var result = new ArrayList<List<String>>();
            var subListSize = countryIndustryEducationLastName.size() / 1000;
            var subLists = new ArrayList<List<Map<String, String>>>();
            for (int i = 0; i < countryIndustryEducationLastName.size(); i += subListSize) {
                int endIdx = Math.min(i + subListSize, countryIndustryEducationLastName.size());
                subLists.add(countryIndustryEducationLastName.subList(i, endIdx));
            }

            var executor = Executors.newVirtualThreadPerTaskExecutor();
            CompletionService<List<List<String>>> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < subLists.size(); i++) {
                var subList = subLists.get(i);
                completionService.submit(() -> subList.stream()
                        .map(map -> Arrays.stream(firstNames)
                                .map(firstName -> {
                                    System.out.println("Generated: " + counter.incrementAndGet() + "/" + total);
                                    return List.of(
                                            createUserName(firstName, map.get("lastName"), map.get("education"),
                                                    map.get("industry"), map.get("country")),
                                            firstName,
                                            map.get("lastName"),
                                            map.get("industry"),
                                            map.get("education"),
                                            map.get("country")
                                    );
                                })
                                .flatMap(List::stream)
                                .toList()
                        )
                        .toList()
                );
            }

            // process results
            for(int i = 0; i < subLists.size(); i++) {
                var future = completionService.take();
                var futureResult = future.get();
                result.addAll(futureResult);
            }

            return result;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException("Error generating data: " + e.getMessage());
        }
    }

    @Override
    public String getInsertStatement() {
        return "INSERT INTO Users (username, first_name, last_name, industry, education, country) VALUES (?, ?, ?, ?, ?, ?)";
    }

    private String createUserName(String... data) {
        return Arrays.stream(data)
                .map(String::toLowerCase)
                .map(s -> s.replaceAll("\\s+", "_"))
                .collect(Collectors.joining("_"));
    }
}
