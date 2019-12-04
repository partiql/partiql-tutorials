package org.partiql.tutorials.ddb.streams.utils;


import org.junit.jupiter.api.Test;
import org.partiql.tutorials.ddb.streams.CustomerReview;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class to read data from TSV file.
 */
public class CustomerReviewsTsvFileUtils {

    private static String TSV_FILE_PATH = "src/test/resources/customer_reviews.txt";


    public static List<CustomerReview> getCustomerReviews(String path) throws IOException {
        List<CustomerReview> customerReviews = new ArrayList<>();

        Files.lines(Paths.get(path)).forEach(
                line -> {
                    String[] tsvRow = line.split("\t");
                    customerReviews.add(new CustomerReview(tsvRow[0],
                            tsvRow[1],
                            tsvRow[2],
                            Integer.valueOf(tsvRow[3]),
                            Integer.valueOf(tsvRow[4]),
                            Integer.valueOf(tsvRow[5]),
                            parseYorN(tsvRow[6]),
                            tsvRow[7]));
                });
        return customerReviews;
    }



    private static boolean parseYorN(String s) {
        String yOrN = s.trim();
        return ("Y".equals(yOrN) || "y".equals(yOrN));
    }

    @Test
    public void readTsv() throws IOException {
        List<CustomerReview> customerReviews = getCustomerReviews(TSV_FILE_PATH);
        System.out.println(customerReviews.size());
        System.out.println(customerReviews);
    }
}
