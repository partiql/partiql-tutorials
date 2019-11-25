package org.partiql.tutorials.ddb.streams;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.partiql.lang.eval.Bindings;
import org.partiql.lang.eval.EvaluationSession;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.Expression;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CustomerReviewsNewImageUpdate extends AbstractCustomerReviews {

    private static String CUSTOMER_REVIEWS_DATA = "src/test/resources/customer_reviews.txt";


    @Test
    void reviewsWith5Stars() throws IOException {
        loadSampleData(CUSTOMER_REVIEWS_DATA);
        Iterable<ExprValue> exprVals = getRecordUpdates(StreamViewType.NEW_IMAGE);

        // PartiQL query
        String partiQLQuery =
                "SELECT s.customer_id, s.star_rating " +
                "FROM ddbstream AS s " +
                "WHERE s.star_rating = 5";

        // Compile the query
        Expression expr = pipeline.compile(partiQLQuery);

        // Use the iterable to create a PartiQL collection (value)
        // What we would typically refer to as the table in a DB
        ExprValue partiQLStream = valueFactory.newList(exprVals);

        // globals can be thought of as the DB's catalogue
        Map<String, ExprValue> globals = new HashMap<>();
        globals.put("ddbstream", partiQLStream); //  ddbstream maps to the partiQL stream

        final EvaluationSession session = EvaluationSession.builder()
                .globals(Bindings.ofMap(globals))
                .build();

        INFO("PartiQL query result is : " + expr.eval(session)); // evaluate the query!

        // DDB query
        Table table = ddb.getTable(CUSTOMER_REVIEWS);
        ScanSpec scanSpec = new ScanSpec()
                .withProjectionExpression("customer_id, star_rating")
                .withFilterExpression("star_rating = :stars")
                .withValueMap(new ValueMap().withNumber(":stars", 5));

        try {
            ItemCollection<ScanOutcome> items = table.scan(scanSpec);

            Iterator<Item> iter = items.iterator();
            while (iter.hasNext()) {
                Item item = iter.next();
                INFO(item.toJSON());
            }

        } catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
    }


}