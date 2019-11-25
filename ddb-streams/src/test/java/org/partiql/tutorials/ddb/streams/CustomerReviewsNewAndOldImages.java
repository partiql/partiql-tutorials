package org.partiql.tutorials.ddb.streams;

import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.google.common.collect.Lists;

import kotlin.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.partiql.lang.eval.Bindings;
import org.partiql.lang.eval.EvaluationSession;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.Expression;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CustomerReviewsNewAndOldImages extends AbstractCustomerReviews {


    public static final int LINE_LENGTH = 80;
    private static String CUSTOMER_REVIEWS_DATA = "src/test/resources/customer_reviews.txt";
    private static String CUSTOMER_REVIEWS_UPDATES = "src/test/resources/customer_reviews_updates.txt";


    @Test
    void reviewsWith5Stars() throws IOException {
        loadSampleData(CUSTOMER_REVIEWS_DATA);
        loadSampleData(CUSTOMER_REVIEWS_UPDATES);
        Iterable<ExprValue> newImagesExprVals = getRecordUpdates(StreamViewType.NEW_IMAGE);
        Iterable<ExprValue> oldImagesExprVals = getRecordUpdates(StreamViewType.OLD_IMAGE);
        ArrayList<ExprValue> lnew = Lists.newArrayList(newImagesExprVals); // JOIN needs to re-iterate the list, iterators are read once
        ArrayList<ExprValue> lold = Lists.newArrayList(oldImagesExprVals);


        // PartiQL query
        String q1 = "SELECT o.customer_id AS id FROM oldImages AS o";
        String q1_filterNulls = "SELECT o.customer_id AS id FROM oldImages AS o WHERE o.customer_id IS NOT NULL";

        String q2 = "SELECT n.customer_id AS id FROM newImages AS n";
        String q3 = "SELECT n.customer_id AS nid, " +
                "o.customer_id AS oid, " +
                "n.star_rating AS nstar, " +
                "o.star_rating AS ostar " +
                "FROM newImages AS n JOIN oldImages AS o ON o.customer_id = n.customer_id " +
                "WHERE n.star_rating > o.star_rating";

        List<String> queries = Lists.newArrayList(q1, q1_filterNulls, q2, q3);

        // Compile the queries
        Stream<Pair<String, Expression>> compiledQueries =
                queries.stream().map(q -> new Pair<>(q, pipeline.compile(q)));

        // Use the iterable to create a PartiQL collection (value)
        // What we would typically refer to as the table in a DB
        ExprValue oldImagesPartiQL = valueFactory.newList(lold);
        ExprValue newImagesPartiQL = valueFactory.newList(lnew);

//        Flip to these lines and see what happens.
//        ExprValue newImagesPartiQL = valueFactory.newList(newImagesExprVals);
//        ExprValue oldImagesPartiQL = valueFactory.newList(oldImagesExprVals);

        // globals can be thought of as the DB's catalogue
        Map<String, ExprValue> globals = new HashMap<>();
        globals.put("newImages", newImagesPartiQL);
        globals.put("oldImages", oldImagesPartiQL);

        final EvaluationSession session = EvaluationSession.builder()
                .globals(Bindings.ofMap(globals))
                .build();


        compiledQueries.forEach(cq -> {
            System.out.println(cq.component1() + "\n\t => \n" + cq.component2().eval(session));
            printHL();
        });
    }

    private void printHL() {
        for(int i = 0; i < LINE_LENGTH; i++) System.out.print("-");
        System.out.println();
    }

    @Override
    protected StreamSpecification getStreamSpecification() {
        return new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
    }
}
