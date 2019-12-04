package org.partiql.tutorials.ddb.streams;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.partiql.lang.CompilerPipeline;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.ExprValueFactory;
import org.partiql.tutorials.ddb.streams.utils.AwsDynamoDbLocalTestUtils;
import org.partiql.tutorials.ddb.streams.utils.CustomerReviewsTsvFileUtils;

import java.io.IOException;
import java.util.*;

public class AbstractCustomerReviews {

    private static final IonSystem ION = IonSystemBuilder.standard().build();
    protected static String CUSTOMER_REVIEWS = "CustomerReviews";
    protected final CompilerPipeline pipeline = CompilerPipeline.standard(ION);
    protected DynamoDB ddb;
    protected ExprValueFactory valueFactory = pipeline.getValueFactory();
    private AmazonDynamoDBLocal localDynamoDB;
    private AmazonDynamoDB ddbClient;
    private AmazonDynamoDBStreams streamsClient;
    private String streamArn;
    private DescribeTableResult describeTable;
    private DynamoDBMapper mapper;

    protected static void INFO(String... msg) {
        System.out.println("[INFO] " + String.join(" ", msg));
    }


    @BeforeAll
    public void setUp() {
        AwsDynamoDbLocalTestUtils.initSqLite();
        localDynamoDB = DynamoDBEmbedded.create();
        ddbClient = localDynamoDB.amazonDynamoDB();
        ddb = new DynamoDB(ddbClient);
    }

    @AfterAll
    public void tearDown() {
        ddbClient.shutdown();
    }

    @BeforeEach
    public void testSetUp() {
        createTable(CUSTOMER_REVIEWS);
        describeTable = ddbClient.describeTable(CUSTOMER_REVIEWS);
        streamArn = describeTable.getTable().getLatestStreamArn();
        INFO("ARN :", streamArn);
        StreamSpecification streamSpec = describeTable.getTable().getStreamSpecification();
        streamsClient = localDynamoDB.amazonDynamoDBStreams();
        mapper = new DynamoDBMapper(ddbClient);
    }

    @AfterEach
    public void testTearDown() {
        deleteTable(CUSTOMER_REVIEWS);
    }

    /**
     * Grab all records from all shards and turn them into ExprValues.
     *
     * @param viewType stream view type NEW_IMAGE or OLD_IMAGE used to select the image from within the record.
     * @return iterator over #newImage() from each record in the stream
     */
    protected Iterable<ExprValue> getRecordUpdates(StreamViewType viewType) {
        Deque<String> shardsQ = getShardIds();
        Iterator<ExprValue> it = makeIterator(shardsQ, viewType);
        return () -> it;
    }

    protected Iterator<ExprValue> makeIterator(Deque<String> shardsQ,
                                               StreamViewType viewType) {
        if (StreamViewType.NEW_IMAGE == viewType) {
            return new NewImageExprValueIterator(shardsQ);
        } else if (StreamViewType.OLD_IMAGE == viewType) {
            return new OldImageExprValueIterator(shardsQ);
        } else {
            throw new IllegalStateException("Only accept NEW_IMAGE or OLD_IMAGE for view type");
        }
    }

    /**
     * Gets all the shard Ids as a Queue.
     * See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.LowLevel.Walkthrough.html
     *
     * @return queue of all shard Ids
     */
    private Deque<String> getShardIds() {
        // get the starting shard iterators--probably insufficient if the topology of shards change...
        Deque<String> shardIters = new ArrayDeque<>();
        String lastEvaluatedShardId = null;
        do {
            DescribeStreamResult describeStreamResult = streamsClient.describeStream(
                    new DescribeStreamRequest()
                            .withStreamArn(streamArn)
                            .withExclusiveStartShardId(lastEvaluatedShardId));
            List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

            for (Shard shard : shards) {
                String shardId = shard.getShardId();
                GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                        .withStreamArn(streamArn)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
                GetShardIteratorResult getShardIteratorResult =
                        streamsClient.getShardIterator(getShardIteratorRequest);
                shardIters.add(getShardIteratorResult.getShardIterator());
            }

            lastEvaluatedShardId = describeStreamResult.getStreamDescription().getLastEvaluatedShardId();

        } while (lastEvaluatedShardId != null);
        INFO("Found " + shardIters.size() + " starting shard iterators");
        if (shardIters.isEmpty()) {
            throw new IllegalStateException("No stream shards");
        } else {
            return shardIters;
        }
    }

    /**
     * Create the DynamoDB table programmatically.
     * See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Java.01.html
     * @param tableName DynamoDB table name
     */
    private void createTable(String tableName) {

        try {

            List<KeySchemaElement> keySchema = Collections.singletonList(new KeySchemaElement()
                    .withAttributeName("customer_id").
                            withKeyType(KeyType.HASH));

            List<AttributeDefinition> attributeDefinitions = Collections.singletonList(new AttributeDefinition()
                    .withAttributeName("customer_id")
                    .withAttributeType("S"));

            // streaming
            StreamSpecification streamSpec = getStreamSpecification();

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(10L)
                            .withWriteCapacityUnits((long) 5))
                    .withStreamSpecification(streamSpec);

            INFO("Issuing CreateTable request for", tableName);
            TableUtils.createTableIfNotExists(ddbClient, request);
            TableUtils.waitUntilActive(ddbClient, CUSTOMER_REVIEWS);
        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    protected StreamSpecification getStreamSpecification() {
        return new StreamSpecification()
                .withStreamEnabled(true)
                .withStreamViewType(StreamViewType.NEW_IMAGE);
    }

    protected void loadSampleData(String path) throws IOException {
        List<CustomerReview> customerReviews = CustomerReviewsTsvFileUtils.getCustomerReviews(path);
        customerReviews.stream().forEach(cr -> mapper.save(cr)); // save to dynamoDB using mapper.
    }

    private void deleteTable(String tableName) {
        TableUtils.deleteTableIfExists(ddbClient, new DeleteTableRequest().withTableName(tableName));
    }


    protected class OldImageExprValueIterator extends NewImageExprValueIterator {

        public OldImageExprValueIterator(Deque<String> shardsQ) {
            super(shardsQ);
        }

        @Override
        public ExprValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Iterator exhausted");
            }

            // wrap the next record into a value
            Map<String, AttributeValue> oldImage = currRecords.removeFirst().getDynamodb().getOldImage();
            if (oldImage == null) {
                return valueFactory.getNullValue();
            } else {
                return CustomerReview.marshallIntoObject(oldImage, mapper).asExprValue(valueFactory);
            }
        }
    }
    protected class NewImageExprValueIterator implements Iterator<ExprValue> {

        private final Deque<String> shardsQ;
        Deque<Record> currRecords;

        public NewImageExprValueIterator(Deque<String> shardsQ) {
            this.shardsQ = shardsQ;
            currRecords = new ArrayDeque<>();
        }

        @Override
        public boolean hasNext() {
            Set<String> emptyIters = new HashSet<>();
            while (currRecords.isEmpty()) {
                if (emptyIters.size() == shardsQ.size()) {
                    // all iterators previous returned nothing so we end the iterator
                    return false;
                }

                String currShardIter = shardsQ.removeFirst();

                GetRecordsResult getRecordsResult = streamsClient.getRecords(new GetRecordsRequest()
                        .withShardIterator(currShardIter));
                List<Record> records = getRecordsResult.getRecords();
                INFO("Found " + records.size() + " records");
                if (!records.isEmpty()) {
                    // remove current shard iterator from the empty list as we got something
                    emptyIters.remove(currShardIter);
                }
                currRecords.addAll(records);
                String nextShardIter = getRecordsResult.getNextShardIterator();
                if (nextShardIter != null) {
                    shardsQ.addLast(nextShardIter);
                }
                if (records.isEmpty()) {
                    // note that the next iter came from an empty result
                    emptyIters.add(nextShardIter);
                }
                if (shardsQ.isEmpty()) {
                    // exhausted all of the iterators (closed)
                    return false;
                }
            }

            return true;
        }

        @Override
        public ExprValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Iterator exhausted");
            }

            // wrap the next record into a value
            Map<String, AttributeValue> newImage = currRecords.removeFirst().getDynamodb().getNewImage();
            return CustomerReview.marshallIntoObject(newImage, mapper).asExprValue(valueFactory);
        }
    }
}
