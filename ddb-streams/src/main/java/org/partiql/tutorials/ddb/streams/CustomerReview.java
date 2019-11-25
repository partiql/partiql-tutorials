package org.partiql.tutorials.ddb.streams;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.partiql.lang.eval.ExprValue;
import org.partiql.lang.eval.ExprValueFactory;

import java.util.Map;

/**
 * POJO for records in CustomerReviews DynamoDB table.
 * Uses the DynamoDB mapper (see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.html)
 */
@DynamoDBTable(tableName = "CustomerReviews")
public class CustomerReview {

    String customerId;
    String reviewId;
    String productTitle;
    int starRating;
    int helpfulVotes;
    int totalVotes;
    boolean verifiedPurchase;
    String reviewHeading;

    public CustomerReview() {
    } // needed for DynamoDBMapper#marshallToObject()

    public CustomerReview(String customerId,
                          String reviewId,
                          String productTitle,
                          int starRating,
                          int helpfulVotes,
                          int totalVotes,
                          boolean verifiedPurchase,
                          String reviewHeading) {
        this.customerId = customerId;
        this.reviewId = reviewId;
        this.productTitle = productTitle;
        this.starRating = starRating;
        this.helpfulVotes = helpfulVotes;
        this.totalVotes = totalVotes;
        this.verifiedPurchase = verifiedPurchase;
        this.reviewHeading = reviewHeading;
    }

    @DynamoDBHashKey(attributeName = "customer_id")
    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    @DynamoDBAttribute(attributeName = "review_id")
    public String getReviewId() {
        return reviewId;
    }

    public void setReviewId(String reviewId) {
        this.reviewId = reviewId;
    }

    @DynamoDBAttribute(attributeName = "product_title")
    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    @DynamoDBAttribute(attributeName = "star_rating")
    public int getStarRating() {
        return starRating;
    }

    public void setStarRating(int starRating) {
        this.starRating = starRating;
    }

    @DynamoDBAttribute(attributeName = "helpful_votes")
    public int getHelpfulVotes() {
        return helpfulVotes;
    }

    public void setHelpfulVotes(int helpfulVotes) {
        this.helpfulVotes = helpfulVotes;
    }

    @DynamoDBAttribute(attributeName = "total_votes")
    public int getTotalVotes() {
        return totalVotes;
    }

    public void setTotalVotes(int totalVotes) {
        this.totalVotes = totalVotes;
    }

    @DynamoDBAttribute(attributeName = "verified_purchase")
    public boolean isVerifiedPurchase() {
        return verifiedPurchase;
    }

    public void setVerifiedPurchase(boolean verifiedPurchase) {
        this.verifiedPurchase = verifiedPurchase;
    }

    @DynamoDBAttribute(attributeName = "review_heading")
    public String getReviewHeading() {
        return reviewHeading;
    }

    public void setReviewHeading(String reviewHeading) {
        this.reviewHeading = reviewHeading;
    }

    public void save(AmazonDynamoDB addb) {
        DynamoDBMapper mapper = new DynamoDBMapper(addb);
        mapper.save(this);
    }

    @Override
    public String toString() {
        return "CustomerReviews{" +
                "customerId='" + customerId + '\'' +
                ", reviewId='" + reviewId + '\'' +
                ", productTitle='" + productTitle + '\'' +
                ", starRating=" + starRating +
                ", helpfulVotes=" + helpfulVotes +
                ", totalVotes=" + totalVotes +
                ", verifiedPurchase=" + verifiedPurchase +
                ", reviewHeading='" + reviewHeading + '\'' +
                '}';
    }

    /**
     * Given a map of names to DynamoDB attribute values and a DynamoDB client,
     * use the mapper to return an instance of CustomerReview.
     *
     * @param itemAttributes Map of DynamoDB key to DynamoDB attribute value
     * @param mapper DynamoDB mapper
     * @return populated instance of CustomerReview
     */
    public static CustomerReview marshallIntoObject(Map<String, AttributeValue> itemAttributes, DynamoDBMapper mapper) {
        return mapper.marshallIntoObject(CustomerReview.class, itemAttributes);
    }

    /**
     * Given a PartiQL Expression Value Factory create the appropriate PartiQL value for this instance.
     *
     * @param evf PartiQL expression value factory
     * @return PartiQL value corresponding to this instance.
     */
    public ExprValue asExprValue(ExprValueFactory evf) {
        IonSystem ion = evf.getIon();
        IonStruct result = ion.newEmptyStruct();
        result.add("customer_id", ion.newString(getCustomerId()));
        result.add("review_id", ion.newString(getReviewId()));
        result.add("product_title", ion.newString(getProductTitle()));
        result.add("star_rating", ion.newInt(getStarRating()));
        result.add("helpful_votes", ion.newInt(getHelpfulVotes()));
        result.add("total_votes", ion.newInt(getTotalVotes()));
        result.add("verified_purchase", ion.newBool(isVerifiedPurchase()));
        result.add("review_heading", ion.newString(getReviewHeading()));
        return evf.newFromIonValue(result);
    }
}
