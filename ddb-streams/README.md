# PartiQL and DynamoDB Streams 

The tutorial shows how to interface PartiQL with user defined data and
use PartiQL queries over user defined data. 

In this tutorial we use data from the [Registry of Open Data on
AWS](https://registry.opendata.aws/amazon-reviews/) to seed a DynamoDB
table and configure [DynamoDB
Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
on that table. We then interface PartiQL with the user defined POJOs
that capture rows from the DynamoDB table and evaluate PartiQL queries
over our data.


## PartiQL implementation overview 

The evaluator follows a typical flow comprised of phases

[Parser and Compiler Diagram](https://github.com/therapon/partiql-lang-kotlin/blob/2eb7ea4613062aa7b1baeaa875bb911b03f7a4d6/docs/dev/img/parser-compiler.png)

### Interfacing with the Evaluator 

Evaluation inside PartiQL relies on the type
[`ExprValue`](https://github.com/therapon/partiql-lang-kotlin/blob/2eb7ea4613062aa7b1baeaa875bb911b03f7a4d6/lang/src/org/partiql/lang/eval/ExprValue.kt#L23).
Evaluation proceeds by manipulating values of type `ExprValue` and
maintaining a map of *bindings*. A binding is an association between a
PartiQL variable (name) and a value, for example when evaluation
starts it is given a map of bindings that maps global names (akin to
tables in other database systems). 

[ExprValue Class Diagram](https://github.com/therapon/partiql-lang-kotlin/blob/2eb7ea4613062aa7b1baeaa875bb911b03f7a4d6/docs/dev/img/expr-value-class.png)


## Tutorial Setup 

The tutorial is self contained. This repository consists of Java code and
JUnit tests. The tests use a local DynamoDB instance and thus do not require 
an AWS account. 

### Test Setup 

As part of each test's setup we get 

1. a [local DynamoDB](src/test/java/org/partiql/tutorials/ddb/streams/AbstractCustomerReviews.java#L44) instance
1. a [new DynamoDB table](src/test/java/org/partiql/tutorials/ddb/streams/AbstractCustomerReviews.java#L57) called `CustomerReviews` [configured with DynamoDB Streams](src/test/java/org/partiql/tutorials/ddb/streams/AbstractCustomerReviews.java#L62) 
1. [seed](src/test/java/org/partiql/tutorials/ddb/streams/AbstractCustomerReviews.java#L181)
   the `CustomerReviews` table with some customer review [data](src/test/resources/customer_reviews.txt)

The `CustomerReviews` DynamoDB table has the following structure 

| `customer_id` | `review_id` | `product_title` | `star_rating` | `helpful_votes` | `total_votes` | `verified_purchase` | `review_headline` |
|---------------|-------------|-----------------|---------------|-----------------|---------------|---------------------|-------------------|
|               |             |                 |               |                 |               |                     |                   |


### Mapping DynamoDB rows to POJO

The DynamoDB library provides a
[Mapper](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.html)
and we use it for our
[`CustomerReview`](src/main/java/org/partiql/tutorials/ddb/streams/CustomerReview.java#L17)
POJO.

We also provide a mapping from our `CustomerReview` object to an
`ExprValue` object through
[`CustomerReview#asExprValue()`](src/main/java/org/partiql/tutorials/ddb/streams/CustomerReview.java#L159)
method. The method `asExprValue()` maps an instance of `CustomerReview` to an *structure* where 

* each field's name is mapped to a *key*
* each field's value is associated to the corresponding key created in the preceding step

For example, a `CustomerReview` instance 

| cr: CustomerReview      |
|-------------------------|
| customerId       = "1"  |
| reviewId         = "2"  |
| productTitle     = "t"  |
| starRating       =  5   |
| helpfulVotes     =  6   |
| totalVotes       =  7   |
| verifiedPurchase = true |
| reviewHeading    = "r"  |

Maps to 

``` json
{
 "customer_id"      : "1",
 "review_id"        : "2",
 "product_title"    : "t",
 "star_rating"      :  5,
 "helpful_votes"    :  6,
 "total_votes"      :  7,
 "verified_purchase":  1,    // true -> 1, false -> 0 
 "review_heading"   : "r",
} 
```

### Running a PartiQL Query 

Let's walk through a simple [test](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L30). 

1. first we [load](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L31) our customer reviews data from a TSV file 
1. we obtain an [iterator](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L32) from the DynamoDB Stream 
   * `getRecordUpdate` can capture the updated record (`StreamViewType.NEW_IMAGE`) or the old record (`StreamViewType.OLD_IMAGE`)
1. we define the PartiQL [query](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L35) we want to evaluate as a string. 
1. we [compile](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L41) the query
1. we [wrap](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L45) our iterator of `ExprVal` into a PartiQL collection (a list) allowing iteration over the data by the PartiQL evaluator 
1. we [create](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L48) a map and [add](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L49) the name `dbbstream` bound to our PartiQL list we created 
1. using the map we created in the previous step we then create a `Session` and use our map to create our [global bindings](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L51)
1. finally we [evaluate our query](src/test/java/org/partiql/tutorials/ddb/streams/CustomerReviewsNewImageUpdate.java#L55) under the global bindings and print the result
