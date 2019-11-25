# Sample Data for Testing 

This folder contains data used for testing. 

## Customer Reviews (`customer_reviews.txt`)

The file contains customer review data obtained from the [Registry of Open
Data on AWS](https://registry.opendata.aws/amazon-reviews/). The data
has been modified, we removed some columns and some rows, specifically,
the columns in [customer_reviews.txt](customer_reviews.txt) are


1. `customer_id`
1. `review_id`
1. `product_title`
1. `star_rating`
1. `helpful_votes`
1. `total_votes`
1. `verified_purchase`
1. `review_headline`

We have also removed 

* the first line (header line) in the original tsv file
* removed rows to decrease the size of the data set 

## Customer Reviews Updates (`customer_reviews_update.txt`)

This is a file we create. The file contains similar lines as `customer_reviews.txt` but with some values altered. 
We use this file as an example of updates to existing records in our DynamoDB table. 