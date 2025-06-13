-- Table 1: Reviews
CREATE OR REPLACE TABLE yelp_reviews (review_text variant) --col review_text is semistructured data type (variant) to hold json

COPY INTO yelp_reviews --COPY INTO loads data from S3 bucket files into yelp_reviews table
FROM 's3://alorae/yelp/'
CREDENTIALS = (
    AWS_KEY_ID = 'omitted'
    AWS_SECRET_KEY = 'omitted'
)
FILE_FORMAT = (TYPE = JSON)

SELECT * FROM yelp_reviews limit 100

CREATE OR REPLACE TABLE tbl_yelp_reviews as
SELECT review_text:business_id::string as business_id,
       review_text:date::date as review_date, 
       review_text:stars::integer as review_stars, 
       review_text:text::string as review_text,
       sentiment_category(review_text) as sentiment_cat,
       sentiment_polarity(review_text) as sentiment_val,
       review_text:user_id::string as review_user_id, 
FROM yelp_reviews

SELECT * FROM tbl_yelp_reviews limit 20

-- Table 2: Businesses
CREATE OR REPLACE TABLE yelp_businesses (business_text variant)

COPY INTO yelp_businesses 
FROM 's3://alorae/yelp/yelp_academic_dataset_business.json'
CREDENTIALS = (
    AWS_KEY_ID = omitted
    AWS_SECRET_KEY = omitted
)
FILE_FORMAT = (TYPE = JSON)

CREATE OR REPLACE TABLE tbl_yelp_businesses as
SELECT business_text:business_id::string as business_id,
       business_text:name::string as business_name,
       business_text:categories::string as categories, 
       business_text:city::string as city, 
       business_text:state::string as state, 
       business_text:review_count::integer as review_count, 
FROM yelp_businesses

select * from tbl_yelp_businesses limit 20

-- Table 3: Users
CREATE OR REPLACE TABLE yelp_users (user_text variant)

COPY INTO yelp_users 
FROM 's3://alorae/yelp/split_user/'
CREDENTIALS = (
    AWS_KEY_ID = 'omitted'
    AWS_SECRET_KEY = 'omitted'
)
FILE_FORMAT = (TYPE = JSON)



CREATE OR REPLACE TABLE tbl_yelp_users AS
SELECT 
    user_text:average_stars::FLOAT AS average_stars,
    user_text:cool::INT AS cool,
    user_text:fans::INT AS fans,
    user_text:funny::INT AS funny,
    user_text:name::STRING AS name,
    user_text:review_count::INT AS review_count,
    user_text:useful::INT AS useful,
    user_text:user_id::STRING AS user_id,
    user_text:yelping_since::DATE AS yelping_since
FROM yelp_users;


SELECT * FROM tbl_yelp_users limit 20

-- Table 4: Checkins 
CREATE OR REPLACE TABLE yelp_checkins (checkin_data variant)

COPY INTO yelp_checkins 
FROM 's3://alorae/yelp/yelp_academic_dataset_checkin.json'
CREDENTIALS = (
    AWS_KEY_ID = 'omitted'
    AWS_SECRET_KEY = 'omitted'
)
FILE_FORMAT = (TYPE = JSON)

CREATE OR REPLACE TABLE tbl_yelp_checkins AS
SELECT
    checkin_data:business_id::STRING AS business_id,
    checkin_data:date::STRING AS checkins -- stores an array of dates
FROM yelp_checkins;
    
SELECT * FROM tbl_yelp_checkins limit 20

-- UDF 1: Sentiment Category
CREATE OR REPLACE FUNCTION sentiment_category(text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('vaderSentiment')
HANDLER = 'get_sentiment_category'
AS $$
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

def get_sentiment_category(text):
    if not text:
        return 'Neutral'
    score = analyzer.polarity_scores(text)['compound']
    if score >= 0.05:
        return 'Positive'
    elif score <= -0.05:
        return 'Negative'
    else:
        return 'Neutral'
$$;


-- UDF 2: Sentiment Polarity Score
CREATE OR REPLACE FUNCTION sentiment_polarity(text STRING)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('vaderSentiment')
HANDLER = 'get_polarity_score'
AS $$
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

def get_polarity_score(text):
    if not text:
        return 0.0
    return analyzer.polarity_scores(text)['compound']
$$;

-- Query 1: Sentiment, Avg Stars, and Review Count Trend Over Time
CREATE OR REPLACE TABLE tbl_ratings_time AS
SELECT
    DATE_TRUNC('month', review_date) AS month,
    ROUND(AVG(sentiment_val), 3) AS avg_sentiment,
    ROUND(AVG(review_stars), 2) AS avg_stars,
    COUNT(*) AS review_count
FROM tbl_yelp_reviews
GROUP BY 1
ORDER BY 1;

-- Query 2: Star Rating vs Sentiment Score
CREATE OR REPLACE TABLE tbl_star_sentiment AS
SELECT
    ROUND(review_stars) AS star_bucket,
    ROUND(sentiment_val, 1) AS sentiment_bucket,
    COUNT(*) AS review_count,
    AVG(sentiment_val) AS avg_sentiment
FROM tbl_yelp_reviews
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT * FROM tbl_star_sentiment

-- Query 3: States with Highest and Lowest average of Star Reviews
CREATE OR REPLACE TABLE tbl_high_low_star_ratings AS
SELECT
    b.state,
    COUNT(*) AS total_reviews,
    ROUND(AVG(r.review_stars), 2) AS avg_star_rating
FROM tbl_yelp_reviews r
LEFT JOIN tbl_yelp_businesses b ON r.business_id = b.business_id
WHERE b.state != 'XMS'
GROUP BY b.state
-- HAVING total_reviews >= 100
ORDER BY avg_star_rating DESC;
SELECT * FROM tbl_high_low_star_ratings

-- Query 4: Sentiment drift of users over time
-- Identify top 5 users by total review count
CREATE OR REPLACE TABLE tbl_top5_user_monthly_sentiment AS
SELECT
    r.review_user_id,
    DATE_TRUNC('month', r.review_date) AS review_month,
    ROUND(AVG(r.sentiment_val), 3) AS avg_sentiment,
    COUNT(*) AS monthly_reviews
FROM tbl_yelp_reviews r
JOIN (
    SELECT
        review_user_id,
        COUNT(*) AS total_reviews
    FROM tbl_yelp_reviews
    GROUP BY review_user_id
    ORDER BY total_reviews DESC
    LIMIT 5
) AS top_users
ON r.review_user_id = top_users.review_user_id
GROUP BY r.review_user_id, review_month
ORDER BY review_month, r.review_user_id;

SELECT * FROM tbl_top5_user_monthly_sentiment limit 50

-- Query 5: Top 10 cities by average sentiment rating
CREATE OR REPLACE TABLE tbl_top10_cities_sentiment AS
SELECT 
    CONCAT(b.city, ', ', b.state) AS location,
    AVG(r.sentiment_val) as average_sentiment,
    COUNT(*) AS total_reviews
FROM tbl_yelp_reviews r
LEFT JOIN tbl_yelp_businesses b ON b.business_id = r.business_id
GROUP BY location
ORDER BY average_sentiment DESC
LIMIT 10;

SELECT * FROM tbl_top10_cities_sentiment

-- Query 6: Top 10 cities by average star rating
CREATE OR REPLACE TABLE tbl_top10_cities_stars AS
SELECT 
    CONCAT(b.city, ', ', b.state) AS location,
    AVG(r.review_stars) as average_stars,
    COUNT(*) AS total_reviews
FROM tbl_yelp_reviews r
LEFT JOIN tbl_yelp_businesses b ON b.business_id = r.business_id
GROUP BY location
ORDER BY average_stars DESC
LIMIT 10;

SELECT * FROM tbl_top10_cities_stars

-- Query 7: Do businesses with more checkins have higher star and sentiment ratings?
CREATE OR REPLACE TABLE tbl_checkins_vs_sentiment_stars AS
SELECT
  FLOOR(c.checkin_count / 1000) * 1000 AS checkin_bin,
  COUNT(DISTINCT c.business_id) AS business_count,
  ROUND(AVG(r.review_stars), 2) AS avg_star_rating,
  ROUND(AVG(r.sentiment_val), 3) AS avg_sentiment
FROM (
    SELECT
      business_id,
      LENGTH(checkins) - LENGTH(REPLACE(checkins, ',', '')) + 1 AS checkin_count
    FROM tbl_yelp_checkins
    WHERE checkins IS NOT NULL
) c
JOIN tbl_yelp_reviews r
  ON c.business_id = r.business_id
GROUP BY checkin_bin
ORDER BY checkin_bin;

SELECT * FROM tbl_checkins_vs_sentiment_stars

-- Query 8: Average review count, star ratings, and sentiment values
CREATE OR REPLACE TABLE tbl_business_averages_totals AS
SELECT
  ROUND(AVG(review_stars), 2) AS avg_star_rating_per_user,
  ROUND(AVG(sentiment_val), 3) AS avg_sentiment_score_per_user,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT review_user_id), 2) AS avg_reviews_per_user,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT business_id), 2) AS avg_reviews_per_business,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT review_stars), 2) AS avg_stars_per_business,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT sentiment_val), 2) AS avg_sentiment_per_business,
  COUNT(*) AS total_reviews,
  (SELECT COUNT(*) FROM tbl_yelp_users) AS total_users,
  (SELECT COUNT(*) FROM tbl_yelp_businesses) AS total_businesses
FROM tbl_yelp_reviews;

SELECT * FROM tbl_business_averages_totals limit 10

-- Query 9: Review Count by location
CREATE OR REPLACE TABLE tbl_city_review_volume AS
SELECT
  b.city,
  b.state,
  CONCAT(b.city, ', ', b.state) AS location,
  COUNT(*) AS total_reviews
FROM tbl_yelp_reviews r
JOIN tbl_yelp_businesses b ON r.business_id = b.business_id
GROUP BY b.city, b.state
ORDER BY total_reviews DESC;
