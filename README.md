# Yelp Review Dashboard

<img width="985" alt="Screenshot 2025-06-12 at 10 34 32 PM" src="https://github.com/user-attachments/assets/d0baaa8f-7688-467e-b8b8-ac1a929c0f7b" />


## 🚀 Project Overview
An end-to-end data analytics project using real-world Yelp review data (5+ GB) to uncover trends in user sentiment, business ratings, and geographic patterns over time.

This project explores how review sentiment and star ratings evolve over time and vary across regions and users. Using a full cloud-native stack, we extract and analyze over a million reviews to reveal behavioral and regional insights about Yelp usage.

## 💡 Notable Insights

- **Rating Bias by Volume**: States with high star ratings often had <50 reviews and outperformed states with >300k reviews, skewing leaderboard-style comparisons
- **Divergence Between Text Reviews and Star Rating**: Review sentiment dropped 27% while star ratings rose from 2010-2021, showing sentiment ≠ rating
- **Pandemic Dip**: Both sentiment and star ratings dropped in 2021 before partial recovery
- **Sample Imbalance**: Most reviews collected came from 10 hot spots, missing major cities along the west and east cost (i.e.-Seattle, Washington D.C., New York City)

## 🧰 Tech Stack

- **Python** (file splitting, S3 upload, UDFs)
- **AWS S3** (data lake for raw JSON ingestion)
- **Snowflake** (data warehouse + SQL analytics)
- **Power BI** (interactive dashboard)
- **VADER Sentiment Analysis** via Snowflake Python UDF

## 📊 Data Pipeline

1. Split 5GB JSON dataset into 10 files using Python for parallel upload and faster S3 ingestion
2. Upload to AWS S3 and load into Snowflake via external staging
3. Create structured tables for reviews, businesses, users, check-ins
4. Apply sentiment analysis UDFs in SQL using Python and VADER
5. Build a Power BI dashboard on top of cleaned Snowflake tables

## 🧾 Schema Design

| Table               | Description                              |
|--------------------|------------------------------------------|
| `tbl_yelp_reviews` | Parsed reviews + sentiment & star scores |
| `tbl_yelp_businesses` | Business metadata (city, state, category) |
| `tbl_yelp_users`   | Reviewer history & fan metrics           |
| `tbl_checkins`     | Business-level visit frequency            |
