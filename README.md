### Title: Apple Weatherkit REST API -> AWS RDS Postgresql Database

#### - End to end automated data pipeline.

*To see the full codebase for this project:*
[Link to my github account](https://github.com/dcremas/awk_aws_pipeline)

#### Description:

##### A project intended to build out a postgresql database schema and then an automated data pipeline of location-hourly key weather metrics for 112 airport locations acrosss the US.  The time period of the data includes the 10 days prior, the current day, and then 9 days into the future. 

##### Purpose:

The ultimate purpose of this project was to produce a live database (refreshed every 4 hours) of by location, by hour observational weather data to be able to build interactive data visualizations of  baromtric pressure and also other key weather metrics for the purpose of informing the user of the 'current' weather patterns.

##### Database Setup Process:

- Multiple python scripts that build the database schema of the four tables needed to achieve the ultimate goal of a live data repository.

##### Data Pipeline Process:

- Utlizing the Apple Weatherkit REST API create an S3 buckey of 112 objects (.json files) that represent the US Aiport locations data for the previously specified 20 day time period.  Produce this fresh view every 4 hours.
- Build out a separted script that takes the 112 .json files, cleans and transforms the data and then uploads this new set of data to the AWS RDS postgresql database, also on the same sequence - just 15 minutes later.

##### Technologies:

1. Python and various standard library modules.
2. Apple Weatherkit REST API.
3. AWS Cloud Platform including: S3, Lambda (including Layers), RDS, EventBridge, CloudWatch
4. The Pandas and Numpy third-party packages.
5. SQLAlchemy and SQLAlchemy ORM.
6. Postgresql database.
7. Knowledge of data cleaning and tidying.
8. Advanced SQL techniques including: CTE's, Window Functions and CASE Statements for data analysis and aggregation.
