# Data Warehousing with Redshift

In this project I built an ETL pipeline that extracts data from AWS S3, stages them in AWS Redshift, and transforms data into a set of dimensional tables for analytics, then I created a dashboard using Power BI

## Database Schema
The data consists of 5 tables. This design focuses on the songplay table (fact table).  The dimensional tables are time, users, songs, and artists help to provide context and additional details for the songplay table.

There are two staging tables **staging_events** and the **staging_songs** tables.  These tables are to temporally hold data from the S3 Bucket before being transformed and inserted into the primary use tables.

 The **staging_songs** table contains:

 | Field           | Data Type          |
  |-------------  | -------------         |
 | artist_id            | VARCHAR                    |
 | artist_latitude   | DECIMAL                   |
 | artist_location  | VARCHAR                 |
 | artist_longitude | DECIMAL                  |
 | artist_name        | VARCHAR                 |
 | duration              | DECIMAL                  |
 | num_songs         | INTEGER                   |
 | song_id               | VARCHAR                |
 | title                     | VARCHAR                 |
 | year                    | INTEGER                 |

  The **staging_events** table contains:

  | Field           | Data Type          |
   |-------------  | -------------         |
  | artist             | VARCHAR                    |
  | auth     | VARCHAR                  |
  | firstName  | VARCHAR                 |
  | gender | CHAR(1)                  |
  | itemInSession       | INTEGER                 |
  | lastName        | VARCHAR                 |
  | length            | DECIMAL                  |
  | level          | VARCHAR                |
  | location              | VARCHAR                |
  | method                    | VARCHAR                |
  | page                  | VARCHAR                 |
  | registration           | FLOAT                  |
  | sessionId          | INTEGER                   |
  | song              | VARCHAR                |
  | status                     | INTEGER            |
  | ts                  | VARCHAR               |
  | userAgent                     | VARCHAR  |
  | userId                 | INTEGER                 |



The use tables are the **songplay_fact**, **time_dim**, **user_dim**, **song_dim**, and **artist_dim** tables.  These tables are in the

 The **time table** which contains:

 | Field        | Data Type          | Key       | KEYDIST |
  |-------------  | ------------- | ------------- | ------------- |
 | start_time      | TIMESTAMP | Primary | SORTKEY |
 | hour      | INTEGER     |    | |
 | day | NUMERIC      |     | |
 | week | NUMERIC      |     | |
 | month | NUMERIC      |     | |
 | year | NUMERIC     |     | |
 | weekday | NUMERIC     |     | |

 The **users table** which contains:

 | Field        | Data Type          | Key  | KEYDIST |
 | ------------- | ------------- |  ------------- | ------------- |
 | user_id      | INTEGER | Primary | SORTKEY |
 | first_name      | VARCHAR      |    | |
 | last_name | VARCHAR      |     | |
 | gender | CHAR(1)      |     | |
 | level | VARCHAR     |     | |

 The **songs table** which contains:

 | Field        | Data Type          | Key  | KEYDIST |
 | ------------- | ------------- |  ------------- | ------------- |
 | song_id      | VARCHAR | Primary | SORTKEY |
 | title      | VARCHAR      |    | |
 | artist_id | VARCHAR      |  Foreign Key   | |
 | year | INTEGER      |     | |
 | duration | DECIMAL     |     | |

 The **artists table** which contains:

 | Field        | Data Type          | Key  | KEYDIST |
 | ------------- | ------------- |  ------------- | ------------- |
 | artist_id      | VARCHAR | Primary | DISTKEY |
 | name      | VARCHAR      |    | |
 | location | VARCHAR      |    | |
 | latitude | DECIMAL      |     | |
 | longitude | DECIMAL   |     | |

 The **songplay table** which contains:

 | Field        | Data Type          | Key  | KEYDIST |
 | ------------- | ------------- |  ------------- | ------------- |
 | songplay_id      | INTEGER | Primary | |
 | start_time      | TIMESTAMP    |  Foreign Key  | SORTKEY |
 | user_id | INTEGER  |  Foreign Key   | |
 | song_id | VARCHAR      |  Foreign Key   | |
 | artist_id | VARCHAR     |  Foreign Key   | |
 | session_id | INTEGER  |     | |
 | location | VARCHAR      |     | |
 | user_agent | VARCHAR     |     | |

<img src="images/database.PNG">