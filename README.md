# End_to_End_ETL_Project_Snowflake

🏏 Cricket Data ETL & Analytics on Snowflake

📖 Project Overview

This project demonstrates a complete end-to-end data engineering and analytics pipeline built on Snowflake, using real-world cricket match data from [Cricsheet.org](https://cricsheet.org/matches/).
Each JSON file represents a single match with detailed ball-by-ball data. The project extracts, transforms, and loads this data into Snowflake, builds analytical fact and dimension tables, and visualizes match insights through a Snowflake dashboard.  



🚀 Key Objectives
- Build an automated ETL pipeline in Snowflake for structured and semi-structured data.
- Design a scalable data model for cricket analytics (dimensional schema).
- Leverage Snowflake-native automation (Snowpipe, Streams, Tasks) for real-time ingestion.
- Derive rich insights like player performance, match summaries, and powerplay analysis.  



🧩 Architecture Overview
1. Data Source – Ball-by-ball cricket match JSONs from Cricsheet.org.
2. Data Ingestion – Files uploaded to AWS S3 and integrated with Snowflake via External Stage.
3. Snowpipe – Automatically loads new JSON files from S3 into staging tables in Snowflake.
4. Streams + Tasks – Track incremental data and trigger scheduled transformations.
5. Transformations (ETL) – Create structured dimension and fact tables:
    - dim_team
    - dim_player
    - fact_match_info
    - fact_match_innings
6. Analytics Layer – Dashboard built within Snowflake showing:
    - Match outcomes and winning margins
    - Total matches, runs and wickets
    - Filtered on year, format and event  



🧠 Key Learnings
- Handling semi-structured JSON data with VARIANT, FLATTEN, and LATERAL JOIN.
- Designing incremental ETL logic with MERGE statements and surrogate keys.
- Implementing fully automated data ingestion using Snowflake-native tools (no external ETL).
- Building a clean dimensional model optimized for analytical queries.
- Visualizing data directly in Snowflake dashboards for quick insights.  


  
🛠️ Tech Stack
- Snowflake – Data warehouse, transformations, automation
- AWS S3 – Data lake storage for raw JSON files
- Snowpipe, Streams, Tasks – Continuous ingestion and orchestration
- SQL / JSON Processing – Data modeling and transformation
- Snowflake Dashboards – Data visualization layer  

  
📸 Dashboard Preview
<img width="3252" height="1718" alt="image" src="https://github.com/user-attachments/assets/470b85ff-d168-4b2b-842f-52e40be6ed96" />
