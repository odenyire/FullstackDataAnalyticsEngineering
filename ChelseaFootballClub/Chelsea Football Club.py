# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/players/players_csv-1.txt"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/players/players_csv-1.txt"
file_type = "csv"

# CSV options
infer_schema = "true"  # Set to true to automatically detect data types
first_row_is_header = "true"  # Set to true if the first row is a header
delimiter = ","  # Comma as the delimiter

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Assuming df is already created and loaded with your CSV data

# Name of the temporary view
temp_players = "players_csv_1_txt"

# Create a temporary view
df.createOrReplaceTempView(temp_players)

# You can now run SQL queries on this temporary view
query = """
SELECT * 
FROM players_csv_1_txt
"""

# Execute the SQL query and display the results
result_df = spark.sql(query)
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp view in a SQL cell */
# MAGIC SELECT * FROM players_csv_1_txt;

# COMMAND ----------

# Name of the permanent table
permanent_players = "players_csv"

# Save the DataFrame as a Parquet table
df.write.format("parquet").saveAsTable(permanent_players)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the permanent table */
# MAGIC SELECT * FROM players_csv;

# COMMAND ----------

# File locations
matches_file_location = "/FileStore/tables/players/matches_csv.txt"
financials_file_location = "/FileStore/tables/players/players_csv-2.txt"


# File type
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# Load matches.csv into a DataFrame
matches_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(matches_file_location)

# Load financials.csv into a DataFrame
financials_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(financials_file_location)

# Save matches_df as a permanent Parquet table
matches_table_name = "matches_csv"
matches_df.write.format("parquet").saveAsTable(matches_table_name)

# Save financials_df as a permanent Parquet table
financials_table_name = "financials_csv"
financials_df.write.format("parquet").saveAsTable(financials_table_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the matches table
# MAGIC SELECT * FROM matches_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the financials table
# MAGIC SELECT * FROM financials_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the players table
# MAGIC SELECT * FROM players_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add PlayerID column to the matches_csv table
# MAGIC ALTER TABLE matches_csv ADD COLUMN PlayerID INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Modify the Date column to ensure it is of type DATE
# MAGIC ALTER TABLE matches_csv CHANGE COLUMN `Date` `Date` DATE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data into matches_csv with PlayerID
# MAGIC INSERT INTO matches_csv (`MatchID`, `Date`, `Opponent`, `Home/Away`, `Result`, `GoalsScored`, `GoalsConceded`, `PlayerID`)
# MAGIC VALUES
# MAGIC (1, DATE('2024-03-14'), 'Manchester United', 'Away', 'Win', 2, 1, 1),
# MAGIC (2, DATE('2024-03-21'), 'Liverpool', 'Home', 'Draw', 1, 1, 2),
# MAGIC (3, DATE('2024-04-05'), 'Arsenal', 'Away', 'Lose', 0, 2, 3),
# MAGIC (4, DATE('2024-04-12'), 'Manchester City', 'Home', 'Win', 3, 2, 4),
# MAGIC (5, DATE('2024-04-19'), 'Tottenham Hotspur', 'Away', 'Win', 2, 0, 5),
# MAGIC (6, DATE('2024-04-26'), 'Leicester City', 'Home', 'Win', 4, 1, 6);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the matches table
# MAGIC SELECT * FROM matches_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the financials table
# MAGIC SELECT * FROM financials_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the schema of players_csv
# MAGIC DESCRIBE players_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Inspect the schema of matches_csv
# MAGIC DESCRIBE matches_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Join players with matches to see player performance in each match
# MAGIC SELECT m.MatchID as match_id, m.Date as date, p.Name as player_name, m.GoalsScored
# MAGIC FROM matches_csv m
# MAGIC INNER JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC WHERE p.PlayerID IS NOT NULL
# MAGIC AND m.PlayerID IS NOT NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe the schema of the financials_csv table
# MAGIC DESCRIBE financials_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a column for revenue to the financials_csv table
# MAGIC ALTER TABLE financials_csv ADD COLUMNS (Revenue FLOAT);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample data into financials_csv with Revenue
# MAGIC INSERT INTO financials_csv (PlayerID, Name, Position, Nationality, Age, Appearances, Goals, Assists, Revenue)
# MAGIC VALUES
# MAGIC (1, 'Player A', 'Forward', 'Country A', 25, 30, 10, 5, 100000),
# MAGIC (2, 'Player B', 'Midfielder', 'Country B', 27, 28, 8, 12, 120000),
# MAGIC (3, 'Player C', 'Defender', 'Country C', 30, 26, 5, 4, 90000);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total revenue for the club
# MAGIC SELECT SUM(Revenue) as total_revenue
# MAGIC FROM financials_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the top goal scorers
# MAGIC SELECT p.Name as player_name, SUM(m.GoalsScored) as total_goals
# MAGIC FROM matches_csv m
# MAGIC INNER JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC GROUP BY p.Name
# MAGIC ORDER BY total_goals DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Loading the Data into Databricks
# MAGIC Historical Performance:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load historical performance data
# MAGIC CREATE OR REPLACE TEMP VIEW historical_performance_csv AS
# MAGIC SELECT *
# MAGIC FROM csv.`/FileStore/tables/players/historical_performance_csv.txt`;
# MAGIC
# MAGIC -- Verify data
# MAGIC SELECT * FROM historical_performance_csv LIMIT 10;

# COMMAND ----------

# Load historical performance data into a DataFrame
df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("/FileStore/tables/players/historical_performance_csv.txt")

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("historical_performance_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data
# MAGIC SELECT * FROM historical_performance_csv LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Managers:

# COMMAND ----------

# Load managers data into a DataFrame
managers_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("/FileStore/tables/players/managers_csv.txt")

# Register the DataFrame as a temporary view
managers_df.createOrReplaceTempView("managers_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data
# MAGIC SELECT * FROM managers_csv LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Given the schema for the managers_csv table, it looks like you have a list of managers with their start and end dates. Unfortunately, this table does not directly include performance metrics. However, if you want to analyze the performance of different managers based on historical match data, you will need to establish a relationship between the managers and matches.
# MAGIC
# MAGIC Possible Approach for Manager Performance Analysis
# MAGIC Assumption: If historical performance data is associated with managers, you would typically have a way to link matches with managers. For instance, if there is an additional column or table linking managers to specific matches, that would be ideal.
# MAGIC
# MAGIC Manually Linking Managers to Matches: If this information is missing, you might need to make some assumptions or adjustments to link managers to matches. For example, you might assume that managers were in charge during certain periods and then analyze match data accordingly.
# MAGIC
# MAGIC Example Queries
# MAGIC Assuming we have a way to link managers to matches, here’s an example of how you might analyze manager performance based on match results:
# MAGIC
# MAGIC Example 1: Total Matches and Average Goals Scored by Manager
# MAGIC If you had a table linking MatchID with ManagerID, you could write a query like this:

# COMMAND ----------

# MAGIC %md
# MAGIC Creating the match_manager_link Table
# MAGIC If you have or can obtain data to create this table, you would typically define it and populate it based on the matches and managers information you have.
# MAGIC
# MAGIC Sample SQL to Create Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE match_manager_link (
# MAGIC     MatchID INT,
# MAGIC     ManagerID INT
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Inserting Data into the Table
# MAGIC Once the table is created, you can insert the data into it. You would need to provide the data based on your matches and managers:

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO match_manager_link (MatchID, ManagerID)
# MAGIC VALUES
# MAGIC (1, 2),
# MAGIC (2, 3),
# MAGIC (3, 4)
# MAGIC -- Add more rows as needed
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC The match_manager_link table is typically used to establish a relationship between matches and managers. It helps link specific matches to the managers who were in charge during those matches. Here’s how this table would generally work:
# MAGIC
# MAGIC Structure of match_manager_link Table
# MAGIC Columns:
# MAGIC
# MAGIC MatchID: The unique identifier for each match. This would reference the MatchID in the historical_performance_csv table.
# MAGIC ManagerID: The unique identifier for each manager. This would reference the ManagerID in the managers_csv table.
# MAGIC Sample Structure:
# MAGIC
# MAGIC plaintext
# MAGIC Copy code
# MAGIC MatchID    | ManagerID
# MAGIC ----------------------
# MAGIC 1         | 2
# MAGIC 2         | 3
# MAGIC 3         | 4
# MAGIC Purpose
# MAGIC Linking Data: This table links the historical_performance_csv (which contains match performance data) with the managers_csv (which contains information about managers). This helps in understanding which manager was in charge during specific matches.
# MAGIC Analysis: It allows for detailed analysis of managerial impact. For instance, you can analyze how different managers performed in terms of match outcomes, average goals scored, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC Verifying Data
# MAGIC You can verify the data inserted into the table:

# COMMAND ----------

# MAGIC %md
# MAGIC Example Use Cases
# MAGIC Performance Analysis:
# MAGIC
# MAGIC Determine which manager had the most successful matches or the highest average goals scored.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.Name AS ManagerName, COUNT(h.MatchID) AS TotalMatches, AVG(h.GoalsScored) AS AvgGoalsScored
# MAGIC FROM historical_performance_csv h
# MAGIC JOIN match_manager_link mm ON h.MatchID = mm.MatchID
# MAGIC JOIN managers_csv m ON mm.ManagerID = m.ManagerID
# MAGIC GROUP BY m.Name
# MAGIC ORDER BY AvgGoalsScored DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Match History by Manager:
# MAGIC
# MAGIC Review all matches managed by a specific manager to analyze their performance over time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.Name AS ManagerName, h.MatchID, h.Date, h.Opponent, h.GoalsScored, h.GoalsConceded
# MAGIC FROM historical_performance_csv h
# MAGIC JOIN match_manager_link mm ON h.MatchID = mm.MatchID
# MAGIC JOIN managers_csv m ON mm.ManagerID = m.ManagerID
# MAGIC WHERE m.Name = 'Frank Lampard';

# COMMAND ----------

# MAGIC %md
# MAGIC Next Steps
# MAGIC Verify Available Tables:
# MAGIC
# MAGIC List all available tables to see if there’s any other relevant data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC Manual Analysis Based on Available Data:
# MAGIC
# MAGIC If no linking information is available, you might perform a more general analysis based on match performance and try to deduce managerial performance.
# MAGIC For example, analyzing overall team performance during different periods can be done without linking specific managers:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- General performance analysis
# MAGIC SELECT AVG(GoalsScored) AS AvgGoalsScored, AVG(GoalsConceded) AS AvgGoalsConceded
# MAGIC FROM historical_performance_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM match_manager_link LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Querying with the New Table
# MAGIC You can now use this table in your queries to join with other tables and perform analyses. For example:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT m.Name AS ManagerName, COUNT(h.MatchID) AS TotalMatches, AVG(h.GoalsScored) AS AvgGoalsScored
# MAGIC FROM historical_performance_csv h
# MAGIC JOIN match_manager_link mm ON h.MatchID = mm.MatchID
# MAGIC JOIN managers_csv m ON mm.ManagerID = m.ManagerID
# MAGIC GROUP BY m.Name
# MAGIC ORDER BY AvgGoalsScored DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Notes
# MAGIC Data Integrity: Without foreign key constraints, you’ll need to ensure the data integrity manually, ensuring that MatchID and ManagerID values in match_manager_link exist in historical_performance_csv and managers_csv, respectively.
# MAGIC Database-Specific Features: Depending on your SQL environment, there might be specific tools or features available to enforce data integrity or simulate foreign key behavior.
# MAGIC If you have any more questions or need further adjustments, feel free to ask!

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Assuming there is a table 'match_manager_link' that links matches to managers
# MAGIC SELECT m.Name AS ManagerName, COUNT(h.MatchID) AS TotalMatches, AVG(h.GoalsScored) AS AvgGoalsScored
# MAGIC FROM historical_performance_csv h
# MAGIC JOIN match_manager_link mm ON h.MatchID = mm.MatchID
# MAGIC JOIN managers_csv m ON mm.ManagerID = m.ManagerID
# MAGIC GROUP BY m.Name
# MAGIC ORDER BY AvgGoalsScored DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Example 2: Performance Over Time
# MAGIC If you want to analyze performance during specific managerial periods:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyzing average goals scored during different managerial periods
# MAGIC SELECT m.Name AS ManagerName, AVG(h.GoalsScored) AS AvgGoalsScored
# MAGIC FROM historical_performance_csv h
# MAGIC JOIN match_manager_link mm ON h.MatchID = mm.MatchID
# MAGIC JOIN managers_csv m ON mm.ManagerID = m.ManagerID
# MAGIC WHERE h.Date BETWEEN m.StartDate AND COALESCE(m.EndDate, CURRENT_DATE())
# MAGIC GROUP BY m.Name
# MAGIC ORDER BY AvgGoalsScored DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Investors:

# COMMAND ----------

# Load investors data into a DataFrame
investors_df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ",").csv("/FileStore/tables/players/investors_csv.txt")

# Register the DataFrame as a temporary view
investors_df.createOrReplaceTempView("investors_csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the Data
# MAGIC
# MAGIC Use SQL to query the data:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify data
# MAGIC SELECT * FROM investors_csv LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Here are the SQL INSERT statements to add 20 more players, additional revenue data, and 20 more matches to your tables.
# MAGIC
# MAGIC 1. Insert 20 More Players into players_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert 20 more players into players_csv
# MAGIC INSERT INTO players_csv (PlayerID, Name, Position, Nationality, Age, Appearances, Goals)
# MAGIC VALUES
# MAGIC (8, 'Marcus Rashford', 'Forward', 'England', 26, 30, 12),
# MAGIC (9, 'Jadon Sancho', 'Forward', 'England', 24, 28, 8),
# MAGIC (10, 'Bruno Fernandes', 'Midfielder', 'Portugal', 29, 32, 15),
# MAGIC (11, 'Paul Pogba', 'Midfielder', 'France', 31, 27, 6),
# MAGIC (12, 'N’Golo Kanté', 'Midfielder', 'France', 33, 25, 3),
# MAGIC (13, 'Riyad Mahrez', 'Forward', 'Algeria', 32, 22, 10),
# MAGIC (14, 'Edouard Mendy', 'Goalkeeper', 'Senegal', 32, 20, 0),
# MAGIC (15, 'Christian Pulisic', 'Forward', 'USA', 25, 26, 7),
# MAGIC (16, 'Kalidou Koulibaly', 'Defender', 'Senegal', 32, 24, 2),
# MAGIC (17, 'Sergio Ramos', 'Defender', 'Spain', 37, 21, 5),
# MAGIC (18, 'Virgil van Dijk', 'Defender', 'Netherlands', 32, 30, 6),
# MAGIC (19, 'Joshua Kimmich', 'Midfielder', 'Germany', 29, 33, 8),
# MAGIC (20, 'Manuel Neuer', 'Goalkeeper', 'Germany', 38, 22, 0),
# MAGIC (21, 'Frenkie de Jong', 'Midfielder', 'Netherlands', 26, 27, 5),
# MAGIC (22, 'Leroy Sané', 'Forward', 'Germany', 28, 28, 12),
# MAGIC (23, 'Thomas Müller', 'Forward', 'Germany', 34, 29, 10),
# MAGIC (24, 'Kylian Mbappé', 'Forward', 'France', 25, 30, 20),
# MAGIC (25, 'Neymar Jr.', 'Forward', 'Brazil', 32, 25, 14),
# MAGIC (26, 'Gianluigi Donnarumma', 'Goalkeeper', 'Italy', 25, 23, 0),
# MAGIC (27, 'Paulo Dybala', 'Forward', 'Argentina', 30, 22, 9),
# MAGIC (28, 'Karim Benzema', 'Forward', 'France', 36, 20, 15),
# MAGIC (29, 'João Cancelo', 'Defender', 'Portugal', 30, 26, 3),
# MAGIC (30, 'Mason Mount', 'Midfielder', 'England', 25, 27, 6);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Insert Additional Revenue Data into financials_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert additional revenue data into financials_csv
# MAGIC INSERT INTO financials_csv (PlayerID, Name, Position, Nationality, Age, Appearances, Goals, Assists)
# MAGIC VALUES
# MAGIC (8, 'Marcus Rashford', 'Forward', 'England', 26, 100, 20, 10),
# MAGIC (9, 'Jadon Sancho', 'Forward', 'England', 24, 90, 15, 8),
# MAGIC (10, 'Bruno Fernandes', 'Midfielder', 'Portugal', 29, 120, 25, 15),
# MAGIC (11, 'Paul Pogba', 'Midfielder', 'France', 31, 80, 12, 10),
# MAGIC (12, 'N’Golo Kanté', 'Midfielder', 'France', 33, 85, 10, 7),
# MAGIC (13, 'Riyad Mahrez', 'Forward', 'Algeria', 32, 95, 18, 6),
# MAGIC (14, 'Edouard Mendy', 'Goalkeeper', 'Senegal', 31, 50, 0, 0),
# MAGIC (15, 'Christian Pulisic', 'Forward', 'USA', 25, 75, 14, 5),
# MAGIC (16, 'Kalidou Koulibaly', 'Defender', 'Senegal', 32, 60, 2, 1),
# MAGIC (17, 'Sergio Ramos', 'Defender', 'Spain', 37, 70, 5, 2),
# MAGIC (18, 'Virgil van Dijk', 'Defender', 'Netherlands', 33, 65, 3, 1),
# MAGIC (19, 'Joshua Kimmich', 'Midfielder', 'Germany', 29, 85, 8, 15),
# MAGIC (20, 'Manuel Neuer', 'Goalkeeper', 'Germany', 38, 50, 0, 0),
# MAGIC (21, 'Frenkie de Jong', 'Midfielder', 'Netherlands', 26, 90, 12, 14),
# MAGIC (22, 'Leroy Sané', 'Forward', 'Germany', 28, 80, 16, 9),
# MAGIC (23, 'Thomas Müller', 'Forward', 'Germany', 34, 75, 14, 11),
# MAGIC (24, 'Kylian Mbappé', 'Forward', 'France', 25, 100, 30, 12),
# MAGIC (25, 'Neymar Jr.', 'Forward', 'Brazil', 32, 85, 22, 11),
# MAGIC (26, 'Gianluigi Donnarumma', 'Goalkeeper', 'Italy', 25, 50, 0, 0),
# MAGIC (27, 'Paulo Dybala', 'Forward', 'Argentina', 30, 90, 20, 8),
# MAGIC (28, 'Karim Benzema', 'Forward', 'France', 36, 95, 25, 10),
# MAGIC (29, 'João Cancelo', 'Defender', 'Portugal', 30, 85, 3, 6),
# MAGIC (30, 'Mason Mount', 'Midfielder', 'England', 25, 70, 10, 12);

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Insert 20 More Matches into matches_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert 20 more matches into matches_csv
# MAGIC INSERT INTO matches_csv (MatchID, Date, Opponent, `Home/Away`, Result, GoalsScored, GoalsConceded, PlayerID)
# MAGIC VALUES
# MAGIC (7, CAST('2024-03-05' AS DATE), 'Fulham', 'Home', 'Win', 3, 1, 8),
# MAGIC (8, CAST('2024-03-12' AS DATE), 'Bournemouth', 'Away', 'Draw', 2, 2, 9),
# MAGIC (9, CAST('2024-03-19' AS DATE), 'Nottingham Forest', 'Home', 'Win', 1, 0, 10),
# MAGIC (10, CAST('2024-03-26' AS DATE), 'West Ham', 'Away', 'Lose', 0, 2, 11),
# MAGIC (11, CAST('2024-04-02' AS DATE), 'Newcastle', 'Home', 'Win', 4, 0, 12),
# MAGIC (12, CAST('2024-04-09' AS DATE), 'Brentford', 'Away', 'Draw', 1, 1, 13),
# MAGIC (13, CAST('2024-04-16' AS DATE), 'Everton', 'Home', 'Win', 2, 0, 14),
# MAGIC (14, CAST('2024-04-23' AS DATE), 'Sheffield United', 'Away', 'Win', 2, 1, 15),
# MAGIC (15, CAST('2024-04-30' AS DATE), 'Wolverhampton', 'Home', 'Draw', 1, 1, 16),
# MAGIC (16, CAST('2024-05-07' AS DATE), 'Burnley', 'Away', 'Win', 3, 0, 17),
# MAGIC (17, CAST('2024-05-14' AS DATE), 'Aston Villa', 'Home', 'Win', 2, 1, 18),
# MAGIC (18, CAST('2024-05-21' AS DATE), 'Manchester City', 'Away', 'Lose', 1, 2, 19),
# MAGIC (19, CAST('2024-06-01' AS DATE), 'Liverpool', 'Home', 'Draw', 1, 1, 20),
# MAGIC (20, CAST('2024-06-08' AS DATE), 'Arsenal', 'Away', 'Win', 3, 2, 8),
# MAGIC (21, CAST('2024-06-15' AS DATE), 'Chelsea', 'Home', 'Draw', 0, 0, 9),
# MAGIC (22, CAST('2024-06-22' AS DATE), 'Tottenham Hotspur', 'Away', 'Win', 2, 1, 10),
# MAGIC (23, CAST('2024-07-01' AS DATE), 'Leicester City', 'Home', 'Win', 4, 1, 11),
# MAGIC (24, CAST('2024-07-08' AS DATE), 'Crystal Palace', 'Away', 'Win', 2, 1, 12),
# MAGIC (25, CAST('2024-07-15' AS DATE), 'Brentford', 'Home', 'Draw', 1, 1, 13),
# MAGIC (26, CAST('2024-07-22' AS DATE), 'Everton', 'Away', 'Win', 3, 0, 14),
# MAGIC (27, CAST('2024-08-01' AS DATE), 'Nottingham Forest', 'Home', 'Win', 2, 0, 15),
# MAGIC (28, CAST('2024-08-08' AS DATE), 'West Ham', 'Away', 'Lose', 1, 3, 16),
# MAGIC (29, CAST('2024-08-15' AS DATE), 'Newcastle', 'Home', 'Draw', 1, 1, 17),
# MAGIC (30, CAST('2024-08-22' AS DATE), 'Wolverhampton', 'Away', 'Win', 2, 1, 18);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC With your datasets loaded, you can start performing various analyses. Here are a few ideas for analysis based on the data:
# MAGIC
# MAGIC Player Performance Analysis
# MAGIC
# MAGIC Top Goal Scorers: Identify the top players based on the number of goals scored.
# MAGIC Player Contributions: Analyze contributions (goals, assists) from different players in various matches.
# MAGIC Match Performance Analysis
# MAGIC
# MAGIC Match Results: Analyze win/loss/draw ratios for different opponents.
# MAGIC Home vs. Away Performance: Compare performance at home versus away games.
# MAGIC Revenue Analysis
# MAGIC
# MAGIC Total Revenue: Calculate total revenue from all sources.
# MAGIC Revenue Trends: Analyze trends over time if the data includes date information.
# MAGIC Manager Performance Analysis
# MAGIC
# MAGIC Manager Impact: Assess how different managers have impacted team performance over time.
# MAGIC Investor Analysis
# MAGIC
# MAGIC Investor Contributions: Analyze contributions or engagements from various investors.
# MAGIC Historical Performance Trends
# MAGIC
# MAGIC Game-by-Game Performance: Examine performance trends over historical games to identify patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC Sample Queries
# MAGIC Top Goal Scorers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.Name AS player_name, SUM(m.GoalsScored) AS total_goals
# MAGIC FROM matches_csv m
# MAGIC INNER JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC GROUP BY p.Name
# MAGIC ORDER BY total_goals DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Match Results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Opponent, COUNT(*) AS num_matches, 
# MAGIC        SUM(CASE WHEN Result = 'Win' THEN 1 ELSE 0 END) AS wins,
# MAGIC        SUM(CASE WHEN Result = 'Draw' THEN 1 ELSE 0 END) AS draws,
# MAGIC        SUM(CASE WHEN Result = 'Lose' THEN 1 ELSE 0 END) AS losses
# MAGIC FROM matches_csv
# MAGIC GROUP BY Opponent
# MAGIC ORDER BY wins DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Revenue Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(revenue) AS total_revenue
# MAGIC FROM financials_csv;

# COMMAND ----------

# MAGIC %md
# MAGIC Manager Impact

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE historical_performance_csv

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like the historical_performance_csv table does not include columns for manager names or performance scores. Based on the available columns, here’s what you can do to proceed with analysis related to historical performance:
# MAGIC
# MAGIC If Manager Information is in Another Table: If you have manager information in a different table, you'll need to join that table with the historical performance data to perform any analysis.
# MAGIC
# MAGIC Calculate Performance Metrics from Available Data: If you want to analyze performance metrics using the available columns, you can aggregate data like goals scored or conceded.
# MAGIC
# MAGIC Here’s an example of how you might aggregate the performance data:
# MAGIC
# MAGIC Example 1: Analyzing Total Goals Scored and Conceded

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total goals scored and conceded in historical matches
# MAGIC SELECT SUM(GoalsScored) AS total_goals_scored, SUM(GoalsConceded) AS total_goals_conceded
# MAGIC FROM historical_performance_csv;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Example 2: Performance by Opponent

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate average goals scored and conceded against each opponent
# MAGIC SELECT Opponent, AVG(GoalsScored) AS avg_goals_scored, AVG(GoalsConceded) AS avg_goals_conceded
# MAGIC FROM historical_performance_csv
# MAGIC GROUP BY Opponent
# MAGIC ORDER BY avg_goals_scored DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Example 3: Performance at Home vs. Away

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare average goals scored and conceded at home vs. away
# MAGIC SELECT `Home/Away`, AVG(GoalsScored) AS avg_goals_scored, AVG(GoalsConceded) AS avg_goals_conceded
# MAGIC FROM historical_performance_csv
# MAGIC GROUP BY `Home/Away`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Investor Contributions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate total contribution from each investor
# MAGIC SELECT Name AS investor_name, SUM(InvestmentAmount) AS total_contribution
# MAGIC FROM investors_csv
# MAGIC GROUP BY Name
# MAGIC ORDER BY total_contribution DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC What if now you see all the tables and the sample data on those tables, then we do some visualizations, and later also do some modeling, and some serious reports with lots of JOINs.
# MAGIC We also create AGGs and load data, we create FACT tables and load data, create some procedures to send email alerts and do scheduled runs to load some tables, we also do monitoring of the environments using procedures and python scripts, we create some views and TEMP tables among other stuffs?

# COMMAND ----------

# MAGIC %md
# MAGIC That sounds like a comprehensive data analysis and management plan! Here’s a broad outline of how you could tackle each part of the project:
# MAGIC
# MAGIC 1. Explore Tables and Sample Data
# MAGIC View Table Structures:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC DESCRIBE <table_name>;
# MAGIC Sample Data:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC SELECT * FROM <table_name> LIMIT 10;
# MAGIC 2. Visualizations
# MAGIC Tools:
# MAGIC
# MAGIC Matplotlib/Seaborn in Python
# MAGIC Tableau or Power BI for interactive dashboards
# MAGIC Examples:
# MAGIC
# MAGIC Bar Charts: Total goals per player
# MAGIC Line Charts: Trends in goals scored over time
# MAGIC Pie Charts: Distribution of match results (Win/Draw/Lose)
# MAGIC 3. Data Modeling
# MAGIC Create Fact Tables:
# MAGIC
# MAGIC Example Fact Table: match_performance_fact
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE TABLE match_performance_fact (
# MAGIC     MatchID INT,
# MAGIC     Date DATE,
# MAGIC     GoalsScored INT,
# MAGIC     GoalsConceded INT,
# MAGIC     PlayerID INT
# MAGIC );
# MAGIC Create Dimension Tables:
# MAGIC
# MAGIC Player Dimension:
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE TABLE player_dim (
# MAGIC     PlayerID INT PRIMARY KEY,
# MAGIC     Name STRING,
# MAGIC     Position STRING,
# MAGIC     Nationality STRING,
# MAGIC     Age INT
# MAGIC );
# MAGIC 4. Aggregations
# MAGIC Example Aggregation:
# MAGIC sql
# MAGIC Copy code
# MAGIC SELECT PlayerID, SUM(GoalsScored) AS TotalGoals
# MAGIC FROM matches_csv
# MAGIC GROUP BY PlayerID;
# MAGIC 5. Procedures and Scheduled Jobs
# MAGIC Send Email Alerts:
# MAGIC
# MAGIC Use Python scripts with smtplib or a database procedure to send emails when certain conditions are met.
# MAGIC Scheduled Data Loads:
# MAGIC
# MAGIC Using SQL:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE PROCEDURE load_match_data()
# MAGIC BEGIN
# MAGIC     -- SQL to load data
# MAGIC END;
# MAGIC Scheduled Jobs:
# MAGIC
# MAGIC Using cron jobs (for Unix-like systems) or Task Scheduler (for Windows) to run the procedure at regular intervals.
# MAGIC 6. Monitoring
# MAGIC Environment Monitoring:
# MAGIC Python Scripts: Use libraries like psutil to monitor system performance.
# MAGIC Database Monitoring: Use database-specific tools to monitor performance metrics.
# MAGIC 7. Views and TEMP Tables
# MAGIC Create Views:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE VIEW player_performance AS
# MAGIC SELECT PlayerID, SUM(GoalsScored) AS TotalGoals
# MAGIC FROM matches_csv
# MAGIC GROUP BY PlayerID;
# MAGIC Create TEMP Tables:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE TEMPORARY TABLE temp_match_stats AS
# MAGIC SELECT * FROM matches_csv;
# MAGIC 8. Reporting
# MAGIC Complex Queries:
# MAGIC Join Operations: Combine data from multiple tables to create comprehensive reports.
# MAGIC Example:
# MAGIC sql
# MAGIC Copy code
# MAGIC SELECT p.Name, m.Date, m.GoalsScored
# MAGIC FROM matches_csv m
# MAGIC JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC WHERE m.Date BETWEEN '2024-01-01' AND '2024-12-31';
# MAGIC Example Workflow
# MAGIC Extract Data: Load data from CSVs or databases.
# MAGIC Transform Data: Clean and preprocess data.
# MAGIC Load Data: Insert into fact and dimension tables.
# MAGIC Visualize Data: Create charts and dashboards.
# MAGIC Analyze Data: Run aggregations and join tables.
# MAGIC Automate: Set up procedures and schedules.
# MAGIC Monitor: Track system and query performance.
# MAGIC Report: Generate and share insights.
# MAGIC Feel free to adjust these steps based on your specific needs and environment. If you have any particular requirements or questions, let me know!

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC It looks like you have several tables and temporary views available. Here's a summary of the tables and views:
# MAGIC
# MAGIC Tables
# MAGIC financials_csv
# MAGIC match_manager_link
# MAGIC matches_csv
# MAGIC players_csv
# MAGIC Temporary Views
# MAGIC historical_performance_csv
# MAGIC investors_csv
# MAGIC managers_csv
# MAGIC players_csv_1_txt
# MAGIC Next Steps
# MAGIC 1. Data Exploration
# MAGIC View Table Schema and Sample Data:
# MAGIC sql
# MAGIC Copy code
# MAGIC DESCRIBE financials_csv;
# MAGIC SELECT * FROM financials_csv LIMIT 10;
# MAGIC
# MAGIC DESCRIBE match_manager_link;
# MAGIC SELECT * FROM match_manager_link LIMIT 10;
# MAGIC
# MAGIC DESCRIBE matches_csv;
# MAGIC SELECT * FROM matches_csv LIMIT 10;
# MAGIC
# MAGIC DESCRIBE players_csv;
# MAGIC SELECT * FROM players_csv LIMIT 10;
# MAGIC
# MAGIC DESCRIBE historical_performance_csv;
# MAGIC SELECT * FROM historical_performance_csv LIMIT 10;
# MAGIC
# MAGIC DESCRIBE investors_csv;
# MAGIC SELECT * FROM investors_csv LIMIT 10;
# MAGIC
# MAGIC DESCRIBE managers_csv;
# MAGIC SELECT * FROM managers_csv LIMIT 10;
# MAGIC
# MAGIC DESCRIBE players_csv_1_txt;
# MAGIC SELECT * FROM players_csv_1_txt LIMIT 10;
# MAGIC 2. Data Analysis and Visualization
# MAGIC Aggregate and Analyze Data:
# MAGIC
# MAGIC Example: Calculate total goals by player
# MAGIC sql
# MAGIC Copy code
# MAGIC SELECT p.Name AS PlayerName, SUM(m.GoalsScored) AS TotalGoals
# MAGIC FROM matches_csv m
# MAGIC JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC GROUP BY p.Name
# MAGIC ORDER BY TotalGoals DESC;
# MAGIC Visualize Data:
# MAGIC
# MAGIC Use tools like Matplotlib, Seaborn, Tableau, or Power BI to create charts and dashboards.
# MAGIC 3. Data Modeling
# MAGIC Create Fact and Dimension Tables:
# MAGIC Example Fact Table:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE TABLE match_performance_fact (
# MAGIC     MatchID INT,
# MAGIC     Date DATE,
# MAGIC     GoalsScored INT,
# MAGIC     GoalsConceded INT,
# MAGIC     PlayerID INT
# MAGIC );
# MAGIC Example Dimension Table:
# MAGIC
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE TABLE player_dim (
# MAGIC     PlayerID INT PRIMARY KEY,
# MAGIC     Name STRING,
# MAGIC     Position STRING,
# MAGIC     Nationality STRING,
# MAGIC     Age INT
# MAGIC );
# MAGIC 4. Procedures and Scheduling
# MAGIC Create Procedures:
# MAGIC
# MAGIC Example:
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE PROCEDURE load_match_data()
# MAGIC BEGIN
# MAGIC     -- SQL to load data
# MAGIC END;
# MAGIC Schedule Jobs:
# MAGIC
# MAGIC Use cron jobs or Task Scheduler to automate data loading and other tasks.
# MAGIC 5. Monitoring and Alerts
# MAGIC Create Monitoring Scripts:
# MAGIC
# MAGIC Use Python with libraries like psutil for system monitoring and SQL for database performance.
# MAGIC Send Alerts:
# MAGIC
# MAGIC Use Python scripts or database procedures to send email alerts based on certain conditions.
# MAGIC 6. Reporting
# MAGIC Create Complex Queries:
# MAGIC
# MAGIC Example:
# MAGIC sql
# MAGIC Copy code
# MAGIC SELECT p.Name AS PlayerName, m.Date, m.GoalsScored
# MAGIC FROM matches_csv m
# MAGIC JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC WHERE m.Date BETWEEN '2024-01-01' AND '2024-12-31';
# MAGIC Generate Reports:
# MAGIC
# MAGIC Use Tableau, Power BI, or custom scripts to generate and share reports.
# MAGIC Feel free to dive into any of these areas based on your priorities and let me know if you need help with specific tasks or queries!

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Data Exploration
# MAGIC View Table Schema and Sample Data:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE financials_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM financials_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE match_manager_link;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM match_manager_link LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE matches_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM matches_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE players_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM players_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE historical_performance_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM historical_performance_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE investors_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM investors_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE managers_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM managers_csv LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE players_csv_1_txt;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM players_csv_1_txt LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Data Analysis and Visualization
# MAGIC Aggregate and Analyze Data:
# MAGIC
# MAGIC Example: Calculate total goals by player

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.Name AS PlayerName, SUM(m.GoalsScored) AS TotalGoals
# MAGIC FROM matches_csv m
# MAGIC JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC GROUP BY p.Name
# MAGIC ORDER BY TotalGoals DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Visualize Data:
# MAGIC
# MAGIC Use tools like Matplotlib, Seaborn, Tableau, or Power BI to create charts and dashboards.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Data Modeling
# MAGIC Create Fact and Dimension Tables:
# MAGIC Example Fact Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE fct_match_performance (
# MAGIC     MatchID INT,
# MAGIC     Date DATE,
# MAGIC     GoalsScored INT,
# MAGIC     GoalsConceded INT,
# MAGIC     PlayerID INT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Example Dimension Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dim_player (
# MAGIC     PlayerID INT,
# MAGIC     Name STRING,
# MAGIC     Position STRING,
# MAGIC     Nationality STRING,
# MAGIC     Age INT
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Procedures and Scheduling
# MAGIC Create Procedures:
# MAGIC
# MAGIC Example:
# MAGIC sql
# MAGIC Copy code
# MAGIC CREATE PROCEDURE load_match_data()
# MAGIC BEGIN
# MAGIC     -- SQL to load data
# MAGIC END;
# MAGIC Schedule Jobs:
# MAGIC
# MAGIC Use cron jobs or Task Scheduler to automate data loading and other tasks.
# MAGIC 5. Monitoring and Alerts
# MAGIC Create Monitoring Scripts:
# MAGIC
# MAGIC Use Python with libraries like psutil for system monitoring and SQL for database performance.
# MAGIC Send Alerts:
# MAGIC
# MAGIC Use Python scripts or database procedures to send email alerts based on certain conditions.
# MAGIC 6. Reporting
# MAGIC Create Complex Queries:
# MAGIC
# MAGIC Example:
# MAGIC sql
# MAGIC Copy code
# MAGIC SELECT p.Name AS PlayerName, m.Date, m.GoalsScored
# MAGIC FROM matches_csv m
# MAGIC JOIN players_csv p ON m.PlayerID = p.PlayerID
# MAGIC WHERE m.Date BETWEEN '2024-01-01' AND '2024-12-31';
# MAGIC Generate Reports:
# MAGIC
# MAGIC Use Tableau, Power BI, or custom scripts to generate and share reports.
# MAGIC Feel free to dive into any of these areas based on your priorities and let me know if you need help with specific tasks or queries!

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
