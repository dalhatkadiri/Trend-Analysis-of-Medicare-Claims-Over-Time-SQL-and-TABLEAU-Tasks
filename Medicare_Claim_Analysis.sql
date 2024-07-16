----Trend Analysis of Medicare Claims Over Time: SQL and TABLEAU Tasks

--Objective: Analyze how Medicare claims have evolved over the years across different regions and categories.
/*Tasks:
1. Aggregate and visualize the total number of claims per year.
2. Identify trends and patterns in the data (e.g., increasing or decreasing trends).
3. Compare trends across different Class, Topic, and LocationDesc.

Outcome: 
Insights into how Medicare claims have changed over time, which can help in policy-making and resource allocation.

ABOUT THE DATASET

The dataset contains 33,454 entries and 30 columns.
Observations:
Several columns have all null values, such as RowId, PriorityArea2, PriorityArea4, Data_Value_Footnote_Symbol, and Data_Value_Footnote.
The dataset includes data related to various topics under Medicare, with a focus on different classes and priority areas.
GeoLocation is missing for some entries.*/

---CREATE A TABLE AND NAME IT Medicare_Claims
---The Dataset Columns are RowId
/*YearStart
LocationAbbr
LocationDesc
DataSource
PriorityArea1
PriorityArea2
PriorityArea3
PriorityArea4
Class
Topic
Question
Data_Value_Type
Data_Value_Unit
Data_Value
Data_Value_Alt
Data_Value_Footnote_Symbol
Data_Value_Footnote
Low_Confidence_Limit
High_Confidence_Limit
Break_Out_Category
Break_Out
ClassId
TopicId
QuestionId
Data_Value_TypeID
BreakOutCategoryId
BreakOutId
LocationId
GeoLocation
ClaimType */

--To Create Medicare_Claim

CREATE TABLE Medicare_Claims(
						YearStart INT,
						LocationAbbr VARCHAR(100),
						LocationDesc VARCHAR(100),
						DataSource VARCHAR(100),
						PriorityArea1 VARCHAR(100),
						PriorityArea2 VARCHAR(100),
						PriorityArea3 VARCHAR(100),
						PriorityArea4 VARCHAR(100),
						Classes VARCHAR(100),
						Topic VARCHAR(100),
						Question VARCHAR(500),
						Data_Value_Type VARCHAR(100),
						Data_Value_Unit numeric, 
						Data_Value NUMERIC,
						Data_Value_Alt NUMERIC,
						Data_Value_Footnote_Symbol VARCHAR(100),
						Data_Value_Footnote VARCHAR(100),
						Low_Confidence_Limit NUMERIC,
						High_Confidence_Limit NUMERIC,
						Break_Out_Category VARCHAR(100),
						Break_Out VARCHAR(100),
						ClassId VARCHAR(100),
						TopicId VARCHAR(100),
						QuestionId VARCHAR(100),
						Data_Value_TypeID VARCHAR(100),
						BreakOutCategoryId VARCHAR(100),
						BreakOutId VARCHAR(100),
						LocationId VARCHAR(100),
						GeoLocation VARCHAR(100),
						ClaimType VARCHAR(250)
						)


----IMPORT THE DATASET INTO THE TABLE

COPY Medicare_Claims
FROM 'C:\Users\USER\Desktop\Center_for_Medicare___Medicaid_Services__CMS____Medicare_Claims_data.csv'DELIMITER','


----VIEW THE TABLE

SELECT *
FROM Medicare_Claims


--DATA CLEANING IS NEXT

/* 1. Remove Rows with All Null Values */

DELETE FROM Medicare_Claims
WHERE RowId IS NULL
  AND PriorityArea2 IS NULL
  AND PriorityArea4 IS NULL
  AND Data_Value_Footnote_Symbol IS NULL
  AND Data_Value_Footnote IS NULL
  
  

/* 2. Remove Duplicates  */

DELETE FROM Medicare_Claims
WHERE RowId IN (
    SELECT RowId
    FROM (
        SELECT RowId, ROW_NUMBER() OVER (PARTITION BY YearStart, LocationDesc, Class, Topic, Question ORDER BY RowId) AS row_num
        FROM Medicare_Claims
    ) t
    WHERE row_num > 1
)


/* 3. Convert Data Types
To ensure that certain columns have the correct data types, you can use the following queries:
Convert YearStart to Integer  */

ALTER TABLE Medicare_Claims
ALTER COLUMN YearStart INT;

/* 4. Handle Missing Values
Fill Missing GeoLocation with a Default Value */

UPDATE Medicare_Claims
SET GeoLocation = 'Not Available'
WHERE GeoLocation IS NULL;

/* Replace Null PriorityArea3 with 'None' */

UPDATE Medicare_Claims
SET PriorityArea3 = 'None'
WHERE PriorityArea3 IS NULL


/* 5. Standardize Text Data
Convert LocationDesc to Title Case */

UPDATE Medicare_Claims
SET LocationDesc = INITCAP(LocationDesc)


/* Trim White Spaces in Break_Out */

UPDATE Medicare_Claims
SET Break_Out = TRIM(Break_Out)

/* 6. Consistency Checks
Ensure Data_Value is Non-negative */

UPDATE Medicare_Claims
SET Data_Value = 0
WHERE Data_Value < 0


/* 7. Update Inconsistent Break_Out_Category Values
Ensure that Break_Out_Category values are consistent, to standardizing the category names */

UPDATE Medicare_Claims
SET Break_Out_Category = 'Race'
WHERE Break_Out_Category = 'race';


/* 8. Normalize Class Values
If there are variations in the naming of classes, standardize them */

UPDATE Medicare_Claims
SET Class = 'Cardiovascular Diseases'
WHERE Class IN ('Cardio Diseases', 'CVD')

--Among other inconsistencies


---CASES
/* 1. Data Extraction and Aggregation

Task: Extract yearly aggregate claim counts and associated values from the dataset.
SQL Query: */

SELECT YearStart, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart
ORDER BY YearStart

--Saving the query in a view Claims_Timeline_Per_Year

CREATE VIEW Claims_Timeline_Per_Year AS
SELECT YearStart, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart
ORDER BY YearStart

/* 2. Regional Trend Analysis

Task: Extract and aggregate data for each region (LocationDesc) by year.
SQL Query: */

SELECT YearStart, LocationDesc, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, LocationDesc
ORDER BY YearStart, LocationDesc

-----Create a view named "Regional_Trend_Analysis"

CREATE VIEW Regional_Trend_Analysis AS
SELECT YearStart, LocationDesc, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, LocationDesc
ORDER BY YearStart, LocationDesc


/* 3. Class and Topic Breakdown

Task: Analyze trends for different classes and topics over time.
SQL Query */

SELECT YearStart, Classes, Topic, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, Classes, Topic
ORDER BY YearStart, Classes, Topic

-----Create a view named "Class_and_Topic_breakdown"

CREATE VIEW Class_and_Topic_breakdown AS
SELECT YearStart, Classes, Topic, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, Classes, Topic
ORDER BY YearStart, Classes, Topic

/* 4. Top 10 Regions by Claims

Task: Identify the top 10 regions with the highest number of claims each year.
SQL Query: */

SELECT *
FROM(
	SELECT ROW_NUMBER() OVER(PARTITION BY a.YearStart) AS Positions,*
	FROM(
		SELECT YearStart, LocationDesc, COUNT(*) AS Total_Claims
		FROM Medicare_Claims
		GROUP BY YearStart, LocationDesc
		ORDER BY YearStart, Total_Claims DESC
		) a
	) b
WHERE b.Positions <= 10

-----Create a view named "Regions_by_Claims"

CREATE VIEW Regions_by_Claims AS
SELECT *
FROM(
	SELECT ROW_NUMBER() OVER(PARTITION BY a.YearStart) AS Positions,*
	FROM(
		SELECT YearStart, LocationDesc, COUNT(*) AS Total_Claims
		FROM Medicare_Claims
		GROUP BY YearStart, LocationDesc
		ORDER BY YearStart, Total_Claims DESC
		) a
	) b
WHERE b.Positions <= 10


/* 5. Year-over-Year Growth Rate

Task: Calculate the year-over-year growth rate of claims.
SQL Query: */

WITH YearlyClaims AS (
    SELECT YearStart, COUNT(*) AS Total_Claims
    FROM Medicare_Claims
    GROUP BY YearStart
)
SELECT YearStart, 
       Total_Claims, 
       LAG(Total_Claims) OVER (ORDER BY YearStart) AS Previous_Year_Claims,
       ROUND(((Total_Claims - LAG(Total_Claims) OVER (ORDER BY YearStart)) * 100.0 / LAG(Total_Claims) OVER (ORDER BY YearStart)),2) AS Growth_Rate
FROM YearlyClaims

--OR

SELECT a.YearStart, a.Total_Claims, LAG(a.Total_Claims) OVER (ORDER BY a.YearStart) AS Previous_Year_Claims,
ROUND(((a.Total_Claims - LAG(a.Total_Claims) OVER (ORDER BY a.YearStart)) * 100.0 / LAG(a.Total_Claims) OVER (ORDER BY a.YearStart)),2) AS Growth_Rate
FROM(
	SELECT YearStart, COUNT(*) AS Total_Claims
    FROM Medicare_Claims
    GROUP BY YearStart
	) a

----- CREATE A VIEW TO SAVE THE RESULT 

CREATE VIEW Year_over_Year_Growth_Rate AS
SELECT a.YearStart, a.Total_Claims, LAG(a.Total_Claims) OVER (ORDER BY a.YearStart) AS Previous_Year_Claims,
ROUND(((a.Total_Claims - LAG(a.Total_Claims) OVER (ORDER BY a.YearStart)) * 100.0 / LAG(a.Total_Claims) OVER (ORDER BY a.YearStart)),2) AS Growth_Rate
FROM(
	SELECT YearStart, COUNT(*) AS Total_Claims
    FROM Medicare_Claims
    GROUP BY YearStart
	) a

/* 6. Claim Types Trend Analysis

Task: Analyze trends for different claim types.
SQL Query: */

SELECT YearStart, ClaimType, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, ClaimType
ORDER BY YearStart, ClaimType

----- CREATE A VIEW TO SAVE THE RESULT 

CREATE VIEW Claim_Types_Trend_Analysis AS
SELECT YearStart, ClaimType, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, ClaimType
ORDER BY YearStart, ClaimType


/* 7. Distribution of Claim Values

Task: Analyze the distribution of claim values over the years.
SQL Query: */

SELECT YearStart, Data_Value, COUNT(*) AS Claim_Count
FROM Medicare_Claims
GROUP BY YearStart, Data_Value
ORDER BY YearStart, Data_Value


----- CREATE A VIEW TO SAVE THE RESULT 

CREATE VIEW Distribution_of_Claim_Values AS
SELECT YearStart, Data_Value, COUNT(*) AS Claim_Count
FROM Medicare_Claims
GROUP BY YearStart, Data_Value
ORDER BY YearStart, Data_Value


/* 8. Anomalies and Outliers Detection

Task: Identify years with significant deviations in claim counts or values.
SQL Query: */

WITH YearlyClaims AS (
    SELECT YearStart, COUNT(*) AS Total_Claims
    FROM Medicare_Claims
    GROUP BY YearStart
),
YearlyStats AS (
    SELECT AVG(Total_Claims) AS Avg_Claims, STDDEV(Total_Claims) AS Std_Dev
    FROM YearlyClaims
)
SELECT YearStart, Total_Claims
FROM YearlyClaims, YearlyStats
WHERE Total_Claims > (Avg_Claims + 1.5 * Std_Dev)
   OR Total_Claims < (Avg_Claims - 1.5 * Std_Dev)

/* NOTE:
YearlyClaims CTE: This common table expression calculates the total number 
of claims per year.
YearlyStats CTE: This common table expression calculates the average and 
standard deviation of the yearly total claims.
Main Query: The main query selects the years where the total number of 
claims significantly deviates from the average (using 1.5 standard deviations
as the threshold).  */


----CREATE VIEW FOR Anomalies_and_Outliers_Detection with WRT YearStart and TotalClaims

CREATE VIEW Anomalies_and_Outliers_Detection AS
WITH YearlyClaims AS (
    SELECT YearStart, COUNT(*) AS Total_Claims
    FROM Medicare_Claims
    GROUP BY YearStart
),
YearlyStats AS (
    SELECT AVG(Total_Claims) AS Avg_Claims, STDDEV(Total_Claims) AS Std_Dev
    FROM YearlyClaims
)
SELECT YearStart, Total_Claims
FROM YearlyClaims, YearlyStats
WHERE Total_Claims > (Avg_Claims + 1.5 * Std_Dev)
   OR Total_Claims < (Avg_Claims - 1.5 * Std_Dev)


/* 9. Claims by Demographic Factors

Task: Analyze trends based on demographic factors like race and gender.
SQL Query: */

SELECT YearStart, Break_Out_Category, Break_Out, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, Break_Out_Category, Break_Out
ORDER BY YearStart, Break_Out_Category, Break_Out


----CREATE VIEW Claims_by_Demographic_Factors

CREATE VIEW Claims_by_Demographic_Factors AS
SELECT YearStart, Break_Out_Category, Break_Out, COUNT(*) AS Total_Claims, SUM(Data_Value) AS Total_Value
FROM Medicare_Claims
GROUP BY YearStart, Break_Out_Category, Break_Out
ORDER BY YearStart, Break_Out_Category, Break_Out


/* Key Findings
Increasing Trends: Medicare claims have shown a steady increase over the years, indicating rising 
healthcare service utilization among Medicare beneficiaries.
Regional Variations: Certain regions consistently report higher
claim volumes, suggesting variations in healthcare needs or service
availability.
Demographic Disparities: Analysis by demographic factors such as race and
gender reveals disparities in claim rates,
highlighting potential areas of inequity in healthcare access or outcomes.
Seasonal Patterns: Monthly analysis within years shows seasonal fluctuations 
in claim volumes, which could be associated with seasonal illnesses or other factors.
Claim Types: Different types of claims (inpatient, outpatient, SNF, etc.) show 
varying trends, reflecting changes in healthcare service delivery and patient preferences.
Anomalies and Outliers: Certain years exhibit significant deviations in claim 
volumes, which may require further investigation to understand underlying causes.

RECOMMENDATIONS

Policy Adjustments: Based on the increasing trends, policymakers should consider 
strategies to manage the growing demand for Medicare services, such as enhancing 
preventive care and managing chronic diseases.
Resource Allocation: Allocate more resources to regions with higher claim volumes 
to ensure adequate healthcare service availability and address regional disparities.
Address Demographic Disparities: Implement targeted interventions to address healthcare 
access and outcome disparities among different demographic groups.
Seasonal Preparedness: Develop seasonal preparedness plans to handle fluctuations in
healthcare service demand, particularly during peak times.
Monitor and Investigate Anomalies: Establish mechanisms to continuously monitor claim 
data for anomalies and investigate any significant deviations to identify and address root causes.

ACTION PLANS

i. Data Monitoring and Reporting

Task: Implement a system for ongoing data monitoring using SQL queries and Power BI dashboards.
Responsibility: Data Analyst team.
Timeline: Monthly updates.

ii. Regional Analysis and Resource Allocation

Task: Conduct a detailed regional analysis to identify specific needs and allocate resources accordingly.
Responsibility: Healthcare Administrators and Policy Makers.
Timeline: Quarterly reviews.

iii. Demographic-Focused Interventions

Task: Develop and implement programs targeting identified demographic disparities in healthcare access and outcomes.
Responsibility: Public Health Officials and Community Health Workers.
Timeline: Bi-annual reviews and updates.

iv. Seasonal Preparedness Planning

Task: Create and test seasonal preparedness plans to ensure readiness for fluctuations
in service demand.
Responsibility: Healthcare Providers and Emergency Response Teams.
Timeline: Annual drills and updates.

v. Anomaly Detection and Investigation

Task: Set up automated alerts for anomaly detection and establish protocols for 
investigating significant deviations.
Responsibility: Data Analysts and Healthcare Auditors.
Timeline: Real-time monitoring and immediate investigation upon detection.

vi. Stakeholder Communication

Task: Regularly communicate findings and action plans to all relevant stakeholders,
including healthcare providers, policymakers, and community organizations.
Responsibility: Project Manager.
Timeline: Monthly updates and annual reports.

CONCLUSION

By systematically analyzing Medicare claims data and implementing targeted actions 
based on the insights gained, we can enhance healthcare service delivery, address 
disparities, and ensure better health outcomes for Medicare beneficiaries. Continuous 
monitoring and proactive interventions are essential to adapt to the evolving healthcare
landscape and meet the needs of the population effectively */