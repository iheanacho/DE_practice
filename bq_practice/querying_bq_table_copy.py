# step 1 installing necessary libraries 
#pip install google-cloud-bigquery
#pip install pandas

# step 2 setup authentication.
#   since i'm working in google cloudshell, it uses my GCP llogin details to manage the application default credientials 

# step 3 script
from google.cloud import bigquery


# 3a. create bq client: by constructing a bigquery client object
client = bigquery.Client()

#3b. create query: this query answers the question, who are our customers and where do they come from?

query= """
WITH
cust AS (SELECT
    DISTINCT oi.user_id,
    SUM(CASE WHEN u.gender = 'M' THEN 1 ELSE null END) AS male,
    SUM(CASE WHEN u.gender = 'F' THEN 1 ELSE null END) AS female,
    u.country AS country
    FROM `altschool-389114.assignment1.users` AS u
    INNER JOIN `altschool-389114.assignment1.order_item` AS oi
    on oi.user_id=u.id
    WHERE oi.status NOT IN ('Cancelled', 'Returned')
    GROUP BY 1, 4
)
SELECT c.country,
        COUNT(DISTINCT c.user_id) AS customer_count,
        COUNT c.female,
        COUNT c.male        
FROM cust as c
GROUP BY 1
ORDER BY 2"""

#3c. run query: by making an api request
query_job = client.query(query)

#3d. get query result
#results = query_job.result()

# step 4
#for row in query_job:
import pandas as pd
df = query_job.to_dataframe()    
print(df)
