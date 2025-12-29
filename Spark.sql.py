from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.appName("Spark_SQL_practice").getOrCreate()
df=spark.read.format("csv")\
   .option("mode",'PERMISSIVE')\
   .option("columnNameofCorruptRecord","Corrut_data_col")\
   .option("header","true")\
   .option("inferSchema","true")\
   .option("input_path")
df.createOrReplaceTempView("employee")
spark.sql("""
SELECT city,
AVG(salary) AS City_Wise_Average_Salary,
COUNT(employee) AS Total_IT_Employee
FROM employee
WHERE department='IT'
GROUP BY city
""")
spark.sql(""" WITH FINAL AS(
SELECT customer_id,trx_date,LAG(trx_date,1) OVER(PARTITION BY CUSTOMER ORDER BY trx_date DESC ) AS previous_transactio-date
from tranaction_table)
SELECT distinct customer_id from final 
where datediff(trx_date,previous_transactio-date)=1
""")







spark.sql("""
SELECT a name AS employee_name,b.name AS Manager_name 
FROM empl_table a left join empl_table b
on b.empl_id=a.manager_id
""")


spark.sql( """ WITH FINAL AS(
SELECT sales_month,revenue, AVG(revenue) OVER(ORDER BY sales_date DESC ROWS BETWEEN 2 PRECEDING  AND CURRENT ROW) AS  moving_3_months_revenue
FROM salaes table )
SELECT sales_month, revenue,moving_3_months_revenue from final
where (revenue-moving_3_months_revenue)>0
""")
sel



)
spark.sql(""" with final as(
SELECT sales_month,category,revenue,LAG(revenue,1) OVER(PARTION BY category ORDER BY sales_date) as previous_month_revenue
from table
)
select sales_month,category,revenue,previous_month_revenue,(revenue-previous_month_revenue)as revenue_diff
from final

""")



spark.sql( """ WITH FINAL AS
(
SELECR a.department,sum(b.salary) over(partition by department) as department_wise salary,
b.empl_id,
CASE WHEN b.salary >10000 THEN 'High_earner' ELSE "Low_earner" END "Earner_category"
FROM  department_data a inner Join employee_data b
ON a.dept_id=b.dept_id
)
SELECT department,Deparment_wise_salary,count() as Employee_count
from Final 
where Earner_category='High_earner'
group by department,Deparment_wise_salary 
""")








spark.sql(""" with final as (
SELECT a.dept_name,sum(b.salary) as Total_salary ,
CASE WHEN SUM(b.salary)>100,00 THEN "HighEarners" ELSE "LowEarners" END AS  earner_category
from department inner join employee b
on a.dept_id=b.dept_id
GROUP BY dept
having Total_salary>500000)
SELECT 
dept_name,Total_salary,COUNT(earners_category) as Depatment_having_high earners
from final where earners_category="HighEarners"

""")