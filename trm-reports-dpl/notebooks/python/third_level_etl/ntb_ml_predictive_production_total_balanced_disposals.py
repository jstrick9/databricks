# Databricks notebook source
from pyspark.sql.functions import cast, col, sum as spark_sum, countDistinct, to_date, date_format, abs, round
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"

config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run  ../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs["schema"]["trgt_catalog"]
spark.conf.set('conf.dbx_env', dbx_env)
dq_catalog = common_configs['schema']['data_quality_catalog']

# COMMAND ----------

# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_ml_predictive_production_total_balanced_disposals'

control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,starttime)

# COMMAND ----------

# Load data
df= spark.table(f"{trgt_catalog}.silver.epquery_stg3")

# Aggregate data by month-year
df_agg = df.withColumn("TRANSACTION_EFFECTIVE_DT", to_date(col("TRANSACTION_EFFECTIVE_DT"))) \
           .groupBy(date_format(col("TRANSACTION_EFFECTIVE_DT"), "yyyy-MM").alias("month_year")) \
           .agg(
               spark_sum("Total_BDS").alias("Total_BDS"),
               countDistinct("Action_Type").alias("case_count"),
               countDistinct("Examining_Attorney").alias("Number_of_Examiners")
           ) \
           .orderBy("month_year")

# Filter data for the specified date range
df_filtered = df_agg.filter((col("month_year") >= "2000-10") & (col("month_year") <= "2024-09"))

# COMMAND ----------

# Convert to Pandas DataFrame
pdf = df_filtered.toPandas()
pdf['month_year'] = pd.to_datetime(pdf['month_year'])
pdf.set_index('month_year', inplace=True)

# Convert columns to float
pdf['Total_BDS'] = pdf['Total_BDS'].astype(float)
pdf['case_count'] = pdf['case_count'].astype(float)
pdf['Number_of_Examiners'] = pdf['Number_of_Examiners'].astype(float)

# Split data into training and test sets
train = pdf["2000-10":'2023-10']
test = pdf['2023-10':'2024-09']

# Define the month_year variable
month_year = pd.date_range(start='2023-10-01', end='2027-09-30', freq='M').strftime('%Y-%m').tolist()

# COMMAND ----------

# Function to train and forecast using SARIMA
def train_and_forecast_sarima(train, test, column, order, seasonal_order):
    model = SARIMAX(train[column], order=order, seasonal_order=seasonal_order)
    model_fit = model.fit(disp=False)
    
    # Forecast for the test period
    forecast = model_fit.predict(start=len(train), end=len(train) + len(test) - 1, dynamic=False)
    
    # Extend the forecast to include the next 3 fiscal years
    future_forecast = model_fit.predict(start=len(train) + len(test), end=len(train) + len(test) + 35, dynamic=False)
    
    # Calculate error for the test period
    error = mean_squared_error(test[column], forecast)
    
    return forecast, future_forecast, error

# Define SARIMA order and seasonal order
order = (1, 1, 1)
seasonal_order = (1, 1, 1, 12)

# COMMAND ----------

# Train and forecast for Total_BDS
forecast_bds, future_forecast_bds, error_bds = train_and_forecast_sarima(
    train, test, 'Total_BDS', order, seasonal_order
)

# Train and forecast for case_count
forecast_case_count, future_forecast_case_count, error_case_count = train_and_forecast_sarima(
    train, test, 'case_count', order, seasonal_order
)

# Train and forecast for Number_of_Examiners
forecast_examiners, future_forecast_examiners, error_examiners = train_and_forecast_sarima(
    train, test, 'Number_of_Examiners', order, seasonal_order
)

# Extend the test data to include future forecast period
extended_test_bds = list(test['Total_BDS'].values) + [None] * len(future_forecast_bds)
extended_test_case_count = list(test['case_count'].values) + [None] * len(future_forecast_case_count)
extended_test_examiners = list(test['Number_of_Examiners'].values) + [None] * len(future_forecast_examiners)

# Combine forecast and future forecast
combined_forecast_bds = list(forecast_bds) + list(future_forecast_bds)
combined_forecast_case_count = list(forecast_case_count) + list(future_forecast_case_count)
combined_forecast_examiners = list(forecast_examiners) + list(future_forecast_examiners)


# Convert Pandas DataFrame to Spark DataFrame
results_bds = spark.createDataFrame(pd.DataFrame({
    'Month_Year': month_year,
    'Actual': extended_test_bds,
    'Forecast': combined_forecast_bds
}))
results_bds = results_bds.withColumn('Error', round((abs(col('Actual') - col('Forecast')) / col('Actual')) * 100, 2))
print(f'Mean Squared Error for Total_BDS: {error_bds}')
# display(results_bds)

results_case_count = spark.createDataFrame(pd.DataFrame({
    'Month_Year': month_year,
    'Actual': extended_test_case_count,
    'Forecast': combined_forecast_case_count
}))
results_case_count = results_case_count.withColumn('Error', round((abs(col('Actual') - col('Forecast')) / col('Actual')) * 100, 2))
print(f'Mean Squared Error for case_count: {error_case_count}')
# display(results_case_count)

results_examiners = spark.createDataFrame(pd.DataFrame({
    'Month_Year': month_year,
    'Actual': extended_test_examiners,
    'Forecast': combined_forecast_examiners
}))
results_examiners = results_examiners.withColumn('Error', round((abs(col('Actual') - col('Forecast')) / col('Actual')) * 100, 2))
print(f'Mean Squared Error for Number_of_Examiners: {error_examiners}')
# display(results_examiners)

# COMMAND ----------

# Plot results
plt.figure(figsize=(10, 6))
plt.plot(train.index, train['Total_BDS'], color='black', label='Train Total_BDS')
plt.plot(test.index, test['Total_BDS'], color='black', linestyle='dashed', label='Test Total_BDS')
plt.plot(test.index, forecast_bds, color='red', label='Forecast Total_BDS')
plt.plot(pd.date_range(start='2024-10-01', end='2027-09-30', freq='M'), future_forecast_bds, color='blue', linestyle='dashed', label='Future Forecast Total_BDS')

plt.plot(train.index, train['case_count'], color='green', label='Train case_count')
plt.plot(test.index, test['case_count'], color='green', linestyle='dashed', label='Test case_count')
plt.plot(test.index, forecast_case_count, color='orange', label='Forecast case_count')
plt.plot(pd.date_range(start='2024-10-01', end='2027-09-30', freq='M'), future_forecast_case_count, color='purple', linestyle='dashed', label='Future Forecast case_count')

plt.plot(train.index, train['Number_of_Examiners'], color='cyan', label='Train Number_of_Examiners')
plt.plot(test.index, test['Number_of_Examiners'], color='cyan', linestyle='dashed', label='Test Number_of_Examiners')
plt.plot(test.index, forecast_examiners, color='magenta', label='Forecast Number_of_Examiners')
plt.plot(pd.date_range(start='2024-10-01', end='2027-09-30', freq='M'), future_forecast_examiners, color='yellow', linestyle='dashed', label='Future Forecast Number_of_Examiners')

plt.xlabel('Date')
plt.ylabel('Values')
plt.title('SARIMA Forecast')
plt.legend()
plt.show()

# COMMAND ----------

results_bds.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{trgt_catalog}.gold.ml_sarima_total_balanced_disposals")

results_case_count.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{trgt_catalog}.gold.ml_sarima_bds_case_count")

results_examiners.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{trgt_catalog}.gold.ml_sarima_bds_examiners")

# COMMAND ----------

recs_count = results_bds.count()
end_job_cntl(f"{trgt_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")