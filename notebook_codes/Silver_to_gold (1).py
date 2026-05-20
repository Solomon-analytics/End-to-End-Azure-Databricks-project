#!/usr/bin/env python
# coding: utf-8

# ## Silver_to_gold
# 
# null

# In[2]:


from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import *


# # **Parameters**

# In[48]:


today_date = ''
workspace = ''
lakehouse = ''
silver_lakehouse = ''


# In[4]:


##workspace = "b00274e4-e583-49e3-a029-066c995f631a"
##lakehouse = "2c043999-7f6f-4d44-be08-a03426b73d72"
##silver_lakehouse = "55a2c543-ac85-4bb2-81a8-2bc862c1cc5d"


# ### Reading customer_silver from silver lakehouse

# In[5]:


customer_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse}/Tables/dbo/customer_silver"

customer_df = spark.read.format('delta').load(customer_silver_path).filter(F.col("processing_date")==str(today_date))


# ### Business Transformation & Enrichment

# In[6]:


# create column Age
# considering the last batch of the data is 2019-04, Age will be calculated in regards to 2019-04-30

ref_date = F.lit("2019-04-30").cast("date")

customer_df = customer_df.withColumn(
    "age",
    F.floor(F.months_between(ref_date, F.col("birthdate")) / 12)
).withColumn(
    "age_category",
    F.when(F.col("age").between(18, 25), "18-25")
     .when(F.col("age").between(26, 35), "26-35")
     .when(F.col("age").between(36, 50), "36-50")
     .when(F.col("age").between(51, 65), "51-65")
     .otherwise("65+")
)


# ### Creating Dim_customer

# In[7]:


# Define schema for customer table
dim_customer_schema = StructType([
    StructField('customer_id', IntegerType(), True), 
    StructField('home_store', IntegerType(), True), 
    StructField('customer_first_name', StringType(), True), 
    StructField('customer_email', StringType(), True), 
    StructField('customer_since', DateType(), True), 
    StructField('loyalty_card_number', StringType(), True), 
    StructField('birthdate', DateType(), True), 
    StructField('gender', StringType(), True), 
    StructField('birth_year', StringType(), True), 
    StructField('processing_date', DateType(), True), 
    StructField('age', LongType(), True), 
    StructField('age_category', StringType(), False)
])

DeltaTable.createIfNotExists(spark).tableName("dim_customer")\
            .addColumns(dim_customer_schema)\
            .execute()


# ### assigning customer_df data into a temp variable

# In[8]:


df_selected_dim_customer = (customer_df.select('customer_id',
 'home_store',
 'customer_first_name',
 'customer_email',
 'customer_since',
 'loyalty_card_number',
 'birthdate',
 'gender',
 'birth_year',
 'processing_date',
 'age',
 'age_category'))


# ### Insert data into dim_employee using UPSERT Logic

# In[9]:


dim_customer_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/dim_customer"

dim_deltacustomer = DeltaTable.forPath(spark, dim_customer_path)

# perform the MERGE (UPSERT)
dim_deltacustomer.alias('target').merge(
    df_selected_dim_customer.alias('source'),
    'target.customer_id = source.customer_id'
).whenMatchedUpdate(set = {
 'home_store' : 'source.home_store',
 'customer_first_name' : 'source.customer_first_name',
 'customer_email' : 'source.customer_email',
 'customer_since' : 'source.customer_since',
 'loyalty_card_number' : 'source.loyalty_card_number',
 'birthdate' : 'source.birthdate',
 'gender' : 'source.gender',
 'birth_year' : 'source.birth_year',
 'processing_date' : 'source.processing_date',
 'age' : 'source.age',
 'age_category' : 'source.age_category'
}).whenNotMatchedInsert(values = {
 'customer_id' : 'source.customer_id',  
 'home_store' : 'source.home_store',
 'customer_first_name' : 'source.customer_first_name',
 'customer_email' : 'source.customer_email',
 'customer_since' : 'source.customer_since',
 'loyalty_card_number' : 'source.loyalty_card_number',
 'birthdate' : 'source.birthdate',
 'gender' : 'source.gender',
 'birth_year' : 'source.birth_year',
 'processing_date' : 'source.processing_date',
 'age' : 'source.age',
 'age_category' : 'source.age_category'
}).execute()


# ### Get history

# In[10]:


# Get the history of the Delta table to extract metrics
history_df = dim_deltacustomer.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',dim_deltacustomer.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")


# ### Reading employee_silver from silver lakehouse

# In[11]:


employee_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse}/Tables/dbo/employee_silver"

employee_df = spark.read.format('delta').load(employee_silver_path).filter(F.col("processing_date")==str(today_date))


# ### Business Transformation & Enrichment

# In[12]:


# creating column Full_name
employee_df = employee_df.withColumn(
    "full_name",
    F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
).withColumn(
    "duration",
    F.floor(F.months_between(ref_date, F.col("start_date")) / 12)
).withColumn(
    "duration_category",
    F.when(F.col("duration").between(0, 1), "0-1yrs")
     .when(F.col("duration").between(1, 3), "1-3yrs")
     .when(F.col("duration").between(3, 5), "3-5yrs")
     .when(F.col("duration").between(5, 7), "5-7yrs")
     .when(F.col("duration").between(7, 10), "7-10yrs")
     .otherwise("10+ yrs")
).withColumn(
    "is_manager",
    F.when(F.col("position")=="Store Manager", "Y").otherwise("N")
).withColumnRenamed(
    "location", "store_id"
)

# convert columns to integer
cols_to_convert = ["staff_id", "store_id"]

for colname in cols_to_convert:
    employee_df = employee_df.withColumn(colname, F.col(colname).cast("integer")
)


# ### Creating Dim_employee schema

# In[13]:


dim_employee_schema = StructType([
    StructField('staff_id', IntegerType(), True), 
    StructField('first_name', StringType(), True), 
    StructField('last_name', StringType(), True), 
    StructField('position', StringType(), True), 
    StructField('start_date', DateType(), True), 
    StructField('store_id', IntegerType(), True), 
    StructField('processing_date', DateType(), True), 
    StructField('full_name', StringType(), False), 
    StructField('duration', LongType(), True), 
    StructField('duration_category', StringType(), False), 
    StructField('is_manager', StringType(), False)
])

DeltaTable.createIfNotExists(spark).tableName("dim_employee")\
                  .addColumns(dim_employee_schema)\
                  .execute()


# ### Assigning employee_df into a temp variable

# In[14]:


df_selected_dim_employee = (employee_df.select(
  'staff_id',
 'first_name',
 'last_name',
 'position',
 'start_date',
 'store_id',
 'processing_date',
 'full_name',
 'duration',
 'duration_category',
 'is_manager'
))


# ### writing data into dim_employee using UPSERT Logic

# In[15]:


dim_employee_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/dim_employee"

dim_deltaemployee = DeltaTable.forPath(spark, dim_employee_path)

# perform the MERGE (UPSERT)
dim_deltaemployee.alias('target').merge(
    df_selected_dim_employee.alias('source'),
    'target.staff_id = source.staff_id'
).whenMatchedUpdate(set = {
    'first_name': 'source.first_name',
    'last_name' : 'source.last_name',
    'position' : 'source.position',
    'start_date' : 'source.start_date',
    'store_id' : 'source.store_id',
    'processing_date' : 'source.processing_date',
    'full_name' : 'source.full_name',
    'duration' : 'source.duration',
    'duration_category' : 'source.duration_category',
    'is_manager': 'source.is_manager'
}).whenNotMatchedInsert(values = {
    'staff_id' : 'source.staff_id',
    'first_name': 'source.first_name',
    'last_name' : 'source.last_name',
    'position' : 'source.position',
    'start_date' : 'source.start_date',
    'store_id' : 'source.store_id',
    'processing_date' : 'source.processing_date',
    'full_name' : 'source.full_name',
    'duration' : 'source.duration',
    'duration_category' : 'source.duration_category',
    'is_manager': 'source.is_manager'
}).execute()


# ### Get History

# In[16]:


# Get the history of the Delta table to extract metrics
history_df = dim_deltaemployee.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',dim_deltaemployee.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")


# ### Reading inventory_silver from silver lakehouse

# In[17]:


inventory_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse}/Tables/dbo/inventory_silver"

inventory_df = spark.read.format('delta').load(inventory_silver_path).filter(F.col("processing_date")==str(today_date))


# ### Business Transformation & Enrichment

# In[18]:


inventory_df = inventory_df.withColumn(
    "quantity_remaining",
    F.col("quantity_start_of_day") - F.col("quantity_sold")
).withColumn(
    "days_since_baked",
    F.datediff(
        F.to_date(F.col("transaction_date")),
        F.to_date(F.col("baked_date"))
    )
).withColumn(
    # business rule: check if there are quantity remaining after 3days from baked date
    "waste_quantity",
    F.when(F.col("days_since_baked") > 3, F.col("quantity_remaining")).otherwise(F.lit(0))
).withColumn(
    "sell_through_rate",
    F.when(
        F.col("quantity_start_of_day") > 0,
        F.round(F.col("quantity_sold") / F.col("quantity_start_of_day"), 3)
    ).otherwise(F.lit(0.0))
).withColumn(
    "is_fresh",
    F.when(F.col("days_since_baked") <= 3, "Y").otherwise("N")
).withColumn(
    "stockout_flag",
    F.when(F.col("quantity_remaining")==0, "Y").otherwise("N")
)


# ### Creating fact_inventory

# In[19]:


fact_inventory_schema = StructType([
    StructField('store_id', IntegerType(), True), 
    StructField('baked_date', DateType(), True), 
    StructField('transaction_date', DateType(), True), 
    StructField('product_id', IntegerType(), True), 
    StructField('quantity_start_of_day', IntegerType(), True), 
    StructField('quantity_sold', IntegerType(), True), 
    StructField('processing_date', DateType(), True), 
    StructField('baked_date_id', StringType(), True), 
    StructField('transaction_date_id', StringType(), True), 
    StructField('inventory_id', StringType(), True), 
    StructField('quantity_remaining', IntegerType(), True), 
    StructField('days_since_baked', IntegerType(), True), 
    StructField('waste_quantity', IntegerType(), True), 
    StructField('sell_through_rate', DoubleType(), True), 
    StructField('is_fresh', StringType(), False), 
    StructField('stockout_flag', StringType(), False)
])

DeltaTable.createIfNotExists(spark).tableName("fact_inventory")\
            .addColumns(fact_inventory_schema)\
            .execute()


# ### assigning inventory_df to a temp variable

# In[20]:


df_selected_fact_inventory = (inventory_df.select(
 'store_id',
 'baked_date',
 'transaction_date',
 'product_id',
 'quantity_start_of_day',
 'quantity_sold',
 'processing_date',
 'baked_date_id',
 'transaction_date_id',
 'inventory_id',
 'quantity_remaining',
 'days_since_baked',
 'waste_quantity',
 'sell_through_rate',
 'is_fresh',
 'stockout_flag'
))


# ### inserting data into fact_inventory using UPSERT Logic

# In[21]:


fact_inventory_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/fact_inventory"

fact_deltainventory = DeltaTable.forPath(spark, fact_inventory_path)

# perform the MERGE (UPSERT)
fact_deltainventory.alias('target').merge(
    df_selected_fact_inventory.alias('source'),
    'target.inventory_id = source.inventory_id'
).whenMatchedUpdate(set = {
'store_id': 'source.store_id',
'baked_date': 'source.baked_date',
'transaction_date': 'source.transaction_date',
'product_id': 'source.product_id',
'quantity_start_of_day' : 'source.quantity_start_of_day',
'quantity_sold' : 'source.quantity_sold',
'processing_date': 'source.processing_date',
'baked_date_id' : 'source.baked_date_id',
'transaction_date_id'   : 'source.transaction_date_id',
'quantity_remaining': 'source.quantity_remaining',
'days_since_baked': 'source.days_since_baked',
'waste_quantity': 'source.waste_quantity',
'sell_through_rate' : 'source.sell_through_rate',
'is_fresh': 'source.is_fresh',
'stockout_flag' : 'source.stockout_flag'
}).whenNotMatchedInsert(values = {
'store_id': 'source.store_id',
'baked_date': 'source.baked_date',
'transaction_date': 'source.transaction_date',
'product_id': 'source.product_id',
'quantity_start_of_day' : 'source.quantity_start_of_day',
'quantity_sold' : 'source.quantity_sold',
'processing_date': 'source.processing_date',
'baked_date_id' : 'source.baked_date_id',
'transaction_date_id'   : 'source.transaction_date_id',
'inventory_id'  : 'source.inventory_id',
'quantity_remaining': 'source.quantity_remaining',
'days_since_baked': 'source.days_since_baked',
'waste_quantity': 'source.waste_quantity',
'sell_through_rate' : 'source.sell_through_rate',
'is_fresh': 'source.is_fresh',
'stockout_flag' : 'source.stockout_flag'
}).execute()


# ### Get History

# In[22]:


# Get the history of the Delta table to extract metrics
history_df = fact_deltainventory.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',fact_deltainventory.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")


# ### Reading product_silver from silver lakehouse

# In[23]:


product_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse}/Tables/dbo/product_silver"

product_df = spark.read.format('delta').load(product_silver_path).filter(F.col("processing_date")==str(today_date))



# ### Business transformation & enrichment

# In[24]:


product_df = product_df.withColumn(
    "gross_margin_pct",
    F.round((F.col("current_retail_price") - F.col("current_wholesale_price")) / F.col("current_retail_price"), 3)
)


# ### creating dim_product schema

# In[25]:


dim_product_schema = StructType([
    StructField('product_id', StringType(), True), 
    StructField('product_group', StringType(), True), 
    StructField('product_category', StringType(), True), 
    StructField('product_type', StringType(), True), 
    StructField('product', StringType(), True), 
    StructField('product_description', StringType(), True), 
    StructField('current_cost', DecimalType(10,2), True), 
    StructField('current_wholesale_price', DecimalType(10,2), True), 
    StructField('current_retail_price', DecimalType(10,2), True), 
    StructField('tax_exempt_yn', StringType(), True), 
    StructField('promo_yn', StringType(), True), 
    StructField('new_product_yn', StringType(), True), 
    StructField('processing_date', DateType(), True), 
    StructField('product_original_uom', StringType(), True), 
    StructField('product_lb', DoubleType(), True), 
    StructField('gross_margin_pct', DecimalType(15,3), True)])

DeltaTable.createIfNotExists(spark).tableName("dim_product")\
            .addColumns(dim_product_schema)\
            .execute()


# ### assigning product_df to a temp variable

# In[26]:


df_selected_dim_product = (product_df.select(
 'product_id',
 'product_group',
 'product_category',
 'product_type',
 'product',
 'product_description',
 'current_cost',
 'current_wholesale_price',
 'current_retail_price',
 'tax_exempt_yn',
 'promo_yn',
 'new_product_yn',
 'processing_date',
 'product_original_uom',
 'product_lb',
 'gross_margin_pct' 
))


# ### inserting data into dim_product using UPSERT Logic

# In[27]:


dim_product_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/dim_product"

dim_deltaproduct = DeltaTable.forPath(spark, dim_product_path)

# perform the MERGE (UPSERT)
dim_deltaproduct.alias('target').merge(
    df_selected_dim_product.alias('source'),
    'target.product_id = source.product_id'
).whenMatchedUpdate(set = {
 'product_group': 'source.product_group',
 'product_category': 'source.product_category',
 'product_type' : 'source.product_type',
 'product': 'source.product',
 'product_description': 'source.product_description',
 'current_cost' : 'source.current_cost',
 'current_wholesale_price'  : 'source.current_wholesale_price',
 'current_retail_price'  : 'source.current_retail_price',
 'tax_exempt_yn': 'source.tax_exempt_yn',
 'promo_yn'  : 'source.promo_yn',
 'new_product_yn' : 'source.new_product_yn',
 'processing_date': 'source.processing_date',
 'product_original_uom'  : 'source.product_original_uom',
 'product_lb': 'source.product_lb',
 'gross_margin_pct': 'source.gross_margin_pct'
}).whenNotMatchedInsert(values = {
 'product_id': 'source.product_id',
 'product_group': 'source.product_group',
 'product_category': 'source.product_category',
 'product_type' : 'source.product_type',
 'product': 'source.product',
 'product_description': 'source.product_description',
 'current_cost' : 'source.current_cost',
 'current_wholesale_price'  : 'source.current_wholesale_price',
 'current_retail_price'  : 'source.current_retail_price',
 'tax_exempt_yn': 'source.tax_exempt_yn',
 'promo_yn'  : 'source.promo_yn',
 'new_product_yn' : 'source.new_product_yn',
 'processing_date': 'source.processing_date',
 'product_original_uom'  : 'source.product_original_uom',
 'product_lb': 'source.product_lb',
 'gross_margin_pct': 'source.gross_margin_pct'
}).execute()


# ### Get History

# In[28]:


# Get the history of the Delta table to extract metrics
history_df = dim_deltaproduct.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',dim_deltaproduct.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")


# ### reading sales silver from silver lakehouse

# In[29]:


sales_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse}/Tables/dbo/sales_silver"

sales_df = spark.read.format('delta').load(sales_silver_path).filter(F.col("processing_date")==str(today_date))


# ### Business Transformation & Enrichment

# In[30]:


sales_df = sales_df.withColumn(
    "time_of_day_category",
    F.when(F.hour(F.col("transaction_time")).between(6, 11), "Morning")
     .when(F.hour(F.col("transaction_time")).between(12, 14), "Lunch")
     .when(F.hour(F.col("transaction_time")).between(15, 17), "Afternoon")
     .when(F.hour(F.col("transaction_time")).between(18, 21), "Evening")
).withColumn(
    "revenue",
    F.round(F.col("unit_price") * F.col("quantity_sold"), 3)
)


# ### Creating fact_sales schema

# In[31]:


fact_sales_schema = StructType([
    StructField('transaction_id', IntegerType(), True), 
    StructField('transaction_date', DateType(), True), 
    StructField('transaction_time', StringType(), True), 
    StructField('store_id', IntegerType(), True), 
    StructField('staff_id', IntegerType(), True), 
    StructField('customer_id', IntegerType(), True), 
    StructField('instore_yn', StringType(), True), 
    StructField('order', IntegerType(), True), 
    StructField('line_item_id', IntegerType(), True), 
    StructField('product_id', IntegerType(), True), 
    StructField('quantity_sold', IntegerType(), True), 
    StructField('unit_price', DoubleType(), True), 
    StructField('promo_item_yn', StringType(), True), 
    StructField('processing_date', DateType(), True), 
    StructField('sales_key', StringType(), True), 
    StructField('transaction_date_id', StringType(), True), 
    StructField('time_of_day_category', StringType(), True), 
    StructField('revenue', DoubleType(), True)
])

DeltaTable.createIfNotExists(spark).tableName("fact_sales")\
            .addColumns(fact_sales_schema)\
            .execute()


# ### assigning sales_df to a temp variable

# In[32]:


df_selected_fact_sales = (sales_df.select(
 'transaction_id',
 'transaction_date',
 'transaction_time',
 'store_id',
 'staff_id',
 'customer_id',
 'instore_yn',
 'order',
 'line_item_id',
 'product_id',
 'quantity_sold',
 'unit_price',
 'promo_item_yn',
 'processing_date',
 'sales_key',
 'transaction_date_id',
 'time_of_day_category',
 'revenue'
))


# ### Inserting data into fact_sales using UPSERT Logic

# In[33]:


fact_sales_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/fact_sales"

fact_deltasales = DeltaTable.forPath(spark, fact_sales_path)

# perform the MERGE (UPSERT)
fact_deltasales.alias('target').merge(
    df_selected_fact_sales.alias('source'),
    'target.sales_key = source.sales_key'
).whenMatchedUpdate(set = {
 'transaction_id' : 'source.transaction_id',
 'transaction_date' : 'source.transaction_date',
 'transaction_time': 'source.transaction_time',
 'store_id' : 'source.store_id',
 'staff_id' : 'source.staff_id',
 'customer_id' : 'source.customer_id',
 'instore_yn' : 'source.instore_yn',
 'order' : 'source.order',
 'line_item_id' : 'source.line_item_id',
 'product_id' : 'source.product_id',
 'quantity_sold' : 'source.quantity_sold',
 'unit_price' : 'source.unit_price',
 'promo_item_yn' : 'source.promo_item_yn',
 'processing_date' : 'source.processing_date',
 'transaction_date_id': 'source.transaction_date_id',
 'time_of_day_category'  : 'source.time_of_day_category',
 'revenue' : 'source.revenue'
}).whenNotMatchedInsert(values = {
 'transaction_id': 'source.transaction_id',
 'transaction_date': 'source.transaction_date',
 'transaction_time': 'source.transaction_time',
 'store_id': 'source.store_id',
 'staff_id': 'source.staff_id',
 'customer_id': 'source.customer_id',
 'instore_yn' : 'source.instore_yn',
 'order' : 'source.order',
 'line_item_id'  : 'source.line_item_id',
 'product_id' : 'source.product_id',
 'quantity_sold' : 'source.quantity_sold',
 'unit_price' : 'source.unit_price',
 'promo_item_yn' : 'source.promo_item_yn',
 'processing_date' : 'source.processing_date',
 'sales_key'  : 'source.sales_key',
 'transaction_date_id': 'source.transaction_date_id',
 'time_of_day_category'  : 'source.time_of_day_category',
 'revenue' : 'source.revenue'
}).execute()


# ### Get History

# In[34]:


# Get the history of the Delta table to extract metrics
history_df = fact_deltasales.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',fact_deltasales.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")


# ### Reading store silver from silver lakehouse

# In[35]:


store_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{silver_lakehouse}/Tables/dbo/store_silver"

store_df = spark.read.format('delta').load(store_silver_path).filter(F.col("processing_date")==str(today_date))


# ### Business Transformation & Enrichment

# In[36]:


store_df = store_df.withColumn(
    "store_full_address",
    F.concat_ws(",",
    F.col("store_address"),
    F.col("store_city"),
    F.col("store_state_province"),
    F.col("store_postal_code")
))


# ### creating dim_store schema

# In[37]:


dim_store_schema = StructType([
    StructField('store_id', IntegerType(), True), 
    StructField('store_type', StringType(), True), 
    StructField('store_square_feet', IntegerType(), True), 
    StructField('store_address', StringType(), True), 
    StructField('store_city', StringType(), True), 
    StructField('store_state_province', StringType(), True), 
    StructField('store_postal_code', IntegerType(), True), 
    StructField('store_longitude', DoubleType(), True), 
    StructField('store_latitude', DoubleType(), True), 
    StructField('manager', DoubleType(), True), 
    StructField('neighorhood', StringType(), True), 
    StructField('processing_date', DateType(), True), 
    StructField('store_full_address', StringType(), False)
])

DeltaTable.createIfNotExists(spark).tableName("dim_store")\
            .addColumns(dim_store_schema)\
            .execute()


# ### assigning store_df to a temp variable

# In[38]:


df_selected_dim_store = (store_df.select(
 'store_id',
 'store_type',
 'store_square_feet',
 'store_address',
 'store_city',
 'store_state_province',
 'store_postal_code',
 'store_longitude',
 'store_latitude',
 'manager',
 'neighorhood',
 'processing_date',
 'store_full_address'
))


# ### inserting data into dim_store using UPSERT Logic

# In[39]:


dim_store_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/dim_store"

dim_deltastore = DeltaTable.forPath(spark, dim_store_path)

# perform the MERGE (UPSERT)
dim_deltastore.alias('target').merge(
    df_selected_dim_store.alias('source'),
    'target.store_id = source.store_id'
).whenMatchedUpdate(set = {
 'store_type' : 'source.store_type',
 'store_square_feet' : 'source.store_square_feet',
 'store_address'  : 'source.store_address',
 'store_city' : 'source.store_city',
 'store_state_province' : 'source.store_state_province',
 'store_postal_code' : 'source.store_postal_code',
 'store_longitude': 'source.store_longitude',
 'store_latitude' : 'source.store_latitude',
 'manager'  : 'source.manager',
 'neighorhood' : 'source.neighorhood',
 'processing_date' : 'source.processing_date',
 'store_full_address' : 'source.store_full_address'
}).whenNotMatchedInsert(values = {
 'store_id' : 'source.store_id',
 'store_type' : 'source.store_type',
 'store_square_feet' : 'source.store_square_feet',
 'store_address'  : 'source.store_address',
 'store_city' : 'source.store_city',
 'store_state_province' : 'source.store_state_province',
 'store_postal_code' : 'source.store_postal_code',
 'store_longitude': 'source.store_longitude',
 'store_latitude' : 'source.store_latitude',
 'manager'  : 'source.manager',
 'neighorhood': 'source.neighorhood',
 'processing_date': 'source.processing_date',
 'store_full_address': 'source.store_full_address'
}).execute()


# ### Get History

# In[40]:


# Get the history of the Delta table to extract metrics
history_df = dim_deltastore.history(1)  # Get the latest operation

# Extract metrics from the history DataFrame
operation_metrics = history_df.select("operationMetrics").collect()[0][0]

# Extract specific metrics
rows_inserted = operation_metrics.get('numTargetRowsInserted', 0)
rows_updated = operation_metrics.get('numTargetRowsUpdated', 0)
rows_deleted = operation_metrics.get('numTargetRowsDeleted', 0)
rows_affected = int(rows_inserted) + int(rows_updated) + int(rows_deleted) 

print('Total rows of table: ',dim_deltastore.toDF().count())
print("Merge Metrics:")
print(f"Rows inserted: {rows_inserted}")
print(f"Rows updated: {rows_updated}")
print(f"Rows deleted: {rows_deleted}")
print(f"Total rows affected: {rows_affected}")


# ### Reading Calendar Table

# In[41]:


calendar_df = spark.read.format("csv").option("header","true").load("Files/static table/4-5-4 Calendar.csv")
# df now is a Spark DataFrame containing CSV data from "Files/static table/4-5-4 Calendar.csv".


# ### Creating a date_id

# In[42]:


calendar_df = calendar_df.withColumn(
    "date_id",
    F.regexp_replace(F.col("date"), "-", "")
)



# ### Creating baked calendar schema

# In[43]:


baked_calendar_schema = StructType([
    StructField('Date', StringType(), True), 
    StructField('FiscalYear', StringType(), True), 
    StructField('FiscalQuarter', StringType(), True), 
    StructField('FiscalMonthNumber', StringType(), True), 
    StructField('FiscalMonthOfQuarter', StringType(), True), 
    StructField('FiscalWeekOfYear', StringType(), True), 
    StructField('DayOfWeek', StringType(), True), 
    StructField('FiscalMonthName', StringType(), True), 
    StructField('FiscalMonthYear', StringType(), True), 
    StructField('FiscalQuarterYear', StringType(), True), 
    StructField('DayOfMonthNumber', StringType(), True), 
    StructField('DayName', StringType(), True), 
    StructField('date_id', StringType(), True)])

DeltaTable.createIfNotExists(spark).tableName("baked_calendar")\
            .addColumns(baked_calendar_schema)\
            .execute()


# ### assigning calendar_df to a temp variable

# In[44]:


df_selected_calendar = (calendar_df.select(
 'Date',
 'FiscalYear',
 'FiscalQuarter',
 'FiscalMonthNumber',
 'FiscalMonthOfQuarter',
 'FiscalWeekOfYear',
 'DayOfWeek',
 'FiscalMonthName',
 'FiscalMonthYear',
 'FiscalQuarterYear',
 'DayOfMonthNumber',
 'DayName',
 'date_id'  
))


# ### inserting data into baked_calendar

# In[45]:


baked_calendar_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/baked_calendar"

cal_deltabaked = DeltaTable.forPath(spark, baked_calendar_path)

# perform the MERGE (UPSERT)
cal_deltabaked.alias('target').merge(
    df_selected_calendar.alias('source'),
    'target.date_id = source.date_id'
).whenMatchedUpdate(set = {
    'Date': 'source.Date',
    'FiscalYear': 'source.FiscalYear',
    'FiscalQuarter': 'source.FiscalQuarter',
    'FiscalMonthNumber': 'source.FiscalMonthNumber',
    'FiscalMonthOfQuarter': 'source.FiscalMonthOfQuarter',
    'FiscalWeekOfYear': 'source.FiscalWeekOfYear',
    'DayOfWeek': 'source.DayOfWeek',
    'FiscalMonthName': 'source.FiscalMonthName',
    'FiscalMonthYear': 'source.FiscalMonthYear',
    'FiscalQuarterYear': 'source.FiscalQuarterYear',
    'DayOfMonthNumber': 'source.DayOfMonthNumber',
    'DayName': 'source.DayName'
}).whenNotMatchedInsert(values = {
    'Date': 'source.Date',
    'FiscalYear': 'source.FiscalYear',
    'FiscalQuarter': 'source.FiscalQuarter',
    'FiscalMonthNumber': 'source.FiscalMonthNumber',
    'FiscalMonthOfQuarter': 'source.FiscalMonthOfQuarter',
    'FiscalWeekOfYear': 'source.FiscalWeekOfYear',
    'DayOfWeek': 'source.DayOfWeek',
    'FiscalMonthName': 'source.FiscalMonthName',
    'FiscalMonthYear': 'source.FiscalMonthYear',
    'FiscalQuarterYear': 'source.FiscalQuarterYear',
    'DayOfMonthNumber': 'source.DayOfMonthNumber',
    'DayName': 'source.DayName',
    'date_id': 'source.date_id'
}).execute()


# ### Creating transaction calendar schema

# In[46]:


transaction_calendar_schema = StructType([
    StructField('Date', StringType(), True), 
    StructField('FiscalYear', StringType(), True), 
    StructField('FiscalQuarter', StringType(), True), 
    StructField('FiscalMonthNumber', StringType(), True), 
    StructField('FiscalMonthOfQuarter', StringType(), True), 
    StructField('FiscalWeekOfYear', StringType(), True), 
    StructField('DayOfWeek', StringType(), True), 
    StructField('FiscalMonthName', StringType(), True), 
    StructField('FiscalMonthYear', StringType(), True), 
    StructField('FiscalQuarterYear', StringType(), True), 
    StructField('DayOfMonthNumber', StringType(), True), 
    StructField('DayName', StringType(), True), 
    StructField('date_id', StringType(), True)])

DeltaTable.createIfNotExists(spark).tableName("transaction_calendar")\
            .addColumns(transaction_calendar_schema)\
            .execute()


# ### inserting data into transaction_calendar

# In[47]:


transaction_calendar_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/2c043999-7f6f-4d44-be08-a03426b73d72/Tables/dbo/transaction_calendar"

cal_deltatransaction = DeltaTable.forPath(spark, transaction_calendar_path)

# perform the MERGE (UPSERT)
cal_deltatransaction.alias('target').merge(
    df_selected_calendar.alias('source'),
    'target.date_id = source.date_id'
).whenMatchedUpdate(set = {
    'Date': 'source.Date',
    'FiscalYear': 'source.FiscalYear',
    'FiscalQuarter': 'source.FiscalQuarter',
    'FiscalMonthNumber': 'source.FiscalMonthNumber',
    'FiscalMonthOfQuarter': 'source.FiscalMonthOfQuarter',
    'FiscalWeekOfYear': 'source.FiscalWeekOfYear',
    'DayOfWeek': 'source.DayOfWeek',
    'FiscalMonthName': 'source.FiscalMonthName',
    'FiscalMonthYear': 'source.FiscalMonthYear',
    'FiscalQuarterYear': 'source.FiscalQuarterYear',
    'DayOfMonthNumber': 'source.DayOfMonthNumber',
    'DayName': 'source.DayName'
}).whenNotMatchedInsert(values = {
    'Date': 'source.Date',
    'FiscalYear': 'source.FiscalYear',
    'FiscalQuarter': 'source.FiscalQuarter',
    'FiscalMonthNumber': 'source.FiscalMonthNumber',
    'FiscalMonthOfQuarter': 'source.FiscalMonthOfQuarter',
    'FiscalWeekOfYear': 'source.FiscalWeekOfYear',
    'DayOfWeek': 'source.DayOfWeek',
    'FiscalMonthName': 'source.FiscalMonthName',
    'FiscalMonthYear': 'source.FiscalMonthYear',
    'FiscalQuarterYear': 'source.FiscalQuarterYear',
    'DayOfMonthNumber': 'source.DayOfMonthNumber',
    'DayName': 'source.DayName',
    'date_id': 'source.date_id'
}).execute()

