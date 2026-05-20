#!/usr/bin/env python
# coding: utf-8

# ## landing to bronze
# 
# null

# # **Parameter**

# In[1]:


from pyspark.sql import functions as functions
from pyspark.sql.types import *


# In[1]:


today_date = ''
source_account = ''
source_container = ''
workspace = ''


# In[ ]:


#source_account = "pocstorage7730"
#source_container = "landing"
#workspace = "b00274e4-e583-49e3-a029-066c995f631a"


# In[10]:


# workspace = "b00274e4-e583-49e3-a029-066c995f631a"
partition_path = f"processing_date={today_date}/"


# # **ADLS Authentication**

# In[11]:


# storage account details
#storage_account_name = "pocstorage7730"
#container_name = "landing"
#container_bronze_name = "bronze"

landing_path = f"abfss://{source_container}@{source_account}.dfs.core.windows.net/"
bronze_path = f"abfss://bronze@{source_account}.dfs.core.windows.net/"
partition_path = f"processing_date={today_date}/"



# Access keys
storage_account_key = "TFQEY1fzaUUz+4Y/Ds5j0U1fIqazNVjEaggx01tC1Wgeej0GOCLoJL/eArBv5eVhahmVU4Mb18b/+AStlzan1g=="
account_fqdn = f"{source_account}.dfs.core.windows.net"

# set authentication
spark.conf.set(
    f"fs.azure.account.key.{account_fqdn}",
    storage_account_key
)


# ### Reading customer_df from ADLS landing

# In[12]:


customer_lookup_path = f"{landing_path}customer_lookup/{partition_path}"


# ### Displaying customer_df schema

# ### Developing Bronze schema

# In[6]:


# Defining bronze customer schema:
schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('home_store', StringType(), True), 
    StructField('customer_first_name', StringType(), True), 
    StructField('customer_email', StringType(), True), 
    StructField('customer_since', StringType(), True), 
    StructField('loyalty_card_number', StringType(), True), 
    StructField('birthdate', StringType(), True), 
    StructField('gender', StringType(), True), 
    StructField('birth_year', StringType(), True)
])


# ### write customer_df into a temp view

# In[7]:


customer_df = spark.read.format('parquet').option("header", True).schema(schema).load(customer_lookup_path)

# write into a temp view

customer_df.createOrReplaceTempView("customer_new_data")



# ### Creating Empty Bronze Table using try/catch exception:

# In[8]:


customer_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/customer_bronze"

try:
    spark.read.format('delta').load(customer_bronze_path).createOrReplaceTempView('customer_bronze')
except:
    create_table = f"""CREATE TABLE IF NOT EXISTS customer_bronze (
        customer_id INT,
        home_store INT,
        customer_first_name STRING,
        customer_email STRING,
        customer_since DATE,
        loyalty_card_number STRING,
        birthdate DATE,
        gender STRING,
        birth_year STRING,
        processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(customer_bronze_path).createOrReplaceTempView('customer_bronze')


# ### Upserting Logic for Insert/Update

# In[9]:


sql_statement = f""" MERGE INTO customer_bronze AS target
                     USING customer_new_data as source 
                     ON target.customer_id = source.customer_id

                     WHEN MATCHED THEN
                          UPDATE SET
                              target.home_store = source.home_store,
                              target.customer_first_name = source.customer_first_name,
                              target.customer_email = source.customer_email,
                              target.customer_since = source.customer_since,
                              target.loyalty_card_number = source.loyalty_card_number,
                              target.birthdate = source.birthdate,
                              target.gender = source.gender,
                              target.birth_year = source.birth_year,
                              target.processing_date = '{today_date}'

                     WHEN NOT MATCHED THEN
                     INSERT (customer_id, home_store, customer_first_name, customer_email, customer_since, loyalty_card_number, birthdate, gender, birth_year, processing_date)
                     VALUES(source.customer_id, source.home_store, source.customer_first_name, source.customer_email, source.customer_since, source.loyalty_card_number, source.birthdate, source.gender, source.birth_year, '{today_date}'
                     )"""

spark.sql(sql_statement).show()


# ### Reading employee_df from ADLS landing

# In[13]:


employee_lookup_path = f"{landing_path}employee_lookup/{partition_path}"


# ### Displaying employee_lookup_df schema

# ### Developing employee bronze schema

# In[11]:


employee_schema = StructType([
    StructField('staff_id', StringType(), True), 
    StructField('first_name', StringType(), True), 
    StructField('last_name', StringType(), True), 
    StructField('position', StringType(), True), 
    StructField('start_date', StringType(), True), 
    StructField('location', StringType(), True)])


# ### Assigning created employee_schema to employee_lookup_df

# In[12]:


employee_lookup_df = spark.read.format('parquet').option("header", True).schema(employee_schema).load(employee_lookup_path)


# ### write employee_lookup_df to a temp view

# In[13]:


employee_lookup_df.createOrReplaceTempView("employee_new_data")


# ### Creating Empty employee Bronze Table using try/catch exception:

# In[14]:


employee_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/employee_bronze"

try:
    spark.read.format("delta").load(employee_bronze_path).createOrReplaceTempView('employee_bronze')
except:
    create_table = f"""CREATE TABLE IF NOT EXISTS employee_bronze (
    staff_id INT,
    first_name STRING,
    last_name STRING,
    position STRING,
    start_date DATE,
    location INT,
    processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format("delta").load(employee_bronze_path).createOrReplaceTempView('employee_bronze')


# ### Upserting Logic for Insert/Update employee_bronze

# In[15]:


sql_statement = f""" MERGE INTO employee_bronze AS target
                     USING employee_new_data as source 
                     ON target.staff_id = source.staff_id

                     WHEN MATCHED THEN
                          UPDATE SET
                                target.first_name = source.first_name,
                                target.last_name = source.last_name,
                                target.position = source.position,
                                target.start_date = source.start_date,
                                target.location = source.location,
                                target.processing_date = '{today_date}'

                     WHEN NOT MATCHED THEN
                     INSERT(staff_id, first_name, last_name, position, start_date, location, processing_date)
                     VALUES(source.staff_id, source.first_name, source.last_name, source.position, source.start_date, source.location, '{today_date}'
                     )"""


spark.sql(sql_statement).show()


# ### Reading food_inventory from ADLS landing

# In[5]:


food_inventory_path = f"{landing_path}food_inventory/{partition_path}"


# ### Developing food inventory bronze schema

# In[6]:


food_inventory_schema = StructType([
    StructField('store_id', StringType(), True), 
    StructField('baked_date', StringType(), True), 
    StructField('transaction_date', StringType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('quantity_start_of_day', StringType(), True), 
    StructField('quantity_sold', StringType(), True)
])


# ### Assigning created food_inventory_schema to food_inventory_df

# In[7]:


food_inventory_df = spark.read.format('parquet').option("header", True).schema(food_inventory_schema).load(food_inventory_path)


# ### write food_inventory_df to a temp view

# In[8]:


food_inventory_df.createOrReplaceTempView("food_inventory_new_data")


# ### Creating empty food inventory Bronze Table using try/catch exception:

# In[9]:


food_inventory_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/food_inventory_bronze"

try:
    spark.read.format('delta').load(food_inventory_bronze_path).createOrReplaceTempView('food_inventory_bronze')
except:
    create_table = f"""CREATE TABLE IF NOT EXISTS food_inventory_bronze (
    store_id INT,
    baked_date DATE,
    transaction_date DATE,
    product_id INT,
    quantity_start_of_day INT,
    quantity_sold INT,
    processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(food_inventory_bronze_path).createOrReplaceTempView('food_inventory_bronze')


# ### Upserting Logic for Insert/Update into food_inventory_bronze

# ### 

# In[10]:


sql_statement = f""" MERGE INTO food_inventory_bronze AS target
                     USING food_inventory_new_data as source 
                     ON target.store_id = source.store_id
                     AND target.product_id = source.product_id
                     AND target.baked_date = source.baked_date
                     AND target.transaction_date = source.transaction_date

                     WHEN MATCHED THEN
                          UPDATE SET 
                               target.quantity_start_of_day = source.quantity_start_of_day,
                               target.quantity_sold = source.quantity_sold,
                               target.processing_date = '{today_date}'

                     WHEN NOT MATCHED THEN
                     INSERT(store_id, baked_date, transaction_date, product_id, quantity_start_of_day, quantity_sold, processing_date)
                     VALUES(source.store_id, source.baked_date, source.transaction_date, source.product_id, source.quantity_start_of_day, source.quantity_sold, '{today_date}'
                     )"""



spark.sql(sql_statement).show()


# ### Reading product_lookup from ADLS landing

# In[11]:


product_lookup_path = f"{landing_path}product_lookup/{partition_path}"


# ### Displaying product_lookup_df schema

# ### Creating a schema for product_lookup

# In[12]:


product_schema = StructType([
    StructField('product_id', StringType(), True), 
    StructField('product_group', StringType(), True), 
    StructField('product_category', StringType(), True), 
    StructField('product_type', StringType(), True), 
    StructField('product', StringType(), True), 
    StructField('product_description', StringType(), True), 
    StructField('unit_of_measure', StringType(), True), 
    StructField('current_cost', StringType(), True), 
    StructField('current_wholesale_price', StringType(), True), 
    StructField('current_retail_price', StringType(), True), 
    StructField('tax_exempt_yn', StringType(), True), 
    StructField('promo_yn', StringType(), True), 
    StructField('new_product_yn', StringType(), True)])


# ### Assigning product_schema to product_lookup_path

# In[13]:


product_lookup_df = spark.read.format('parquet').option("header", True).schema(product_schema).load(product_lookup_path)


# ### assigning product_lookup_df to a temp view

# In[14]:


product_lookup_df.createOrReplaceTempView("product_new_data")


# ### creating an empty product bronze table

# In[15]:


product_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/product_bronze"

try:
    spark.read.format('delta').load(product_bronze_path).createOrReplaceTempView("product_bronze")
except:
    
    create_table = f""" CREATE TABLE IF NOT EXISTS product_bronze (
     product_id STRING,
     product_group STRING,
     product_category STRING,
     product_type STRING,
     product STRING,
     product_description STRING,
     unit_of_measure STRING,
     current_cost DOUBLE,
     current_wholesale_price DOUBLE,
     current_retail_price DOUBLE,
     tax_exempt_yn STRING,
     promo_yn STRING,
     new_product_yn STRING,
     processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(product_bronze_path).createOrReplaceTempView("product_bronze")


# ### Upserting Logic for Insert/Update into product_bronze

# In[16]:


sql_statement = f""" MERGE INTO product_bronze AS target
                     USING product_new_data as source 
                     ON target.product_id = source.product_id

                     WHEN MATCHED THEN
                          UPDATE SET 
                              target.product_group = source.product_group,
                              target.product_category = source.product_category,
                              target.product_type = source.product_type,
                              target.product = source.product,
                              target.product_description = source.product_description,
                              target.unit_of_measure = source.unit_of_measure,
                              target.current_cost = source.current_cost,
                              target.current_wholesale_price = source.current_wholesale_price,
                              target.current_retail_price = source.current_retail_price,
                              target.tax_exempt_yn = source.tax_exempt_yn,
                              target.promo_yn = source.promo_yn,
                              target.new_product_yn = source.new_product_yn,
                              target.processing_date = '{today_date}'

                         WHEN NOT MATCHED THEN
                              INSERT (product_id, product_group, product_category, product_type, product, product_description, unit_of_measure, current_cost, current_wholesale_price,
                               current_retail_price, tax_exempt_yn, promo_yn, new_product_yn, processing_date)
                               VALUES (source.product_id, source.product_group, source.product_category, source.product_type, source.product, source.product_description, source.unit_of_measure, source.current_cost, source.current_wholesale_price,
                               source.current_retail_price, source.tax_exempt_yn, source.promo_yn, source.new_product_yn, '{today_date}'
                            )"""


spark.sql(sql_statement).show()


# ### reading sales by store data from ADLS container

# In[17]:


sales_path = f"{landing_path}sales_by_store/{partition_path}"


# ### displaying sales by store schema

# ### Creating a schema for sales df

# In[18]:


sales_schema = StructType([
    StructField('transaction_id', StringType(), True), 
    StructField('transaction_date', StringType(), True), 
    StructField('transaction_time', StringType(), True), 
    StructField('store_id', StringType(), True), 
    StructField('staff_id', StringType(), True), 
    StructField('customer_id', StringType(), True), 
    StructField('instore_yn', StringType(), True), 
    StructField('order', StringType(), True), 
    StructField('line_item_id', StringType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('quantity_sold', StringType(), True), 
    StructField('unit_price', StringType(), True), 
    StructField('promo_item_yn', StringType(), True)])


# ### assigning sales schema to sales path

# In[19]:


sales_df = spark.read.format('parquet').option("header", True).schema(sales_schema).load(sales_path)


# ### assigning sales df to a temp view

# In[20]:


sales_df.createOrReplaceTempView("sales_new_data")


# ### Creating empty bronze table for sales df using try/catch exception:

# In[21]:


sales_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/sales_bronze"

try:
    spark.read.format('delta').load(sales_bronze_path).createOrReplaceTempView("sales_bronze")

except:
    create_table = f"""CREATE TABLE IF NOT EXISTS sales_bronze (
    transaction_id INT,
    transaction_date DATE,
    transaction_time STRING,
    store_id INT,
    staff_id INT,
    customer_id INT,
    instore_yn STRING,
    order INT,
    line_item_id INT,
    product_id INT,
    quantity_sold INT,
    unit_price DOUBLE,
    promo_item_yn STRING,
    processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(sales_bronze_path).createOrReplaceTempView("sales_bronze")


# ### Upserting Logic for Insert/Update into sales_bronze

# In[22]:


sql_statement = f""" MERGE INTO sales_bronze AS target
                     USING sales_new_data as source 
                     ON target.transaction_id = source.transaction_id
                     AND target.transaction_date = source.transaction_date
                     AND target.transaction_time = source.transaction_time
                     AND target.order = source.order
                     AND target.line_item_id = source.line_item_id

                     WHEN MATCHED THEN
                          UPDATE SET 
                               target.store_id = source.store_id,
                               target.staff_id = source.staff_id,
                               target.customer_id = source.customer_id,
                               target.instore_yn = source.instore_yn,
                               target.product_id = source.product_id,
                               target.quantity_sold = source.quantity_sold,
                               target.unit_price = source.unit_price,
                               target.promo_item_yn = source.promo_item_yn,
                               target.processing_date = '{today_date}'
                    
                     WHEN NOT MATCHED THEN
                          INSERT(transaction_id, transaction_date, transaction_time, store_id, staff_id, customer_id,
                           instore_yn, order, line_item_id, product_id, quantity_sold, unit_price, promo_item_yn, processing_date)
                           VALUES(source.transaction_id, source.transaction_date, source.transaction_time, source.store_id, source.staff_id, source.customer_id, 
                           source.instore_yn, source.order, source.line_item_id, source.product_id, source.quantity_sold, source.unit_price, source.promo_item_yn, '{today_date}'
                )"""


spark.sql(sql_statement).show()


# ### Reading store lookup from ADLS container

# In[23]:


store_path = f"{landing_path}store_lookup/{partition_path}"


# ### Creating a schema for store df

# In[24]:


store_schema = StructType([
    StructField('store_id', StringType(), True), 
    StructField('store_type', StringType(), True), 
    StructField('store_square_feet', StringType(), True), 
    StructField('store_address', StringType(), True), 
    StructField('store_city', StringType(), True), 
    StructField('store_state_province', StringType(), True), 
    StructField('store_postal_code', StringType(), True), 
    StructField('store_longitude', StringType(), True), 
    StructField('store_latitude', StringType(), True), 
    StructField('manager', StringType(), True), 
    StructField('neighorhood', StringType(), True)])


# ### Assigning store_schema to store path

# In[25]:


store_df = spark.read.format('parquet').option("header", True).schema(store_schema).load(store_path)


# ### Assigning store df to a temp view

# In[26]:


store_df.createOrReplaceTempView("store_new_data")


# ### Creating an empty store bronze table using try/catch exception

# In[27]:


store_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/store_bronze"

try:
    spark.read.format('delta').load(store_bronze_path).createOrReplaceTempView("store_bronze")

except:
    create_table = f"""CREATE TABLE IF NOT EXISTS store_bronze (
    store_id INT,
    store_type STRING,
    store_square_feet INT, 
    store_address STRING,
    store_city STRING,
    store_state_province STRING,
    store_postal_code INT, 
    store_longitude DOUBLE, 
    store_latitude DOUBLE, 
    manager DOUBLE, 
    neighorhood STRING,
    processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(store_bronze_path).createOrReplaceTempView("store_bronze")


# ### Upserting Logic for Insert/Update into store_bronze

# In[28]:


sql_statement = f"""
    MERGE INTO store_bronze AS target
    USING store_new_data AS source
    ON target.store_id = source.store_id

    WHEN MATCHED THEN
        UPDATE SET
            target.store_type  = source.store_type,
            target.store_square_feet = source.store_square_feet,
            target.store_address = source.store_address,
            target.store_city = source.store_city,
            target.store_state_province = source.store_state_province,
            target.store_postal_code = source.store_postal_code,
            target.store_longitude = source.store_longitude,
            target.store_latitude = source.store_latitude,
            target.manager = source.manager,
            target.neighorhood = source.neighorhood,
            target.processing_date = '{today_date}'

    WHEN NOT MATCHED THEN
        INSERT (
            store_id, store_type, store_square_feet,
            store_address, store_city, store_state_province,
            store_postal_code, store_longitude, store_latitude,
            manager, neighorhood, processing_date
        )
        VALUES (
            source.store_id, source.store_type, source.store_square_feet,
            source.store_address, source.store_city, source.store_state_province,
            source.store_postal_code, source.store_longitude, source.store_latitude,
            source.manager, source.neighorhood, '{today_date}'
        )"""

spark.sql(sql_statement)


# ### 
