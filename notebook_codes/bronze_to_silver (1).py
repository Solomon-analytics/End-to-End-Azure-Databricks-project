#!/usr/bin/env python
# coding: utf-8

# ## bronze_to_silver
# 
# null

# # **Silver Transformation**

# # **DATA Cleaning, Transformation & Standardisation**

# # **Framework used to ensure data quality, standardisation & completeness**
# 
# # 1. Duplicate detection and removal using combination of important keys & attributes
# # 2. Null checks on primary and mandatory columns
# # 3. Data type casting and format validation
# # 4. Standardisation - trim, case, special characters, dates
# # 5. Referential integrity checks against dimension tables

# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.types import *


# ### Parameter

# In[ ]:


today_date = ''
workspace = ''
lakehouse = ''


# In[5]:


##workspace = "b00274e4-e583-49e3-a029-066c995f631a"
##lakehouse = "55a2c543-ac85-4bb2-81a8-2bc862c1cc5d"


# ### Reading customer_bronze from bronze lakehouse

# In[3]:


customer_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/customer_bronze"

customers_df = spark.read.format('delta').load(customer_bronze_path).filter(F.col("processing_date") == str(today_date))


# ### customer_bronze Cleaning, Validation & Standardisation

# In[4]:


customers_count = customers_df.count()

# remove duplicates using important keys and attributes
def_cols = ["customer_id", "customer_since", "birthdate"]
customers_df_new = customers_df.dropDuplicates(def_cols)
print(f"before duplicate: {customers_count} | after duplicates: {customers_df_new.count()}")

# Trim white spaces
for trim_col in customers_df_new.columns: 
    customers_df_new = customers_df_new.withColumn(trim_col, F.trim(F.col(trim_col)))

# standardising column headers to lower case
for col_names in customers_df_new.columns:
        customers_df_new = customers_df_new.withColumnRenamed(col_names, col_names.lower())

# standardising column values to always have a title case
cols_to_standard = ["customer_first_name", "customer_email"]
for col_name in cols_to_standard:
    customers_df_new = customers_df_new.withColumn(col_name, F.initcap(F.trim(F.col(col_name)))).withColumnRenamed("customer_first-name", "customer_first_name")

# defining data validation rules for columns: customer_email, customer_since, birthdate, customer_id

# 1. Customer_email

# i. customer_email : check if there are more than one "@" or "." - if yes, drop to a single "@"
# ii. if there are no "@" or "." replace value as ""

customers_df_new = customers_df_new.withColumn(
    "customer_email",
    F.when(
        # rule 1: more than one "@" sign
        (F.size(F.split(F.col("customer_email"), "@")) - 1) > 1, F.lit("")
    ).when(
        # rule 2: more than one "." sign
        (F.size(F.split(F.col("customer_email"), "\\.")) - 1) > 1, F.lit("")
    ).when(
        # rule 3: no "@" sign at all
        ~F.col("customer_email").contains("@"), F.lit("")
    ).when(
        # rule 4: no "." sign at all
        ~F.col("customer_email").contains("."), F.lit("")
    ).otherwise(
        F.col("customer_email")
    )
)

# 2. customer_since & birthdate

# i. check for nulls
# ii. ensure all date formats are in "yyyy-MM-dd"
# iii. if value > today, set to ""
# iv. if birthdate < 1900-01-01, set to ""

cols_to_check = ["customer_since", "birthdate"]

for col_name_check in cols_to_check:
    customers_df_new = customers_df_new.withColumn(
    col_name_check,
    F.when(
        # rule 1: check if value is null or empty
        F.col(col_name_check).isNull() | (F.trim(F.col(col_name_check)) == ""), F.lit(None)
    ).otherwise(
        # rule 2: convert all values to standard date format
        F.coalesce(
            F.to_date(F.col(col_name_check), "yyyy-MM-dd"),
            F.to_date(F.col(col_name_check), "yyyy/MM/dd"),
            F.to_date(F.col(col_name_check), "yyyy-M-d"),
            F.to_date(F.col(col_name_check), "yyyy/M/d"),
            F.to_date(F.col(col_name_check), "MM-dd-yyyy"),
            F.to_date(F.col(col_name_check), "MM/dd/yyyy"),
            F.to_date(F.col(col_name_check), "M-d-yyyy"),
            F.to_date(F.col(col_name_check), "M/d/yyyy"),
            F.to_date(F.col(col_name_check), "dd-MM-yyyy"),
            F.to_date(F.col(col_name_check), "dd/MM/yyyy"),
            F.to_date(F.col(col_name_check), "d-M-yyyy"),
            F.to_date(F.col(col_name_check), "d/M/yyyy")
        )
    )
    ).withColumn(
    col_name_check,
    # rule 3: check the validity of the date in regards future and past
    F.when(
        F.col(col_name_check).cast('date') > F.current_date(),
        F.lit(None)
    ).when(
            F.col(col_name_check) < F.lit("1900-01-01").cast("date"),
        F.lit(None)
    ).otherwise(
        F.col(col_name_check)
)
)
    
# standardising column gender for future entries:
customers_df_new = customers_df_new.withColumn(
    "gender",
    F.when(
        F.col("gender").isin("m", "M", "Male", "MALE", "male"), "Male"
    ).when(
        F.col("gender").isin("f", "F", "Female", "female", "FEMALE"), "Female"
    ).when(
        F.col("gender").isin("o", "O", "other", "OTHER", "Other"), "Other"
    ).otherwise(F.col("gender"))
)



# drop columns with null customer_ids
customers_df_new = customers_df_new.withColumn(
    "customer_id_dq",
    F.when(
        F.col("customer_id").isNotNull(), "Y"
    ).otherwise("N")
)

customer_df_cleaned = customers_df_new.filter(F.col("customer_id_dq") == "Y").drop("customer_id_dq")
new_cust_count = customer_df_cleaned.count()

print(f"before cust_id drop: {customers_count} | after cust_id drop: {new_cust_count} | total dropped: {customers_count - new_cust_count}")


# ### Assigning customer_df to a temp view

# In[7]:


customers_df.createOrReplaceTempView("new_customers_data")


# ### Creating customer silver table using the try/catch exception:

# In[8]:


customer_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/dbo/customer_silver"

try:
    print('Reading data from customer_silver table: ')
    spark.read.format('delta').load(customer_silver_path).createOrReplaceTempView('customer_silver')
except:
    print("no table found, creating customer_silver table: ")
    create_table = f"""CREATE TABLE IF NOT EXISTS customer_silver (
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
    spark.read.format('delta').load(customer_silver_path).createOrReplaceTempView('customer_silver')


# ### Writing customer_df into customer_silver using the UPSERT Logic

# In[9]:


sql_statement = f""" MERGE INTO customer_silver AS target
                     USING new_customers_data as source 
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


# ### Reading employee_bronze from bronze lakehouse

# In[11]:


# employee bronze path
employees_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/employee_bronze"

employees_df = spark.read.format('delta').load(employees_bronze_path).filter(F.col("processing_date") == str(today_date))


# ### Data Cleaning, Validation & Standardisation

# In[12]:


emp_count = employees_df.count()

# remove duplicates using important keys and attributes
def_cols = ["staff_id", "start_date"]

employees_df = employees_df.dropDuplicates(def_cols)
print(f"before remove duplicates: {emp_count} | after: {employees_df.count()} | total dropped: {emp_count - employees_df.count()}")

# trim white spaces
for col_name in employees_df.columns:
    employees_df = employees_df.withColumn(col_name, F.trim(F.col(col_name)))

# standardising column headers to lower case
for col_names in employees_df.columns:
    employees_df = employees_df.withColumnRenamed(col_names, col_names.lower())

# standardise descriptive column values to always have a title case
col_to_standard = ["first_name", "last_name", "position"]
for col_names in col_to_standard:
    employees_df = employees_df.withColumn(col_names, F.initcap(F.trim(F.col(col_names))))

# defining validation rules for column: start date

# i. check for nulls
# ii. ensure all date formats are in "yyyy-MM-dd"
# iii. if value > today, set to ""
# iv. if birthdate < 1900-01-01, set to ""

employees_df = employees_df.withColumn(
    "start_date",
    F.when(
        # rule 1: check if value is null or empty
        F.col("start_date").isNull() | (F.trim(F.col("start_date")) == ""), F.lit(None)
    ).otherwise(
                # rule 2: convert all values to standard date format
            F.coalesce(
            F.to_date(F.col("start_date"), "yyyy-MM-dd"),
            F.to_date(F.col("start_date"), "yyyy/MM/dd"),
            F.to_date(F.col("start_date"), "yyyy-M-d"),
            F.to_date(F.col("start_date"), "yyyy/M/d"),
            F.to_date(F.col("start_date"), "MM-dd-yyyy"),
            F.to_date(F.col("start_date"), "MM/dd/yyyy"),
            F.to_date(F.col("start_date"), "M-d-yyyy"),
            F.to_date(F.col("start_date"), "M/d/yyyy"),
            F.to_date(F.col("start_date"), "dd-MM-yyyy"),
            F.to_date(F.col("start_date"), "dd/MM/yyyy"),
            F.to_date(F.col("start_date"), "d-M-yyyy"),
            F.to_date(F.col("start_date"), "d/M/yyyy")
        )
    )
).withColumn(
    "start_date",
    # rule 3: check the validity of the date in regards future and past
    F.when(
        F.col("start_date").cast('date') > F.current_date(),
        F.lit(None)
    ).when(
            F.col("start_date") < F.lit("1900-01-01").cast("date"),
        F.lit(None)
    ).otherwise(
        F.col("start_date")
)
)

# converting staff_id and location_id to string, since they are not needed for any numerical calculations:
cols_to_convert = ["staff_id", "location"]
for colname in cols_to_convert:
    employees_df = employees_df.withColumn(colname, F.col(colname).cast("string"))

# establishing a data flag rule based on staff_id
employees_df = employees_df.withColumn(
    "staff_id_dq",
    F.when(F.col("staff_id").isNotNull(), "Y").otherwise("N")
)

# dropping invalid rows based on rows with unknown or na staff_id
employees_df_cleaned = employees_df.filter(F.col("staff_id_dq") == "Y").drop("staff_id_dq")

print(f" Duplicate outcome: before = {emp_count} | after = {employees_df_cleaned.count()} | difference = {emp_count - employees_df_cleaned.count()}")



# ### Assigning employees_df to a temp view

# In[14]:


employees_df.createOrReplaceTempView('employee_new_data')


# ### Creating employee silver using the try/catch exception:

# In[16]:


employee_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/dbo/employee_silver"

try:
    print("Reading data from employee_silver table")
    spark.read.format('delta').load(employee_silver_path).createOrReplaceTempView('employee_silver')
except:
    print('no table found, creating employee_silver table: ')

    create_table = f""" CREATE TABLE IF NOT EXISTS employee_silver (
       staff_id STRING,
       first_name STRING,
       last_name STRING,
       position STRING,
       start_date DATE,
       location STRING,
       processing_date DATE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(employee_silver_path).createOrReplaceTempView('employee_silver')


# ### Writing employees_df into employee_silver using the UPSERT Logic

# In[17]:


sql_statement = f""" MERGE INTO employee_silver AS target
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


# ### Reading food_inventory bronze data from bronze lakehouse

# In[21]:


food_inventory_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/food_inventory_bronze"

inventory_df = spark.read.format('delta').load(food_inventory_bronze_path).filter(F.col("processing_date")==str(today_date))


# ### Data Cleaning, Validation and Standardisation

# In[24]:


inventory_count = inventory_df.count()

# remove dulicates using keys and important attributes:
dup_keys = ["store_id", "product_id", "baked_date", "transaction_date"]
inventory_df = inventory_df.dropDuplicates(dup_keys)

print(f"duplicates: before = {inventory_count} | after = {inventory_df.count()} | difference = {inventory_count - inventory_df.count()}")

# trim white spaces
for col_trim in inventory_df.columns:
    inventory_df = inventory_df.withColumn(col_trim, F.trim(F.col(col_trim)))

# defining validation rules for date columns:

# i. check for nulls
# ii. ensure all date formats are in "yyyy-MM-dd"
# iii. if value > today, set to ""
# iv. if birthdate < 1900-01-01, set to ""

def_cols = ["baked_date", "transaction_date"]
for datecols in def_cols:
    inventory_df = inventory_df.withColumn(
        datecols,
        F.when(
            # rule 1: check if value is null or empty
            F.col(datecols).isNull() | (F.col(datecols) == ""), F.lit(None)
        ).otherwise(
                # rule 2: convert all values to standard date format
            F.coalesce(
            F.to_date(F.col(datecols), "yyyy-MM-dd"),
            F.to_date(F.col(datecols), "yyyy/MM/dd"),
            F.to_date(F.col(datecols), "yyyy-M-d"),
            F.to_date(F.col(datecols), "yyyy/M/d"),
            F.to_date(F.col(datecols), "MM-dd-yyyy"),
            F.to_date(F.col(datecols), "MM/dd/yyyy"),
            F.to_date(F.col(datecols), "M-d-yyyy"),
            F.to_date(F.col(datecols), "M/d/yyyy"),
            F.to_date(F.col(datecols), "dd-MM-yyyy"),
            F.to_date(F.col(datecols), "dd/MM/yyyy"),
            F.to_date(F.col(datecols), "d-M-yyyy"),
            F.to_date(F.col(datecols), "d/M/yyyy")
        )
    )
).withColumn(
    datecols,
    # rule 3: check the validity of the date in regards future and past
    F.when(
        F.col(datecols).cast('date') > F.current_date(),
        F.lit(None)
    ).when(
            F.col(datecols) < F.lit("1900-01-01").cast("date"),
        F.lit(None)
    ).otherwise(
        F.col(datecols)
)
)

# convert columns to appropriate data type
cols_convert = ["quantity_start_of_day", "quantity_sold"]
for col_convert in cols_convert:
    inventory_df = inventory_df.withColumn(
        col_convert,
        F.col(col_convert).cast("integer")
)


# creating business columns: baked_date_id & transaction_date_id - this will used to create a relationship with a calendar table
inventory_df = inventory_df.withColumn(
    "baked_date_id",
    F.regexp_replace(F.col("baked_date"), "-", "").cast("string")
)

inventory_df = inventory_df.withColumn(
    "transaction_date_id",
    F.regexp_replace(F.col("transaction_date"), "-", "").cast("string")
)

# Creating a surrogate key to uniquely identify every row
inventory_df = inventory_df.withColumn(
    "inventory_id",
    F.sha2(
        F.concat_ws(
            "||",
            F.col("store_id"),
            F.col("product_id"),
            F.col("baked_date"),
            F.col("transaction_date")
        ),
        256
    )
)


# ### Assigning inventory_df to a temp view

# In[25]:


inventory_df.createOrReplaceTempView('new_inventory_data')


# ### Creating inventory_silver using the try/catch exception:

# In[26]:


inventory_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/dbo/inventory_silver"

try:
    print("Read data from inventory_silver: ")
    spark.read.format('delta').load(inventory_silver_path).createOrReplaceTempView('inventory_silver')
except:
    print("there is no table present - create inventory_silver table: ")
    create_table = f""" CREATE TABLE IF NOT EXISTS inventory_silver (
    store_id INT,
    baked_date DATE,
    transaction_date DATE,
    product_id INT,
    quantity_start_of_day INT,
    quantity_sold INT,
    processing_date DATE,
    baked_date_id STRING,
    transaction_date_id STRING,
    inventory_id STRING
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(inventory_silver_path).createOrReplaceTempView('inventory_silver')


# ### using inventory_df as source and inventory_silver as target - insert/update data using the UPSERT Logic

# In[27]:


sql_statement = f""" MERGE INTO inventory_silver AS target
                     USING new_inventory_data as source 
                     ON target.inventory_id = source.inventory_id

                     WHEN MATCHED THEN
                          UPDATE SET 
                               target.store_id = target.store_id,
                               target.baked_date = source.baked_date,
                               target.transaction_date = source.transaction_date,
                               target.product_id = source.product_id,
                               target.quantity_start_of_day = source.quantity_start_of_day,
                               target.quantity_sold = source.quantity_sold,
                               target.processing_date = '{today_date}',
                               target.baked_date_id = source.baked_date_id,
                               target.transaction_date_id = source.transaction_date_id

                     WHEN NOT MATCHED THEN
                     INSERT(store_id, baked_date, transaction_date, product_id, quantity_start_of_day, quantity_sold, processing_date, baked_date_id, transaction_date_id, inventory_id)
                     VALUES(source.store_id, source.baked_date, source.transaction_date, source.product_id, source.quantity_start_of_day, source.quantity_sold, '{today_date}',
                     source.baked_date_id, source.transaction_date_id, source.inventory_id
                     )"""



spark.sql(sql_statement).show()


# ### Reading product bronze from bronze lakehouse

# In[29]:


products_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/product_bronze"


products_df = spark.read.format('delta').load(products_bronze_path).filter(F.col("processing_date")== str(today_date))


# ### Data Cleaning, Validation & Standardisation

# In[ ]:


prd_count = products_df.count()

# drop duplicates
products_df = products_df.dropDuplicates(["product_id"])
print(f"duplicates: before = {prd_count} | after = {products_df.count()}")

# trim white spaces
for col_trim in products_df.columns:
    products_df = products_df.withColumn(col_trim, F.trim(F.col(col_trim)))

# convert columns to appropriate data type
cols_convert = ["current_cost", "current_wholesale_price", "current_retail_price"]
for col_convert in cols_convert:
    products_df = products_df.withColumn(col_convert, F.col(col_convert).cast("decimal(10,2)"))

# standardising columns
cols_to_standard = ["tax_exempt_yn", "promo_yn", "new_product_yn"]
for col_standard in cols_to_standard:
    products_df = products_df.withColumn(
        col_standard,
        F.when(
            F.upper(F.col(col_standard)).isin("Y", "YES", "TRUE"), "Y").otherwise("N")
)

# Creating new columns based off unit of measure

products_df = products_df.withColumn(
    # Clean up the raw value first
    "uom_cleaned", F.trim(F.col("unit_of_measure"))
).withColumn(
    # Extract numeric part (e.g. 1.5, 12, 0.9)
    "uom_value",
    F.regexp_extract(F.col("uom_cleaned"), r"([\d\.]+)", 1).cast("double")
).withColumn(
    # Extract unit part (oz, lb, pump, single)
    "product_original_uom",
    F.when(F.col("uom_cleaned").rlike(r"(?i)pump"), "pump")
     .when(F.col("uom_cleaned").rlike(r"(?i)single"), "single")
     .when(F.col("uom_cleaned").rlike(r"(?i)oz"), "oz")
     .when(F.col("uom_cleaned").rlike(r"(?i)lb"), "lb")
     .otherwise("unknown")
).withColumn(
    # Convert to lb
    "product_lb",
    F.when(F.col("product_original_uom") == "lb", F.col("uom_value"))
     .when(F.col("product_original_uom") == "oz", F.round(F.col("uom_value") / 16, 4))
     .otherwise(F.lit(None).cast("double"))  # pump/single = None
).drop("uom_cleaned", "uom_value")


# Creating data quality check for product_id
products_df = products_df.withColumn(
    "product_id_dq", 
    F.when(
        F.col("product_id").isNotNull(), "Y").otherwise("N")
)

# return only valid data
products_df = products_df.filter(F.col("product_id_dq") == "Y").drop("product_id_dq", "unit_of_measure")
print(f"valid data: before = {prd_count} | after = {products_df.count()} | difference = {prd_count - products_df.count()}")                                                


# ### Assigning products_df to a temp view

# In[ ]:


products_df.createOrReplaceTempView("product_new_data")


# ### Create a product_silver table using a try/catch exception:

# In[ ]:


product_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/dbo/product_silver"

try:
    print("loading product_silver table: ")
    spark.read.format('delta').load(product_silver_path).createOrReplaceTempView("product_silver")
except:
    print("product_silver table does not exist - creating product_silver")
    create_table = f""" CREATE TABLE IF NOT EXISTS product_silver (
     product_id STRING,
     product_group STRING,
     product_category STRING,
     product_type STRING,
     product STRING,
     product_description STRING,
     current_cost DECIMAL(10, 2),
     current_wholesale_price DECIMAL(10, 2),
     current_retail_price DECIMAL(10, 2),
     tax_exempt_yn STRING,
     promo_yn STRING,
     new_product_yn STRING,
     processing_date DATE,
     product_original_uom STRING,
     product_lb DOUBLE
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(product_silver_path).createOrReplaceTempView("product_silver")


# ### using product_new_data as source and product_silver as target - insert/update data using UPSERT Logic

# In[ ]:


sql_statement = f""" MERGE INTO product_silver AS target
                     USING product_new_data as source 
                     ON target.product_id = source.product_id

                     WHEN MATCHED THEN
                          UPDATE SET 
                              target.product_group = source.product_group,
                              target.product_category = source.product_category,
                              target.product_type = source.product_type,
                              target.product = source.product,
                              target.product_description = source.product_description,
                              target.current_cost = source.current_cost,
                              target.current_wholesale_price = source.current_wholesale_price,
                              target.current_retail_price = source.current_retail_price,
                              target.tax_exempt_yn = source.tax_exempt_yn,
                              target.promo_yn = source.promo_yn,
                              target.new_product_yn = source.new_product_yn,
                              target.processing_date = '{today_date}',
                              target.product_original_uom = source.product_original_uom,
                              target.product_lb = source.product_lb

                         WHEN NOT MATCHED THEN
                              INSERT (product_id, product_group, product_category, product_type, product, product_description, current_cost, current_wholesale_price,
                               current_retail_price, tax_exempt_yn, promo_yn, new_product_yn, processing_date, product_original_uom, product_lb)
                               VALUES (source.product_id, source.product_group, source.product_category, source.product_type, source.product, source.product_description, source.current_cost, source.current_wholesale_price,
                               source.current_retail_price, source.tax_exempt_yn, source.promo_yn, source.new_product_yn, '{today_date}', product_original_uom, product_lb
                            )"""


spark.sql(sql_statement).show()


# ### Reading sales bronze from bronze lakehouse

# In[ ]:


sales_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/sales_bronze"

sales_df = spark.read.format('delta').load(sales_bronze_path).filter(F.col("processing_date")==str(today_date))


# ### Data Cleaning. Validating, Standardising & Enrichment

# ### Sales Data Grain Investigation
# 
# ### Creating Surrogate Key
# 
# 1. The transaction_id column is not a transaction reference, this indicate why multiple dates, tills and products are attached to the same transaction_id. 
# 2. The `order` column represents the till number, where each till maintains its own independent `line_item_id` counter - meaning a group order can be split across multiple tills simultaneously. 
# 3. The true grain of the table is `transaction_id + transaction_date + transaction_time + order + line_item_id`, with `transaction_time` being the critical column that separates two genuine visits by the same customer on the same day.

# In[ ]:


sls_count = sales_df.count()

# creating a data quality column to ensure completeness
critical_columns = ["transaction_id", "product_id", "customer_id", "store_id", "staff_id", "transaction_date", "transaction_time"]
condition = F.col(critical_columns[0]).isNotNull()
for column in critical_columns[1:]:
    condition = condition & F.col(column).isNotNull()

sales_df = sales_df.withColumn(
    "sales_dq",
    F.when(condition, "Y").otherwise("N")
)

sales_df = sales_df.filter(F.col("sales_dq") == "Y").drop("sales_dq")
print(f"Valid rows check: before = {sls_count} | after = {sales_df.count()} | difference = {sls_count - sales_df.count()}")

# trim white spaces
for colname in sales_df.columns:
    sales_df = sales_df.withColumn(colname, F.trim(F.col(colname)))


# 2. validating date column and ensure it stays consistent

# i. check for nulls
# ii. ensure all date formats are in "yyyy-MM-dd"
# iii. if value > today, set to ""
# iv. if birthdate < 1900-01-01, set to ""

sales_df = sales_df.withColumn(
    "transaction_date",
    F.when(
        # rule 1: check if value is null or empty
        F.col("transaction_date").isNull() | (F.trim(F.col("transaction_date")) == ""), F.lit(None)
    ).otherwise(
        # rule 2: convert all values to standard date format
        F.coalesce(
            F.to_date(F.col("transaction_date"), "yyyy-MM-dd"),
            F.to_date(F.col("transaction_date"), "yyyy/MM/dd"),
            F.to_date(F.col("transaction_date"), "yyyy-M-d"),
            F.to_date(F.col("transaction_date"), "yyyy/M/d"),
            F.to_date(F.col("transaction_date"), "MM-dd-yyyy"),
            F.to_date(F.col("transaction_date"), "MM/dd/yyyy"),
            F.to_date(F.col("transaction_date"), "M-d-yyyy"),
            F.to_date(F.col("transaction_date"), "M/d/yyyy"),
            F.to_date(F.col("transaction_date"), "dd-MM-yyyy"),
            F.to_date(F.col("transaction_date"), "dd/MM/yyyy"),
            F.to_date(F.col("transaction_date"), "d-M-yyyy"),
            F.to_date(F.col("transaction_date"), "d/M/yyyy")
        )
    )
).withColumn(
    "transaction_date",
    # rule 3: check the validity of the date in regards future and past
    F.when(
        F.col("transaction_date").cast("date") > F.current_date(),
        F.lit(None)
    ).when(
        F.col("transaction_date") < F.lit("1900-01-01").cast("date"),
        F.lit(None)
    ).otherwise(
        F.col("transaction_date")
    )
)

# enforcing consistency across columns: 
col_to_enforce = ["instore_yn", "promo_item_yn"]
for enf_cols in col_to_enforce:
    sales_df = sales_df.withColumn(
        enf_cols,
        F.when(F.upper(F.col(enf_cols)).isin("Y", "YES", "TRUE"), "Y").otherwise("N")
    )

# convert columns to appropriate data type
cols_integer = ["order", "line_item_id", "quantity_sold"]
for colinteger in cols_integer:
    sales_df = sales_df.withColumn(colinteger, F.col(colinteger).cast("int"))

sales_df = sales_df.withColumn("unit_price", F.col("unit_price").cast("decimal(10, 2)"))


# Creating a surrogate key to uniquely identify every row
sales_df = sales_df.withColumn(
    "sales_key",
    F.sha2(
        F.concat_ws(
            "||",
            F.col("transaction_id"),
            F.col("transaction_date"),
            F.col("transaction_time"),
            F.col("order"),
            F.col("line_item_id")
        ),
        256
    )
)

# creating new column transaction_date_id
sales_df = sales_df.withColumn(
    "transaction_date_id",
    F.regexp_replace(F.col("transaction_date"), "-", "")
)


# ### Assigning sales_df to a temp view

# In[ ]:


sales_df.createOrReplaceTempView("sales_new_data")


# ### Create a sales_silver table using the try/catch exception:

# In[ ]:


sales_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/dbo/sales_silver"

try:
    print("loading sales_silver table: ")
    spark.read.format('delta').load(sales_silver_path).createOrReplaceTempView("sales_silver")
except:
    print("sales_silver does not exist - creating sales_silver table ")
    create_table = f""" CREATE TABLE IF NOT EXISTS sales_silver (
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
    processing_date DATE,
    sales_key STRING,
    transaction_date_id STRING
    )"""
    spark.sql(create_table)
    spark.read.format('delta').load(sales_silver_path).createOrReplaceTempView("sales_silver")


# ### using sales_silver as target and sales_new_data as source - insert/update data using UPSERT Logic

# In[ ]:


sql_statement = f""" MERGE INTO sales_silver AS target
                     USING sales_new_data as source 
                     ON target.sales_key = source.sales_key

                     WHEN MATCHED THEN
                          UPDATE SET 
                               target.transaction_id = source.transaction_id,
                               target.transaction_date = source.transaction_date,
                               target.transaction_time = source.transaction_time,
                               target.store_id = source.store_id,
                               target.staff_id = source.staff_id,
                               target.customer_id = source.customer_id,
                               target.instore_yn = source.instore_yn,
                               target.order = source.order,
                               target.line_item_id = source.line_item_id,
                               target.product_id = source.product_id,
                               target.quantity_sold = source.quantity_sold,
                               target.unit_price = source.unit_price,
                               target.promo_item_yn = source.promo_item_yn,
                               target.processing_date = '{today_date}',
                               target.transaction_date_id = source.transaction_date_id
                    
                     WHEN NOT MATCHED THEN
                          INSERT(transaction_id, transaction_date, transaction_time, store_id, staff_id, customer_id,
                           instore_yn, order, line_item_id, product_id, quantity_sold, unit_price, promo_item_yn, processing_date, sales_key, transaction_date_id)
                           VALUES(source.transaction_id, source.transaction_date, source.transaction_time, source.store_id, source.staff_id, source.customer_id, 
                           source.instore_yn, source.order, source.line_item_id, source.product_id, source.quantity_sold, source.unit_price, source.promo_item_yn, '{today_date}',
                           source.sales_key, source.transaction_date_id
                )"""


spark.sql(sql_statement).show()


# ### Reading store bronze data from bronze lakehouse

# In[ ]:


store_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/e7851d7a-ff4c-4847-bdc9-2787f1662e31/Tables/dbo/store_bronze"

store_df = spark.read.format('delta').load(store_bronze_path).filter(F.col("processing_date")==str(today_date))


# ### Data Cleaning, Standardising & Validation

# In[ ]:


store_count = store_df.count()

# remove duplicates
store_df_new = store_df.dropDuplicates(["store_id"])
print(f"before duplicate: {store_count} | after duplicates: {store_df_new.count()}")

# Trim white spaces
for trim_col in store_df_new.columns:
  store_df_new = store_df_new.withColumn(trim_col, F.trim(F.col(trim_col)))

# standardising column headers
for col_names in store_df.columns:
        store_df = store_df.withColumnRenamed(col_names, col_names.lower())


# drop columns with null store_ids
store_df = store_df.withColumn(
    "store_id_dq",
    F.when(F.col("store_id").isNotNull(), "Y").otherwise("N")
)

store_df = store_df.filter(F.col("store_id_dq") == "Y").drop("store_id_dq")

                           


# ### Assigning store_df to a temp view

# In[ ]:


store_df.createOrReplaceTempView("store_new_data")


# ### Creating store_silver using try/catch exception:

# In[ ]:


store_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}/Tables/dbo/store_silver"

try:
    print("loading store_silver table: ")
    spark.read.format('delta').load(store_silver_path).createOrReplaceTempView("store_silver")
except:
    print("store_silver does not exist - creating store_silver table: ")
    create_table = f"""CREATE TABLE IF NOT EXISTS store_silver (
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
    spark.read.format('delta').load(store_silver_path).createOrReplaceTempView("store_silver")


# ### Using store_silver as target and store_new_data as source - insert/update using UPSERT Logic

# In[ ]:


sql_statement = f"""
    MERGE INTO store_silver AS target
    USING store_new_data AS source
    ON target.store_id = source.store_id

    WHEN MATCHED THEN
        UPDATE SET
            target.store_type = source.store_type,
            target.store_square_feet = source.store_square_feet,
            target.store_address = source.store_address,
            target.store_city  = source.store_city,
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

spark.sql(sql_statement).show()


# ### 
