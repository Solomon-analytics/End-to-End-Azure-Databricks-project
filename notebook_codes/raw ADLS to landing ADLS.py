#!/usr/bin/env python
# coding: utf-8

# ## raw ADLS to landing ADLS
# 
# null

# In[2]:


from pyspark.sql import functions as F


# # **Notebook Parameters**

# In[12]:


processed_date = ''
batch_month = ''
source_account = ''
source_container = ''
destination_account = ''
destination_container = ''


# In[ ]:


##processed_date = ''
##batch_month = ''
##source_account = ""
##source_container = "raw"
##destination_account = ""
##destination_container = "landing"


# # **Authenticating Connection with ADLS Storage/Container**

# In[10]:


# storage account details
raw_path = f"abfss://{source_container}@{source_account}.dfs.core.windows.net/"
landing_path = f"abfss://{destination_container}@{destination_account}.dfs.core.windows.net/"


# Access keys
storage_account_key = ""
account_fqdn = f"{source_account}.dfs.core.windows.net"

# set authentication
spark.conf.set(
    f"fs.azure.account.key.{account_fqdn}",
    storage_account_key
)


# ### Reading customer data from ADLS

# In[11]:


customer_lookup_path = f"{raw_path}{batch_month}/customer_lookup-{batch_month}.csv"

customer_df = spark.read.format('csv') \
    .option("header", True) \
    .load(customer_lookup_path)

customer_df.schema


# ### Data table check - add new column (processing_date) for partioning purpose

# In[5]:


if len(customer_df.take(1)) > 0:
    print("file contains data")
    """
    if file contains data, then add a new column, processing_date for partitioning purposes and write outcome to ADLS landing
    """
    customer_df = customer_df.withColumn(
        "processing_date",
        F.lit(processed_date)
    )

    customer_df.write.format('parquet').option("header", True).partitionBy('processing_date').mode("append").save(f"{landing_path}customer_lookup")
else:
    print("file contains no data")


# ### Reading employee data from ADLS Container

# In[6]:


employee_lookup_path = f"{raw_path}{batch_month}/employee_lookup-{batch_month}.csv"

employee_df = spark.read.format('csv') \
    .option("header", True) \
    .load(employee_lookup_path)


# ### Data table check - add new column (processing_date) for partitioning purpose

# In[7]:


if len(employee_df.take(1)) > 0:
    print("file contains data")
    """
    if file contains data, then add a new column, processing_date for partitioning purposes and write outcome to ADLS landing
    """
    employee_df = employee_df.withColumn(
        "processing_date",
        F.lit(processed_date)
    )

    employee_df.coalesce(1).write.format('parquet').option("header", True).partitionBy('processing_date').mode("append").save(f"{landing_path}employee_lookup")
else:
    print("file contains no data")


# ### Reading food_inventory data from ADLS

# In[5]:


food_inventory_path = f"{raw_path}{batch_month}/food_inventory-{batch_month}.csv"

food_inventory_df = spark.read.format('csv') \
    .option("header", True) \
    .load(food_inventory_path)


# ### Data table check - add new column (processing_date) for partitioning purpose

# In[6]:


if len(food_inventory_df.take(1)) > 0:
    print("file contains data")
    """
    if file contains data, then add a new column, processing_date for partitioning purposes and write outcome to ADLS landing
    """
    food_inventory_df = food_inventory_df.withColumn(
        "processing_date",
        F.lit(processed_date)
    )

    food_inventory_df.coalesce(1).write.format('parquet').option("header", True).partitionBy('processing_date').mode("append").save(f"{landing_path}food_inventory")
else:
    print("file contains no data")


# ### Reading product_lookup data from ADLS

# In[10]:


product_lookup_path = f"{raw_path}{batch_month}/product_lookup-{batch_month}.csv"

product_lookup_df = spark.read.format('csv') \
    .option("header", True) \
    .load(product_lookup_path)


# ### Data table check - add new column (processing_date) for partitioning purpose

# In[11]:


if len(product_lookup_df.take(1)) > 0:
    print("file contains data")
    """
    if file contains data, then add a new column, processing_date for partitioning purposes and write outcome to ADLS landing
    """
    product_lookup_df = product_lookup_df.withColumn(
        "processing_date",
        F.lit(processed_date)
    )

    product_lookup_df.coalesce(1).write.format('parquet').option("header", True).partitionBy('processing_date').mode("append").save(f"{landing_path}product_lookup")
else:
    print("file contains no data")


# ### Reading sales_by_store data from ADLS

# In[12]:


sales_by_store_path = f"{raw_path}{batch_month}/sales_by_store-{batch_month}.csv"

sales_by_store_df = spark.read.format('csv') \
    .option("header", True) \
    .load(sales_by_store_path)


# ### Data table check - add new column (processing_date) for partitioning purpose

# In[13]:


if len(sales_by_store_df.take(1)) > 0:
    print("file contains data")
    """
    if file contains data, then add a new column, processing_date for partitioning purposes and write outcome to ADLS landing
    """
    sales_by_store_df = sales_by_store_df.withColumn(
        "processing_date",
        F.lit(processed_date)
    )

    sales_by_store_df.coalesce(1).write.format('parquet').option("header", True).partitionBy('processing_date').mode("append").save(f"{landing_path}sales_by_store")
else:
    print("file contains no data")


# ### Reading store_lookup data from ADLS

# In[14]:


store_lookup_path = f"{raw_path}{batch_month}/store_lookup-{batch_month}.csv"

store_lookup_df = spark.read.format('csv') \
    .option("header", True) \
    .load(store_lookup_path)


# ### Data table check - add new column (processing_date) for partitioning purpose

# In[15]:


if len(store_lookup_df.take(1)) > 0:
    print("file contains data")
    """
    if file contains data, then add a new column, processing_date for partitioning purposes and write outcome to ADLS landing
    """
    store_lookup_df = store_lookup_df.withColumn(
        "processing_date",
        F.lit(processed_date)
    )

    store_lookup_df.coalesce(1).write.format('parquet').option("header", True).partitionBy('processing_date').mode("append").save(f"{landing_path}store_lookup")
else:
    print("file contains no data")

