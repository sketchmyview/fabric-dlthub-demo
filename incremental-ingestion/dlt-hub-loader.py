#!/usr/bin/env python
# coding: utf-8

# ## dlt-hub-loader
# 
# New notebook

# In[ ]:


get_ipython().system('pip install dlt')


# In[ ]:


import os
import dlt
import requests
import duckdb


# In[ ]:


md_token = notebookutils.credentials.getSecret("https://{akv_name}.vault.azure.net/", "mdtoken")


# In[ ]:


conn = duckdb.connect(f'md:?motherduck_token={md_token}')


# In[ ]:


os.environ["DESTINATION__MOTHERDUCK__CREDENTIALS__PASSWORD"] = md_token


# In[ ]:


import dlt
from dlt.sources.filesystem import filesystem, read_csv_duckdb


# In[ ]:


filesystem_pipe = filesystem(
  bucket_url="/lakehouse/default/Files/autoloader/repossession_data",
  file_glob="*.csv"
)

filesystem_pipe.apply_hints(incremental=dlt.sources.incremental("modification_date"))

reader = (filesystem_pipe | read_csv_duckdb())

pipeline = dlt.pipeline(pipeline_name="dlt_autoloaderdemo", destination="motherduck")

info = pipeline.run(reader, write_disposition="append", table_name="dltautoloader")

#print(info)


# In[ ]:


data = pipeline.last_trace.last_extract_info.metrics
# Extracting table_metrics
table_metrics = data[list(data.keys())[0]][0]['table_metrics']

print(table_metrics)

