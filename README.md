# doit-ascend-dbt-bakeoff
A repository for the related codes/processes run for the DoiT Ascend vs dbt bakeoff article

# Introduction
Note that these queries go along with an upcoming blog entry about comparing the use of Ascend.io vs dbt written by Matthew Richardson of DoiT International.

These queries include the setup of test Dataflows in Ascend.io, a test DAG in dbt Cloud and queries run via the BigQuery Information Schema and Snowflake Account Usage views to assess slot utilization and credit usage for both services for either Data Warehouse Platform respectively.

# Usage
The codes used in this example are fairly simplified and tests included on either side of solution in final repo have been kept fairly generic (i.e. to not-null or unique tests), however the examples here can be easily tweaked to add further logic or tests into either the Ascend Dataflows or the dbt configuration for our sample DAG.

The source files in the weather_source_files folder give sample weather data files from July 31st through to August 19th to provide sample data to test the two pipelines. The pipelines can be configured to read this data from load into BigQuery directly or from GCS with tweaks to the configuration (the example pipelines included in this repo use the data directly from BigQuery). In terms of the data provided in the source folder, this was taken from the weather data included in the Public GSOD BigQuery weather dataset.

# Ascend.io notes
The Ascend folder for this repo contains the Dataflow elements for running the example given in the blog entry for the BigQuery Data Warehouse service specifically, the same codes used should also be transferrable to running the same processes via Snowflake Data Warehouse however, so codes/other config settings are the same for both warehouses tested in the blog post.

Please note that you will also need to set up an Ascend.io account to follow this example through for your DWH of choice, please access the link at - https://www.ascend.io/signup/ - if you do not already have an Ascend account.

# dbt Cloud notes
The dbt folder contains the dbt Cloud project layout for the equivalent process to the Ascend one above, used in the blog post. This structure should be configurable however if you are using dbt Core rather than dbt Cloud, however if you want to follow this example through using dbt core, please be aware that some further config files or variables may be needed to be added into your environment to get the scripts running from your side.

In my example dbt environment, I followed the jaffle_shop sandbox example to set up my underlying dbt project as per the documentation here - https://github.com/dbt-labs/jaffle-shop - to replicate this side of the article you will need to populate this with your own credentials accordingly as per the guides below.

For info on setting up a dbt Cloud account, please see info at - https://docs.getdbt.com/docs/cloud/about-cloud-setup
For info on setting up dbt Core, please see info for that at - https://docs.getdbt.com/docs/core/about-core-setup

# Query costs
The queries run within my blog post should be relatively benign cost-wise in BigQuery and Snowflake, however in terms of BigQuery do ensure that you look at the bytes scanned estimates when running any queries and continually monitor slot usage. Within Snowflake, the credits consumed by these queries should be covered by a Free Trial account, however if you have a Production account with setup, please be aware of your credit costs within Snowflake when running through this demo in either Ascend or dbt.

# Contributing
If you see any bugs please feel free to reach out to myself at matthew@doit.com or perform a pull request on the code and make any tweaks necessary.
