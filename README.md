# Import CSV files into HBase using Spark


This is a Spark application in java. I have coded it in generic way so it could handle any CSV file schema without requiring a code change. This doesn't perform any transformations of the given data though, just plain import CSV as such to a HBase table.

Note: The HBase table needs to be created before execution.

## Input parameters

The csv files to import, the hbase table name & schema are provided in the YAML file `params.yml` and passed to the application. The `rowKey` and the `value` under `rowValues` corresponds to the CSV file headers. The input could also be a pattern like `worldcupplayerinfo_*.csv` to import data from multiple files into the table.

Have written a [blog](http://sathish.me/java/2017/09/24/import-data-from-csv-files-to-hbase-using-spark.html) explaining the code, execution & debugging. Feel free to modify & use based on your requirements. Let me know if there is something that I can help with.
