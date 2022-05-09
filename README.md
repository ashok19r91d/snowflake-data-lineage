# Snowflake Data Lineage

Snowflake dosen't provide any out of box solutions to verify or check data lineage, and this project establish Data Lineage between Snowflake Database Objects using Query History.

> For example:
>
> `insert into table1 select * from table2`
>
> From the above query, it is easy infer that `table1` is build using the data available in `table2`.
 
Extrapolation of same idea, we can build entire data lineage.

## Strategy
1. Read DML Queries from Query History of Snowflake Metadata Views
2. Parse Query (dependend on thirdparty library sqlparse)
3. Identify Source and Target tables for Each Query
4. Remove Duplicates Targets (Out of available source sets for a target select most recent source set)
5. Consolidate Source tables for each target table (Remove Deleted Table, View and Stage references, Remove Duplicates, Trim, Build Fully Qualified Object Names)
6. Build JSON
7. Build dotGraph visual
8. Save HTML file and Check output


## How to Run this Script
```
# python -m venv env  # (env is the environment name)
# \env\Scripts\activate
# pip install snowflake-connector-pyhton, sqlparse
# python app.py
# 
# (after the script ends, check outout html file.
```

## Credits:
On visualization part this project is inspired by the ideas in the post written by @Cristian Scutaru [How to Display Object Dependencies in Snowflake](https://medium.com/snowflake/how-to-display-object-dependencies-in-snowflake-43914a7fc275)
