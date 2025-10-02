# Assignment on Parquet, Polars and DuckDB

## Submitting the Assignment

You will follow the submission process for all labs and assignments:

- Add all your requested files to the GitHub assignment repo for the appropriate deliverable.
- Submit a final commit message called "final-submission" to your repo. This is critical so that instructional team can evaluate your work. Do not change your GitHub repo after submitting the "final-submission" commit message

Make sure you commit **only the files requested**, and push your repository to GitHub!

* `metadata.json`
* `metadata.ipynb`
* `compression_type.txt` (optional)
* `ls.txt`
* `quazyilx_polars.ipynb`
* `anomalies.csv`
* `readings_per_hour.csv`
* `quazyilx_duckdb.ipynb`
* `quazyilx_advanced_duckdb.ipynb`
* `measurement_correlations.csv`
* `time_patterns.csv`
* `hourly_timeseries.png`

### Reminders

* All files must be within the repository directory otherwise git will not see them.
* Commit frequently on the master node and push back to GitHub as you are doing your work.

## Prerequisites and Setup

### Development Environment
1. **AWS EC2 Instance**: Use an Amazon EC2 `t3.large` instance
2. **IDE**: Connect to the EC2 instance using VSCode with the Remote-SSH extension
3. **Python**: Python 3.10 or higher with uv package manager

### Setting up VSCode with EC2
1. Launch an EC2 `t3.large` instance
2. Configure security group to allow SSH access (port 22)
3. Install VSCode and the Remote-SSH extension on your local machine
4. Connect to your EC2 instance using VSCode Remote-SSH

### Install uv package manager and setup project
```bash
# On your EC2 instance, install uv:
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone this repository
git clone <your-repo-url>
cd assignment-polars-duckdb

# Install all dependencies using uv
uv sync

# Activate the virtual environment
source .venv/bin/activate

# Install Jupyter kernel for the notebooks
python -m ipykernel install --user --name=polars-duckdb --display-name="Polars-DuckDB"
```

**Important:** When running the notebooks, make sure to select the "Polars-DuckDB" kernel to use the correct environment with all dependencies.

## Background - Amazon S3 bucket for this assignment

For this assignment, you'll need to create an S3 bucket to store your data. Use the naming convention: `assignment-polars-duckdb-<your-account-id>`, for example, if your AWS account id is `123456789012`, then create a bucket named `assignment-polars-duckdb-123456789012`.

Create your bucket using the AWS CLI:
```bash
aws s3 mb s3://assignment-polars-duckdb-<your-account-id>
```

We will be using this bucket for storing and processing data throughout the assignment.



## Problem 1 - Read parquet file metadata (15 points)

Recall from the lecture that parquet files have [`metadata`](https://parquet.apache.org/docs/file-format/metadata/).The metadata information can used to determine the shape of the data, compression type etc. In this problem we will read the metadata and extract some key fields to save to a file.

1. Create a notebook called `metadata.ipynb` in your EC2 environment. Use Jupyter with the "Polars-DuckDB" kernel that you installed during setup.

1. Read the metadata of the `s3://bigdatateaching/nyc_tlc/trip_data/yyyy=2021/yellow_tripdata_2021-01.parquet` file.

1. Convert the metadata to a JSON and extract the `created_by`, `num_columns`, `num_rows`, `num_row_groups`, `format_version`, `serialized_size` fields from the metadata JSON and save them to a file called `metadata.json`. Your `metadata.json` file should look like this:

    ```
    {
        "created_by": ...
        "num_columns": ..
        "num_rows": ..
        "num_row_groups": ..
        "format_version": ..
        "serialized_size": ..
    }
    ```

Note that you can read the number of rows and columns from this metadata. This can be very useful in case of large datasets to get a sense of how much data you are working with. ***You need to checkin the `metadata.json` file and the `metadata.ipynb` file as part of this problem***.

**HINT**: You would need to use the [`PyArrow`](https://arrow.apache.org/docs/python/index.html) package for this, see [ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html). You would also have to use `S3FileSystem`. These packages are already included in the project dependencies.


### Get compression type for a parquet file

The metadata information also contains the compression type for the file. What is the compression used for this file? Extract it programatically from the metadata and save the compression type to a file called `compression_type.txt`.


 ## Problem 2 - Working with the `quazyilx` dataset (25 points)

For this problem, you will be working with data from the _quazyilx_ instrument. The files you will use contain hypothetic measurements of a scientific instrument called a _quazyilx_ that has been specially created for this class. Every few seconds the quazyilx makes four measurements: _fnard_, _fnok_, _cark_ and _gnuck_. The output looks like this:

    
    YYYY-MM-DDTHH:MM:SSZ fnard:10 fnok:4 cark:2 gnuck:9
    

(This time format is called [ISO-8601](https://en.wikipedia.org/wiki/ISO_8601) and it has the advantage that it is both unambiguous and that it sorts properly. The Z stands for _Greenwich Mean Time_ or GMT, and is sometimes called _Zulu Time_ because the [NATO Phonetic Alphabet](https://en.wikipedia.org/wiki/NATO_phonetic_alphabet) word for **Z** is _Zulu_.)

There are four different versions of the _quazyilx_ file, each of a different size. As you can see in the output below the file sizes are 50MB (1,000,000 rows), 4.8GB (100,000,000 rows), 18GB (369,865,098 rows) and 36.7GB (752,981,134 rows). The only difference is the length of the number of records, the file structure is the same.

These files are available in `s3://bigdatateaching/quazyilx/`.

    
    !aws s3 ls s3://bigdatateaching/quazyilx/ --request-payer requester
    
    
    2018-01-25 10:36:00          0
    2018-01-25 10:37:21   52443735 quazyilx0.txt
    2018-01-25 10:37:28 5244417004 quazyilx1.txt
    2018-01-25 10:38:01 19397230888 quazyilx2.txt
    2018-01-25 10:41:08 39489364082 quazyilx3.txt
    
### Problem 2 part 1 - Data preparation

The objective is to read data from `s3://bigdatateaching/quazyilx/quazyilx0.txt` file into a dataframe and save it as a parquet file `s3://assignment-polars-duckdb-<your-account-id>/quazyilx0.parquet`, this will be done in the following steps.

1. Create a notebook called `quazyilx_polars.ipynb` in your EC2 environment for this problem. Use Jupyter with the "Polars-DuckDB" kernel.

1. Run the `head` command on the `s3://bigdatateaching/quazyilx/quazyilx0.txt` to see a glimpse of the contents of this file. You would need to understand this content to read this file into a Polars dataframe.

    ```
    !aws s3 cp s3://bigdatateaching/quazyilx/quazyilx0.txt - --request-payer requester | head
    ```

1. Copy the `s3://bigdatateaching/quazyilx/quazyilx0.txt` to your S3 bucket. Due to S3 permissions, you'll need to do this in two steps. ***Replace the `your-account-id` part with your actual account-id***.

   ```bash
   # Step 1: Download the file locally
   aws s3 cp s3://bigdatateaching/quazyilx/quazyilx0.txt /tmp/quazyilx0.txt --request-payer requester

   # Step 2: Upload to your bucket
   aws s3 cp /tmp/quazyilx0.txt s3://assignment-polars-duckdb-<your-account-id>/quazyilx0.txt
   ```

1. Read the `quazyilx0.txt` file from your bucket (you copied it there in the previous step) into a Polars dataframe. This is a text file, so you would need to use the [`read_csv`](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.read_csv.html) or `scan_csv`, pay attention to the `separator` parameter, do you need to set it to a non-default value?

1. Once the data in `quazyilx0.txt` is read into a dataframe, use Polars to convert this dataframe into a new dataframe that looks as shown below. *Notice the column names and notice the data types of the column names*. 

    This will require you to create data processing pipeline like we did in the lab, this would include combining columns, doing operations on string, and changing data types amongst others. You would probably need to use [`concat_str`](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.concat_str.html), [`replace_all`](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.Expr.str.replace_all.html) and [`cast`](https://pola-rs.github.io/polars-book/user-guide/expressions/casting/#numerics).

    ```
    date_time            fnard    fnok    cark    gnuck
    datetime[μs]           i32      i32   i32      i32
    2000-01-01 00:00:03    7        8     19       25
    2000-01-01 00:00:08    14       19    16       37
    2000-01-01 00:00:17    12       11    12       8
    2000-01-01 00:00:22    18       16    3        8
    2000-01-01 00:00:32    7        16    7        37
    ```
1. Save the dataframe created in the previous step to a file called `quazyilx0.parquet` in s3 as `s3://assignment-polars-duckdb-<your-account-id>/quazyilx0.parquet`. You would need to experiment with the `use_pyarrow` parameter of the [`write_parquet`](https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.write_parquet.html) function to be able to do this. ***We will use this file in the next problem***.

1. Get the size of the `quazyilx0.parquet` that you just saved using the following command.

    ```
    !aws s3 ls s3://assignment-polars-duckdb-<your-account-id>/quazyilx0.parquet > ls.txt
    ```

1. ***You need to checkin the `ls.txt` file and the `quazyilx_polars.ipynb` file as part of this problem***.

### Problem 2 part 2 - Finding anomalies

When the quazyilx instrument malfunctions, then all four measurements _fnard_, _fnok_, _cark_ and _gnuck_ are equal to -1. 

Find all the anomalies in the `quazyilx0.txt` file, at this time you already have the data read into a Polars dataframe, anomalies are all the rows where _fnard_, _fnok_, _cark_ and _gnuck_ are all equal to -1. 

Find the anomalies and save them to a file called `anomalies.csv`. ***You need to checkin the `anomalies.csv` file as part of this problem***.

## Problem 3 - Find out the number of readings per hour using DuckDB (20 points)

**Important:** This problem depends on the `quazyilx0.parquet` file that you create in Problem 2 Part 1. Make sure you have successfully completed Problem 2 Part 1 before starting this problem.

1. Create a notebook called `quazyilx_duckdb.ipynb` in your EC2 environment for this problem. Use Jupyter with the "Polars-DuckDB" kernel. All necessary packages are already included in the project dependencies.
1. Use the `s3://assignment-polars-duckdb-<your-account-id>/quazyilx0.parquet` from Problem 2 and DuckDB to find out the count of readings (i.e. number of rows) per hour of every year, month and day in the dataset. **It would be good to review the date time manipulation examples we saw while working on the DuckDB lab**. The resulting dataframe should be as follows:

    ```
    year	month   day	hour	count
    2000	3       4	14	    546
    2000	3       4	13	    656
    2000	3       4	12	    656
    2000	3       4	11	    650
    2000	3       4	10	    653
    ...
    ...
    ```
1. Save this dataframe to a file called `readings_per_hour.csv`. ***You need to checkin the `readings_per_hour.csv` and `quazyilx_duckdb.ipynb` files as part of this problem***.

## Problem 4 - Advanced Analytics with DuckDB (40 points)

In this problem, you'll perform more complex analytical queries using DuckDB to understand patterns in the quazyilx instrument measurements.

**Important:** This problem depends on the `quazyilx0.parquet` file from Problem 2 Part 1.

1. Create a notebook called `quazyilx_advanced_duckdb.ipynb` in your EC2 environment. Use Jupyter with the "Polars-DuckDB" kernel.

2. Using DuckDB and the `s3://assignment-polars-duckdb-<your-account-id>/quazyilx0.parquet` file, perform the following analyses:

### Analysis 1: Time Series Visualization
Create a time series chart showing average values per hour for each measurement (fnard, fnok, cark, gnuck) for the time period from 2000-01-01 to 2000-01-08 (first week of January 2000). This visualization will help you understand the hourly patterns and variations in the quazyilx instrument readings.

Save the time series chart as `hourly_timeseries.png`.

### Analysis 2: Moving Averages and Volatility
Calculate 7-day moving averages for each measurement (fnard, fnok, cark, gnuck) and identify periods of high volatility (when any measurement deviates more than 50% from its moving average).

Expected output structure:
```
date        fnard_ma7  fnok_ma7  cark_ma7  gnuck_ma7  volatility_flag
2000-01-07  12.5       13.2      11.8      20.1       false
2000-01-08  13.1       12.9      12.3      19.8       true
...
```

### Analysis 3: Correlation Analysis
Find the correlation between different measurements over time. Specifically:
- Calculate daily averages for each measurement
- Compute correlation coefficients between all pairs of measurements
- Identify the pair with the highest correlation

Save the correlation matrix to `measurement_correlations.csv`.

### Analysis 4: Time-based Pattern Detection
Identify patterns in the measurements:
- Find the hour of day with the highest average readings for each measurement
- Find days where all four measurements peaked simultaneously (all in top 10% of their respective ranges)
- Calculate the percentage of readings that occur during "business hours" (9 AM - 5 PM)

Save the pattern analysis results to `time_patterns.csv`.

## SQL Hints and Documentation

This problem requires advanced SQL techniques. Here are detailed hints with links to DuckDB documentation:

### **Time Series and Date Functions:**
- **DATE_TRUNC()**: [DuckDB Date/Time Functions](https://duckdb.org/docs/sql/functions/date) - Use `DATE_TRUNC('hour', date_time)` for hourly aggregation
- **EXTRACT()**: [Extract Function](https://duckdb.org/docs/sql/functions/date#extract) - Use `EXTRACT(hour FROM date_time)` to get hour of day
- **Date Filtering**: Use `WHERE date_time >= '2000-01-01' AND date_time < '2000-01-08'` for specific date ranges

### **Window Functions (Analysis 2):**
- **Moving Averages**: [DuckDB Window Functions](https://duckdb.org/docs/sql/window_functions) - Use `AVG() OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`
- **LAG/LEAD**: [Window Function List](https://duckdb.org/docs/sql/window_functions#window-function-list) - For comparing previous values

### **Statistical Functions (Analysis 3):**
- **CORR()**: [Statistical Functions](https://duckdb.org/docs/sql/functions/statistical) - Calculate correlation between columns
- **PERCENTILE_CONT()**: [Percentile Functions](https://duckdb.org/docs/sql/functions/statistical#percentile_cont) - For finding top 10% thresholds
- **STDDEV()**: Standard deviation for volatility analysis

### **Advanced SQL Patterns:**
- **CTEs (Common Table Expressions)**: [WITH Clause](https://duckdb.org/docs/sql/query_syntax/with) - Use `WITH table_name AS (SELECT ...)` to organize complex queries
- **Aggregation**: [GROUP BY](https://duckdb.org/docs/sql/query_syntax/groupby) - Essential for daily/hourly summaries
- **Joins**: [Join Operations](https://duckdb.org/docs/sql/query_syntax/from#joins) - For combining different analysis results

### **Example Query Pattern:**
```sql
WITH daily_data AS (
    SELECT
        DATE_TRUNC('day', date_time) as date,
        AVG(fnard) as fnard_avg
    FROM quazyilx0
    GROUP BY DATE_TRUNC('day', date_time)
),
moving_avg AS (
    SELECT
        date,
        fnard_avg,
        AVG(fnard_avg) OVER (
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as fnard_ma7
    FROM daily_data
)
SELECT * FROM moving_avg;
```

## Alternative Implementation

**Note**: While this problem is designed to showcase DuckDB's SQL capabilities, students who prefer to use **Polars** can implement the same analyses using Polars' DataFrame operations. The conceptual approach remains the same:

- **Time Series**: Use Polars' `group_by_dynamic()` for time-based aggregation
- **Moving Averages**: Use `rolling()` functions for window operations
- **Correlations**: Use `corr()` method on DataFrames
- **Statistical Analysis**: Use Polars' built-in statistical functions

**Deliverables:**
- `quazyilx_advanced_duckdb.ipynb` - Completed notebook with all analyses
- `measurement_correlations.csv` - Correlation matrix between measurements
- `time_patterns.csv` - Time-based pattern analysis results
- `hourly_timeseries.png` - Time series chart showing hourly variations
