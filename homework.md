# AWS Glue (Serverless Spark)

## Overview

Create a Glue 4.0 job using Glue Studio to parse Apache/Nginx Combined Log Format lines from S3 into a typed DataFrame using built‑in Spark functions. Convert timestamps, derive partition columns, and write Parquet to S3. Register the output with Glue (Crawler or external table) and validate by running Athena queries.

**About the Data Format:** Files are in JSON Lines format (`.json.gz`), where each line is a JSON object containing a `raw_line` field with an Apache/Nginx Combined Log Format string. This format allows Glue Job Bookmarks to track processed files while still requiring you to parse the log format using regex.

**Example JSON line:**
```json
{"raw_line": "203.0.113.42 - - [24/Nov/2025:16:23:45 +0000] \"GET /api/users HTTP/1.1\" 200 1234 \"https://example.com/home\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64)\""}
```

Your Glue job must:
1. Read the JSON file and extract the `raw_line` field
2. Parse the Combined Log Format string inside `raw_line` using regex

**About Combined Log Format:** This is a standard web server log format that extends the Common Log Format with referrer and user-agent fields. Each log line contains: client IP, identifiers, timestamp, HTTP request, status code, bytes sent, referrer, and user agent. See: https://httpd.apache.org/docs/current/logs.html#combined

Constraints and defaults
- Region: use `us-west-2` for all region‑scoped CLI/API steps (course default)
- AWS Academy: do not create IAM users/roles; use pre‑created `LabRole`
- Glue config: Glue 4.0, worker type G.1X (or Standard), start with 2 workers, max concurrency = 1
- S3 outputs: your own bucket, e.g., `s3://your-bucket/wk09/glue_output/logs_parsed/`

References (official docs)
- Glue Studio (create job): https://docs.aws.amazon.com/glue/latest/ug/console-jobs.html
- Glue start job run (CLI): https://docs.aws.amazon.com/cli/latest/reference/glue/start-job-run.html
- Glue get job run (CLI): https://docs.aws.amazon.com/cli/latest/reference/glue/get-job-run.html
- Glue get job runs (CLI): https://docs.aws.amazon.com/cli/latest/reference/glue/get-job-runs.html
- S3 list (CLI): https://docs.aws.amazon.com/cli/latest/reference/s3/ls.html
- Glue Crawler: https://docs.aws.amazon.com/glue/latest/ug/add-crawler.html
- Athena start query (CLI): https://docs.aws.amazon.com/cli/latest/reference/athena/start-query-execution.html
- Spark SQL functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

---

## Part 1 — Prepare Input Data

Shared dataset available in two batches for incremental processing validation:
- Initial load: `s3://csed516-shared-resources-au2025-sharedclassbucket-mc0zal7gjcmu/class09/logs_json/batch1/`
- Incremental: `s3://csed516-shared-resources-au2025-sharedclassbucket-mc0zal7gjcmu/class09/logs_json/batch2/`

**Important**: Copy batch1 to your own bucket so your Glue job reads from a location you control. This ensures consistent behavior when you add batch2 later for bookmark validation.

Steps
1) Confirm you can list files in both batch prefixes in the shared bucket
2) Copy batch1 to your own bucket's input prefix:
   ```bash
   aws s3 cp s3://csed516-shared-resources-au2025-sharedclassbucket-mc0zal7gjcmu/class09/logs_json/batch1/ s3://your-bucket/wk09/input/ --recursive --region us-west-2
   ```
3) Verify the copy succeeded by listing your input prefix

For Part 2-3, configure your Glue job to read from `s3://your-bucket/wk09/input/` (your bucket, not the shared bucket). You'll add `batch2/` to this same destination later.

---

## Part 2 — Author a Glue Job

Goal: Build a Glue 4.0 job to parse Combined Log Format into a typed DataFrame and write partitioned Parquet.

You can create the job via:
- **Option A:** Glue Studio web console (recommended)
- **Option B:** AWS CLI using `aws glue create-job` (see AWS CLI reference docs)

Configuration
- Runtime: Glue 4.0 (Spark 3.x, Python 3.10)
- IAM role: `LabRole` (pre‑created; do not create new roles)
- Worker type/count: G.1X (1 DPU per worker), 2 workers (= 2 DPUs total)
- **Max concurrency: 1** (prevents multiple runs from processing overlapping files)
  - **Note:** If using Glue Studio (Option A), set this in the "Job details" tab
  - **If using CLI:** Max concurrency can only be set via console or `aws glue update-job` after creation. For this assignment, you can omit it during creation since you'll manually trigger runs one at a time.
- **Job bookmark: Enable** (`--job-bookmark-option job-bookmark-enable` in job parameters)
  - **CRITICAL FOR BOOKMARKS:** Job bookmarks only track state for Glue DynamicFrames, not native Spark DataFrames
  - **REQUIRED:** You must also include the `transformation_ctx` parameter in all DynamicFrame read/write operations (see code pattern below). Without it, bookmarks are disabled for those operations.
- **Python packages:** Glue 4.0 includes all required packages (`awsglue`, `pyspark`, `boto3`, etc.) by default. No `--extra-py-files` or `--additional-python-modules` needed for this assignment.

Logic requirements
- **Read: Use GlueContext.create_dynamic_frame.from_options()** with format="json" and connection_type="s3"
  - **CRITICAL:** Job bookmarks only track state for Glue DynamicFrames, not Spark DataFrames
  - If you use `spark.read.json()` instead, bookmarks will NOT work and your second run will reprocess all data
  - Convert to Spark DataFrame for transformations: `df = dynamic_frame.toDF()`
  - Extract the `raw_line` field: `df = df.select("raw_line")`
- Parse: use `regexp_extract` on the `raw_line` column to extract groups into columns
  - Columns: host, ident, user, ts_str, method, path, protocol, status (INT), bytes (BIGINT), referrer, user_agent
  - Handle `bytes` where `-` becomes NULL
- Timestamp: convert `ts_str` with `to_timestamp(ts_str, 'dd/MMM/yyyy:HH:mm:ss Z') AS ts`
- Partitions: derive `year`, `month`, `day`, `hour`
- **Write: Convert back to DynamicFrame before writing** using `glueContext.write_dynamic_frame.from_options()`
  - Convert DataFrame → DynamicFrame: `DynamicFrame.fromDF(final_df, glueContext, "output")`
  - Write Parquet to `s3://your-bucket/wk09/glue_output/logs_parsed/` partitioned by year/month/day/hour

**About transformation_ctx:**
The `transformation_ctx` parameter is **required** for job bookmarks to work. According to AWS documentation: "If you don't pass in the `transformation_ctx` parameter, then job bookmarks are not enabled for a dynamic frame or a table used in the method."

Key requirements:
- Each `transformation_ctx` value must be unique across your script (e.g., "read_json_logs", "write_parquet_logs")
- The context serves as the identifier that AWS Glue uses to track which data has been processed
- Once set, keep the `transformation_ctx` consistent across job runs - changing it breaks bookmark tracking
- You must include it in both read and write DynamicFrame operations for bookmarks to function properly

Without `transformation_ctx`, your Glue job will reprocess all input files on every run, defeating the purpose of bookmarks.

**Code pattern for bookmark tracking:**
```python
from awsglue.dynamicframe import DynamicFrame

# Read with DynamicFrame (required for bookmarks)
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="json",
    transformation_ctx="read_json_logs"  # REQUIRED: Unique identifier for bookmark tracking
)

# Define regex pattern to match against
pattern = r'enter_pattern_here'

# Convert to DataFrame for transformations
df = dynamic_frame.toDF()

# Extract the raw_line field from JSON
df = df.select("raw_line")

# Apply regex to parse the raw_line column
df = df.select(
    regexp_extract("raw_line", pattern, 1).alias("host"),
    regexp_extract("raw_line", pattern, 2).alias("ident"),
    # ... continue for all capture groups ...
)

# ... perform type conversions, timestamp parsing, partition derivation here ...

# Convert back to DynamicFrame before writing
output_dyf = DynamicFrame.fromDF(final_df, glueContext, "output")

# Write using Glue writer (preserves bookmark state)
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={"path": output_path, "partitionKeys": ["year", "month", "day", "hour"]},
    format="parquet",
    transformation_ctx="write_parquet_logs"  # REQUIRED: Unique identifier for bookmark tracking
)
```

Hints
- Recommended regex (Python raw‑string):
  - `r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+) "([^"]*)" "([^"]*)"'`
- Column mapping by pattern position: host, ident, user, ts_str, method, path, protocol, status, bytes_str, referrer, user_agent
- Timestamp format: `dd/MMM/yyyy:HH:mm:ss Z`
- Convert `bytes_str` to `bytes BIGINT`, mapping `'-'` to NULL before cast

---

## Part 3 — Run and Monitor the Job

Goal: Execute the Glue job and capture one successful run.

Steps
1) Start a job run and record the returned JobRunId
2) Poll job run status until `SUCCEEDED`
3) List recent job runs and note start/end times and final state

---

## Part 4 — Verify S3 Outputs

Goal: Confirm Parquet files exist at the output prefix with the expected partition layout.

Steps
1) List objects recursively under `wk09/glue_output/logs_parsed/`
2) Observe partition directory keys (year=YYYY/month=MM/day=DD/hour=HH) and file sizes by listing the contents of the S3 destination

---

## Part 5 — Register Schema for Athena

Goal: Create an Athena table pointing to your Parquet outputs using explicit DDL.

**Why DDL instead of Crawler:** DDL gives you explicit control over the schema and partition structure. While Glue Crawlers can work, they sometimes miss new partitions created by subsequent job runs. DDL + MSCK REPAIR TABLE is more reliable for this assignment.

Steps:
1) Create the Glue database if it doesn't exist:
   ```bash
   aws glue create-database --database-input '{"Name":"wk09_logs"}' --region us-west-2
   ```
   (If it already exists, you'll get an error - that's fine, continue to step 2)

2) Run this DDL in Athena (via console or CLI) to create the external table:
   ```sql
   CREATE EXTERNAL TABLE IF NOT EXISTS wk09_logs.logs_parsed (
     host STRING,
     ident STRING,
     user STRING,
     ts_str STRING,
     method STRING,
     path STRING,
     protocol STRING,
     status INT,
     referrer STRING,
     user_agent STRING,
     ts TIMESTAMP,
     bytes BIGINT
   )
   PARTITIONED BY (year STRING, month STRING, day STRING, hour STRING)
   STORED AS PARQUET
   LOCATION 's3://your-bucket/wk09/glue_output/logs_parsed/';
   ```
   **Important:** Replace `your-bucket` with your actual bucket name.

3) Discover partitions created by your Glue job:
   ```sql
   MSCK REPAIR TABLE wk09_logs.logs_parsed;
   ```

4) Verify partitions were discovered:
   ```sql
   SHOW PARTITIONS wk09_logs.logs_parsed;
   ```
   You should see entries like: `year=2025/month=11/day=24/hour=16`

**After each subsequent Glue job run (e.g., Part 7), you MUST re-run MSCK REPAIR TABLE to discover new partitions. Athena doesn't automatically detect new partition directories.**

---

## Part 6 — Validate with Athena Queries

Goal: Query the Parquet dataset to establish a baseline row count before adding batch2. Use either the web UI or follow the below steps to use the CLI.

Query:
```sql
SELECT COUNT(*) FROM wk09_logs.logs_parsed;
```

CLI Steps:
1) Create a workgroup for this assignment:
   ```bash
   aws athena create-work-group --name wk09_workgroup \
     --configuration "ResultConfiguration={OutputLocation=s3://your-bucket/athena-results/}" \
     --region us-west-2
   ```

2) Start the query via the Athena CLI using `--work-group` parameter

You are expecting 1,650,000 rows from processing the initial batch.

---

## Part 7 — Validate Incremental Processing

Goal: Demonstrate that Job Bookmarks skip already-processed files on subsequent runs.

Steps:

1) Copy batch2 files to your input prefix:
   ```bash
   aws s3 cp s3://csed516-shared-resources-au2025-sharedclassbucket-mc0zal7gjcmu/class09/logs_json/batch2/ s3://your-bucket/wk09/input/ --recursive --region us-west-2
   ```

2) Start a second job run (same job, same command as Part 3)

3) Wait for SUCCEEDED status and record the new JobRunId

4) Refresh partition metadata (required for Athena to see new partitions):
   ```sql
   MSCK REPAIR TABLE wk09_logs.logs_parsed;
   ```

5) Verify new partitions are registered:
   ```sql
   SHOW PARTITIONS wk09_logs.logs_parsed;
   ```
   You should see both old AND new partitions listed.

6) Run another Athena COUNT(*) query:
   ```sql
   SELECT COUNT(*) FROM wk09_logs.logs_parsed;
   ```
**Expected result:** final count = 2,500,000 rows
  - Baseline (batch1): 1,650,000 rows
  - Increase (batch2): 850,000 rows
  - Validation: Final count = baseline + batch2 (NOT doubled, NOT unchanged)

**Bookmark Validation Matrix:**
| Final Count | Partition Behavior | Diagnosis |
|------------|-------------------|-----------|
| Baseline + New | Only new partitions created | Bookmarks working correctly |
| Baseline × 2 | Old partitions recreated | All files reprocessed |
| Baseline (unchanged) | No new partitions | Batch2 not processed OR partitions not discovered |

---

## Part 8 — Teardown

Because none of these resources are reoccuring compute services, they won't incur any charges when idle. 

As such, there is not a strong requirement to teardown these resources unless you want to keep your account tidy.

---

## Common Issues

**Glue job fails with "Access Denied"**: Ensure LabRole has S3 read/write access to your bucket. Check that both input and output bucket paths are accessible. Test with: `aws s3 ls s3://your-bucket/wk09/ --region us-west-2`

**Regex extracts empty strings**: Your regex isn't matching the input format. Test locally first (see Part 2 regex testing section). Common mistake: applying `regexp_extract` to the DataFrame instead of the `"raw_line"` column. Use: `regexp_extract(col("raw_line"), pattern, 1)` not `regexp_extract(df, pattern, 1)`.

**COUNT(*) returns 0 after running Glue job successfully**:
- **Diagnosis:** Athena doesn't know about your partitions yet
- **Fix:** Run `MSCK REPAIR TABLE wk09_logs.logs_parsed` in Athena
- **Verify:** Run `SHOW PARTITIONS wk09_logs.logs_parsed` - should list partition paths
- **Prevention:** Remember to run MSCK REPAIR after EVERY Glue job run

**Bookmarks not working (wrong row count in Part 7)**:
| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| Final count = baseline × 2 | Used `spark.read.json()` instead of `GlueContext.create_dynamic_frame` OR missing `transformation_ctx` parameter | Rewrite job to use DynamicFrame for reading AND writing with `transformation_ctx` parameter (see Part 2 code pattern) |
| Final count = baseline (unchanged) | Batch2 files not copied OR partitions not discovered | Verify batch2 in S3 input prefix, then run MSCK REPAIR TABLE |
| S3 shows duplicate partitions | Job reprocessed all files | Check bookmark is enabled in job definition, verify using DynamicFrames with `transformation_ctx` |

**New partitions not appearing in Athena after Part 7**:
1. Confirm partitions exist in S3: `aws s3 ls s3://your-bucket/wk09/glue_output/logs_parsed/ --recursive`
2. Run `MSCK REPAIR TABLE wk09_logs.logs_parsed`
3. Verify with `SHOW PARTITIONS wk09_logs.logs_parsed`
4. If MSCK doesn't find them, manually add partitions:
   ```sql
   ALTER TABLE wk09_logs.logs_parsed ADD
   PARTITION (year='2025', month='11', day='24', hour='16')
   LOCATION 's3://your-bucket/wk09/glue_output/logs_parsed/year=2025/month=11/day=24/hour=16/';
   ```

**Glue job runs but produces no output**:
- Check CloudWatch Logs for the job run (link in Glue console or via CLI)
- Common causes: input path doesn't exist, no data matches filters, error in transformation logic
- Add `print(df.count())` statements in your script to debug data flow

---

## Graded Deliverables

- S3URI for your finished AWS Glue ETL Job Script
- S3URI for the path of your post-processed Parquet files
- Narrative for reflecting on Spark, between 400-600 words.
  - Write a cohesive narrative that addresses each of the topics below. Integrate your technical understanding with personal reflection on your development experience. You may organize the content as you see fit, but ensure all six topics are meaningfully addressed.
    - Why use Spark versus other data services?
    - Why was Spark necessary for this assignment?
    - If you were to try again with a different data service, which one would you try?
    - Thoughts on the bookmark feature?
    - What made this assignment challenging?
    - How much time did you spend on this assignment?
