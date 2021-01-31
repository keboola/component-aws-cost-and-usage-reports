This extractor downloads AWS CUR reports exported to S3 in CSV format. 

### Prerequisite: The AWS setup

First, the CUR report exports need to be set up in the AWS account to be exported to S3 bucket 
in the selected granularity and CSV format. Follow this [guide](https://docs.aws.amazon.com/cur/latest/userguide/cur-create.html)
 to set up the export.
 
 Export Setup:
 
 - Setup S3 bucket
 - Set the report prefix
 - Select granularity
 - Select report versioning (overwrite recommended)
 - Choose GZIP compression type
 