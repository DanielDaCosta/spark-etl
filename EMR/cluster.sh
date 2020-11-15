#!/bin/bash
aws emr create-default-roles

aws emr create-cluster --name cluster-sparkify \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 2 \
--applications Name=Spark \
--bootstrap-actions Path=s3://pyspark-etl/bootstrap_emr.sh \
--ec2-attributes KeyName=MyKeyPair \
--instance-type m5.xlarge \
--instance-count 3