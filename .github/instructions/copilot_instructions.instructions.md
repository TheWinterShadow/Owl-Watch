---
applyTo: '**'
---
This project is a data pipeline that takes data in from an S3 bucket and sends it to Glue Jobs for ETL. The cdk folder has AWS CDK in typescript, the execution and integration_tests folders are python code and the tests folder has unit tests for all three folders. Also each time you need to run a python command use hatch, do not try to just run python on its own.