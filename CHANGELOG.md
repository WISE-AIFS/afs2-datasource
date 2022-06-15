# Change Log
All notable changes to this project will be documented in this file.

## [3.8.2] - 2022-06-15
### Remove
- Azure Blob not support `is_file_exist` function

## [3.8.1] - 2022-06-08
### Changed
- Merge 3.7.2 & 3.7.3

## [3.8.0] - 2021-12-28
### Added
- #22412 - OracleDB Helper
- #22413 - SQLServer Helper
## [3.7.4] - 2022-05-12
### Fixed
- Azure Blob download folder error

## [3.7.3] - 2022-01-06
### Fixed
- DBManager no event loop is running error

## [3.7.2] - 2022-01-05
### Fixed
- S3 Return fixed bucket order

## [3.7.1] - 2021-11-25
### Fixed
- Datahub uri none type error

## [3.7.0] - 2021-09-28
### Added
- #19554 - DataHub support InfluxDB
- #20401 - MySql Helper
- #20773 - S3 helper support AWS S3

## [3.6.0] - 2021-06-08
### Added
- #18389 - APM3.0 + Datahub3.0
- #18718 - DataHub Helper
- #19203 - Upgrade pandas version

### Fixed
- Fix S3 Bucket forbidden


## [3.4.2] - 2020-12-02
- Fix Regex bug

## [3.4.1] - 2020-12-01
- AzureBlobHelper & S3Helper
  - exceute_query() return dataframe if only download one csv file

## [3.4.0] - 2020-11-30
- AzureBlobHelper & S3Helper
  - check container name & bucket name

## [3.3.5.6] - 2020-10-27
### Added
- DBManager
  - encrypt data_dir
  - execute_query() support user provided query
- MongoHelper
  - support ISODate query string

## [3.2.5] - 2020-07-14
### Changed
- APMHelper
  - For EnSaaS 4.0 APM

## [2.1.28] - 2019-12-06
### Added
- S3Helper
    - Add is_verify
- Delete table, bucket or container
- Delete record in Postgres and MongoDB, and file in S3 and AzureBlob
### Changed
- APMHelper
    - init
    - execute_query return format

## [2.1.27] - 2019-08-27
### Changed
- S3Helper & AzureBlobHelper
    - execute_query()

## [2.1.25] - 2019-08-06
### Changed
- S3Helper
  - config

## [2.1.23] - 2019-07-24
### Added
- AzureBlobHelper

## [2.1.20] - 2019-07-01
### Added
- APMDSHelper
  - time last

## [2.1.19] - 2019-06-24
### Added
- S3Helper
  - is_file_exist
  - delete_file
- APMDSHelper
  - execute_query

## [2.1.18] - 2019-06-10
### Added
- DBManager support S3

## [2.1.17] - 2019-05-30
### Added
- Init DBManager with local config

## [2.1.15] - 2019-05-16
### Added
- DBManager
  - create_table(table_name, columns)
  - is_table_exist(table_name)
  - insert(table_name, columns, records

## [2.1.14] - 2019-05-13
### Added
- DBManger
  - DBManager()
  - connect()
  - execute_query()
