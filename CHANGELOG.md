# Change Log
All notable changes to this project will be documented in this file.
## [2.1.28] - 2019-09-12
### Feature
- Sync apm configuration with afs portal and os environment (for demo only)

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
