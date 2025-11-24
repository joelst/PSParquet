# Versions

0.3.0
* Null value support - properly preserves null values instead of converting to defaults
* DateTime type detection - automatically detects and properly types DateTime values
* Configurable compression - added CompressionMethod and CompressionLevel parameters
* Enhanced type support - added Boolean and Guid type handling
* Improved error handling - proper PowerShell error records instead of console output
* Input validation - validates empty arrays and missing properties

# 

0.2.17
* Get-ParquetFileInfo added

0.2.16
* Support for multiple RowGroups added for Import-Parquet

0.2.0:
* Project made Open Source

0.1.0:
* Parquet.Net updated to 4.16.4
* Implemented low level API
* Export-Parquet InputObject takes values from pipeline
