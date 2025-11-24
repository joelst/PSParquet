# PSParquet

<div style="text-align:center">
<img src="./.media/PowerShellLovesParquet600.png" alt="PowerShellLovesParquet"/>
</div>

## Import and Export Data to and from Parquet Files  
   
This PowerShell module is a free and open source tool that allows you to easily import and export data to and from Parquet files directly from the command line.

It's built on the [Parquet.Net library](https://github.com/aloneguid/parquet-dotnet) developed by https://github.com/aloneguid
   
## Advantages of Using this Module  
   
- **Efficient Storage:** Parquet is a columnar storage format that is highly optimized for performance and space efficiency. This module makes it easy to store and retrieve very large datasets efficiently.  

- **Null Value Support:** Properly handles null values in your data, preserving them instead of converting to default values.

- **Smart Type Detection:** Automatically detects data types including DateTime, Boolean, Guid, and numeric types for accurate data representation.

- **Configurable Compression:** Choose from multiple compression algorithms (Gzip, Snappy, Brotli, Zstd, Lzo) and compression levels to optimize for speed or file size.
   
- **Open Source and Free:** The module is free and open source, making it accessible to everyone and giving users the ability to contribute and improve the code.  
   
- **Easy to Use:** With this module, you can easily import and export data to and from Parquet files directly from the command line. This makes it an ideal tool for ETL.  
   
- **Works with Microsoft Fabric's OneLake Client:** This module is perfect for modern big data implementations using Microsoft products like Microsoft Fabric's OneLake client, as well as other Cloud native tools in Azure.  
   
## How to Use  
   
To get started, simply install the module from the PowerShell Gallery:  
   
```powershell  
Install-Module PSParquet  
```  
   
Once installed, you can use the `Import-Parquet` and `Export-Parquet` cmdlets to import and export data to and from Parquet files.   
  
For example, to import data from a Parquet file:  
   
```powershell  
Import-Parquet -FilePath "path/to/file.parquet"  
```  
   
And to export data to a Parquet file:  
   
```powershell  
Export-Parquet -InputObject $data -FilePath "path/to/file.parquet"  
```

### Advanced Export Options

Export with custom compression settings:

```powershell
# Use Snappy compression for faster writes
Export-Parquet -InputObject $data -FilePath "data.parquet" -CompressionMethod Snappy

# Use maximum compression
Export-Parquet -InputObject $data -FilePath "data.parquet" -CompressionLevel SmallestSize

# Disable compression for maximum speed
Export-Parquet -InputObject $data -FilePath "data.parquet" -CompressionMethod None
```

Available compression methods: `None`, `Gzip` (default), `Snappy`, `Brotli`, `Zstd`, `Lzo`  
Available compression levels: `Optimal` (default), `Fastest`, `NoCompression`, `SmallestSize`

### Handling Null Values and Data Types

The module now handles null values and automatically detects data types:

```powershell
$data = 1..100 | ForEach-Object {
    [PSCustomObject]@{
        Date        = Get-Date
        Name        = "Item $_"
        Value       = if ($_ % 3 -eq 0) { $null } else { $_ }  # Nulls preserved
        IsActive    = ($_ % 2 -eq 0)  # Boolean type detected
        Id          = [Guid]::NewGuid()  # Guid type supported
    }
}

Export-Parquet -InputObject $data -FilePath "data.parquet"
$imported = Import-Parquet -FilePath "data.parquet"
# Null values and types are preserved correctly
```

### Get File Information

Inspect Parquet file metadata and schema:

```powershell
$info = Get-ParquetFileInfo -FilePath "data.parquet"
$info.RowGroupCount
$info.Schema | Format-Table Name, Type
```

Use the `Get-Help` cmdlet for more details:

```powershell
Get-Help Export-Parquet -Full
Get-Help Import-Parquet -Full
Get-Help Get-ParquetFileInfo -Full
```
   
## Contributions  
   
Contributions to this open source module are always welcome! If you find a bug or have an idea for an improvement, you can submit an issue on the GitHub repository. If you'd like to contribute code, you can fork the repository, make your changes, and submit a pull request. The project's README.md file contains information on how to build and test the module locally, which can help you get started. By contributing to this module, you can help make it even more useful and accessible to others in the community.

## Build and test

The project consists of 2 parts:

* A PowerShell module compiler that builds the C# code, wraps the dlls in a PowerShell and handles documentation.
* A C# project for developing the binary PowerShell module


### PowerShell module compiler

The PowerShell module compiler uses the modules psake, Pester and platyPS. Make sure these modules are installed.

To compile the module, simply run the following command in from the project root folder:

```powershell
Invoke-psake ./PSParquet.psake.ps1 Test    
```

Look in the psake file for a details on the tasks it carries out.

Running the build increments the build version. To avoid that, set the environment variable `$env:psakeDeploy` to true

### C# project

* Load the solution in src/PSParquet. The project is a standard binary PowerShell module project.
* Setup The debugger with a Executable profile that launches PowerShell with NoProfile and imports the DLL from the debug folder

Once the C# code has been updated with the desired changes, the PowerShell module compiler packages and exports the module to the output/PSParquet folder. This folder then have a working PowerShell module with everything required for import or upload to the PowerShell Gallery.

## License

This PowerShell module is released under the [MIT license](https://github.com/username/repo/blob/master/LICENSE). This means that it is free to use, modify, and distribute, even for commercial purposes, as long as the original license is included. By using this module, you agree to the terms and conditions of the MIT license. If you plan to contribute to the code, please make sure to read the license carefully to understand your rights and obligations.

## Donations

While this module is free and open source, donations are always appreciated to support its development and maintenance. If you find this module useful and would like to say thanks, you can [buy me a coffee](https://www.buymeacoffee.com/axely) or make a donation of your choice. Your contribution will help ensure that this module continues to be maintained and improved for the benefit of the community. Thank you for your support!