<#
.SYNOPSIS
    Tests the PSParquet module package for Azure Automation compatibility.

.DESCRIPTION
    This script validates the module package structure and tests basic functionality
    to ensure it will work correctly when uploaded to Azure Automation.
    Uses PowerShell jobs to avoid assembly loading conflicts.

.PARAMETER PackagePath
    Path to the .zip package file. If not specified, looks for the package in ./output

.EXAMPLE
    .\Test-AzureAutomationPackage.ps1

.EXAMPLE
    .\Test-AzureAutomationPackage.ps1 -PackagePath "C:\Packages\PSParquet.zip"
#>

[CmdletBinding()]
param(
    [Parameter()]
    [string]$PackagePath
)

$ErrorActionPreference = 'Stop'

Write-Host "=== PSParquet Azure Automation Package Validator ===" -ForegroundColor Cyan
Write-Host ""

# Find package if not specified
if (-not $PackagePath) {
    $outputPath = Join-Path $PSScriptRoot "output"
    $PackagePath = Join-Path $outputPath "PSParquet.zip"
}

# Verify package exists
if (-not (Test-Path $PackagePath)) {
    throw "Package not found at: $PackagePath`nRun Build-AzureAutomationPackage.ps1 first."
}

Write-Host "Testing package: $PackagePath" -ForegroundColor Yellow
Write-Host ""

# Create temp directory for extraction
$tempDir = Join-Path $env:TEMP "PSParquet-Test-$(Get-Random)"
New-Item -Path $tempDir -ItemType Directory -Force | Out-Null

try {
    # Extract package
    Write-Host "Extracting package..." -ForegroundColor Yellow
    Expand-Archive -Path $PackagePath -DestinationPath $tempDir -Force
    
    # Validate structure
    Write-Host "Validating package structure..." -ForegroundColor Yellow
    
    $requiredFiles = @(
        "PSParquet.psd1",
        "PSParquet.psm1",
        "bin\PSParquet.dll"
    )
    
    $allValid = $true
    foreach ($file in $requiredFiles) {
        $filePath = Join-Path $tempDir $file
        if (Test-Path $filePath) {
            Write-Output "  Found: $file" -ForegroundColor Green
        } else {
            Write-Output "  Missing: $file" -ForegroundColor Red
            $allValid = $false
        }
    }
    
    if (-not $allValid) {
        throw "Package structure validation failed. Required files are missing."
    }
    
    Write-Host ""
    
    # Run all tests in a separate PowerShell job to avoid assembly conflicts
    Write-Host "Running module tests in isolated session..." -ForegroundColor Yellow
    
    $testResults = Start-Job -ScriptBlock {
        param($modulePath)
        
        $results = @{
            AllPassed = $true
            Messages = @()
        }
        
        try {
            # Test module import
            Import-Module $modulePath -Force -ErrorAction Stop
            $results.Messages += "✓ Module imported successfully"
            
            # Verify exported commands
            $expectedCommands = @('Import-Parquet', 'Export-Parquet', 'Get-ParquetFileInfo')
            $module = Get-Module PSParquet
            
            foreach ($cmd in $expectedCommands) {
                if ($module.ExportedCommands.ContainsKey($cmd)) {
                    $results.Messages += "✓ Command available: $cmd"
                } else {
                    $results.Messages += "✗ Command missing: $cmd"
                    $results.AllPassed = $false
                }
            }
            
            # Test basic functionality
            $testFile = Join-Path $env:TEMP "psparquet-test-$(Get-Random).parquet"
            
            # Create test data with null values
            $testData = 1..10 | ForEach-Object {
                [PSCustomObject]@{
                    Id = $_
                    Name = "Test $_"
                    Date = Get-Date
                    Value = $_ * 10
                    IsActive = ($_ % 2 -eq 0)
                    NullValue = if ($_ -eq 5) { $null } else { $_ }
                }
            }
            
            # Test export
            Export-Parquet -FilePath $testFile -InputObject $testData -Force
            $results.Messages += "✓ Export-Parquet executed successfully"
            
            if (Test-Path $testFile) {
                $results.Messages += "✓ Parquet file created"
            } else {
                $results.Messages += "✗ Parquet file was not created"
                $results.AllPassed = $false
            }
            
            # Test import
            $importedData = Import-Parquet -FilePath $testFile
            $results.Messages += "✓ Import-Parquet executed successfully"
            
            if ($importedData.Count -eq 10) {
                $results.Messages += "✓ Correct number of records imported (10)"
            } else {
                $results.Messages += "✗ Expected 10 records, got $($importedData.Count)"
                $results.AllPassed = $false
            }
            
            # Verify null value preservation
            $nullRecord = $importedData | Where-Object { $_.Id -eq 5 }
            if ($null -eq $nullRecord.NullValue) {
                $results.Messages += "✓ Null values preserved correctly"
            } else {
                $results.Messages += "✗ Null values not preserved (got: $($nullRecord.NullValue))"
                $results.AllPassed = $false
            }
            
            # Test Get-ParquetFileInfo
            $fileInfo = Get-ParquetFileInfo -FilePath $testFile
            $results.Messages += "✓ Get-ParquetFileInfo executed successfully"
            
            if ($fileInfo.Schema.Count -eq 6) {
                $results.Messages += "✓ Schema contains 6 columns as expected"
            } else {
                $results.Messages += "✗ Expected 6 schema columns, got $($fileInfo.Schema.Count)"
                $results.AllPassed = $false
            }
            
            # Test compression parameter
            $compressedFile = Join-Path $env:TEMP "psparquet-compressed-$(Get-Random).parquet"
            Export-Parquet -FilePath $compressedFile -InputObject $testData -CompressionMethod Snappy -Force
            $results.Messages += "✓ Compression parameter works"
            
            # Cleanup test files
            Remove-Item $testFile -Force -ErrorAction SilentlyContinue
            Remove-Item $compressedFile -Force -ErrorAction SilentlyContinue
            
        } catch {
            $results.Messages += "✗ Test failed: $($_.Exception.Message)"
            $results.AllPassed = $false
        }
        
        return $results
    } -ArgumentList (Join-Path $tempDir "PSParquet.psd1") | Wait-Job | Receive-Job
    
    Get-Job | Remove-Job -Force
    
    # Display results
    Write-Host ""
    foreach ($message in $testResults.Messages) {
        if ($message.StartsWith("✓")) {
            Write-Output "  $message" -ForegroundColor Green
        } else {
            Write-Output "  $message" -ForegroundColor Red
        }
    }
    
    Write-Host ""
    
    if ($testResults.AllPassed) {
        Write-Output "=== All Tests Passed ===" -ForegroundColor Green
        Write-Output "Package is ready for Azure Automation upload!" -ForegroundColor Green
        Write-Output ""
        Write-Output "Package file: $PackagePath" -ForegroundColor Cyan
        return $true
    } else {
        Write-Output "=== Some Tests Failed ===" -ForegroundColor Red
        throw "Package validation failed."
    }
    
} catch {
    Write-Output ""
    Write-Output "Package validation failed: $_" -ForegroundColor Red
    throw
} finally {
    # Cleanup
    if (Test-Path $tempDir) {
        Remove-Item $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}
