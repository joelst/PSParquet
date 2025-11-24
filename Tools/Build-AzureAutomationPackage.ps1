<#
.SYNOPSIS
    Builds PSParquet module and creates a package for Azure Automation upload.

.DESCRIPTION
    This script builds the PSParquet module using psake and creates a .zip package
    that can be uploaded directly to Azure Automation as a custom module.

.PARAMETER OutputPath
    The path where the .zip package will be created. Defaults to ./output

.PARAMETER SkipBuild
    Skip the psake build process and package existing output folder.

.EXAMPLE
    .\Build-AzureAutomationPackage.ps1

.EXAMPLE
    .\Build-AzureAutomationPackage.ps1 -OutputPath "C:\Packages"

.EXAMPLE
    .\Build-AzureAutomationPackage.ps1 -SkipBuild
#>

[CmdletBinding()]
param(
    [Parameter()]
    [string]$OutputPath = (Join-Path $PSScriptRoot "output"),
    
    [Parameter()]
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

# Define paths
$scriptRoot = $PSScriptRoot
$moduleName = "PSParquet"
$moduleSourcePath = Join-Path $scriptRoot "output\$moduleName"
$zipFileName = "$moduleName.zip"
$zipFilePath = Join-Path $OutputPath $zipFileName

Write-Host "=== PSParquet Azure Automation Package Builder ===" -ForegroundColor Cyan
Write-Host ""

# Check for required modules
$requiredModules = @('psake', 'Pester', 'platyPS')
foreach ($module in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Warning "Module '$module' not found. Installing..."
        Install-Module -Name $module -Scope CurrentUser -Force -AllowClobber
    }
}

# Build the module using psake if not skipped
if (-not $SkipBuild) {
    Write-Host "Building module with psake..." -ForegroundColor Yellow
    $psakePath = Join-Path $scriptRoot "PSParquet.psake.ps1"
    
    if (-not (Test-Path $psakePath)) {
        throw "psake build script not found at: $psakePath"
    }
    
    Invoke-psake $psakePath Build -Verbose:$VerbosePreference
    
    if (-not $psake.build_success) {
        throw "Build failed. Cannot continue with packaging."
    }
    
    Write-Host "Build completed successfully." -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "Skipping build (using existing output)..." -ForegroundColor Yellow
    Write-Host ""
}

# Verify module output exists
if (-not (Test-Path $moduleSourcePath)) {
    throw "Module output not found at: $moduleSourcePath. Run build first."
}

# Verify module manifest
$manifestPath = Join-Path $moduleSourcePath "$moduleName.psd1"
if (-not (Test-Path $manifestPath)) {
    throw "Module manifest not found at: $manifestPath"
}

# Get module version from manifest
$manifest = Test-ModuleManifest -Path $manifestPath
$version = $manifest.Version.ToString()

Write-Output "Module Information:" -ForegroundColor Cyan
Write-Output "  Name:    $moduleName"
Write-Output "  Version: $version"
Write-Output "  Path:    $moduleSourcePath"
Write-Output ""
# Create output directory if it doesn't exist
if (-not (Test-Path $OutputPath)) {
    Write-Output "Creating output directory: $OutputPath" -ForegroundColor Yellow
    New-Item -Path $OutputPath -ItemType Directory -Force | Out-Null
}

# Remove existing zip if present
if (Test-Path $zipFilePath) {
    Write-Output "Removing existing package: $zipFileName" -ForegroundColor Yellow
    Remove-Item $zipFilePath -Force
}

# Create the zip package
Write-Output "Creating Azure Automation package..." -ForegroundColor Yellow
try {
    # Azure Automation expects the module files directly in the zip root
    # We need to compress the contents of the PSParquet folder, not the folder itself
    $tempZipPath = Join-Path $env:TEMP "$moduleName-temp.zip"
    
    # Create zip with module contents
    Compress-Archive -Path "$moduleSourcePath\*" -DestinationPath $tempZipPath -CompressionLevel Optimal -Force
    
    # Move to final location
    Move-Item -Path $tempZipPath -Destination $zipFilePath -Force
    
    Write-Output "Package created successfully!" -ForegroundColor Green
    Write-Output ""
} catch {
    throw "Failed to create package: $_"
}

# Display package information
$zipInfo = Get-Item $zipFilePath
$sizeInMB = [math]::Round($zipInfo.Length / 1MB, 2)

Write-Output "=== Package Details ===" -ForegroundColor Cyan
Write-Output "  File:     $zipFileName"
Write-Output "  Location: $($zipInfo.FullName)"
Write-Output "  Size:     $sizeInMB MB"
Write-Output ""
# Instructions for Azure Automation
Write-Output "=== Upload Instructions ===" -ForegroundColor Green
Write-Output "Azure Automation Module Upload:"
Write-Output "1. Go to your Azure Automation Account in the Azure Portal"
Write-Output "2. Navigate to: Modules -> Add a module"
Write-Output "3. Select 'Upload a module package'"
Write-Output "4. Choose the file: $zipFileName"
Write-Output "5. Select Runtime version: PowerShell 7.4"
Write-Output "6. Click 'Import' and wait for the module to be extracted"
Write-Output ""
Write-Output "Note: Azure Automation expects module files in the zip root (no version folder)." -ForegroundColor Yellow
Write-Output "This package structure is correct for Azure Automation." -ForegroundColor Yellow
Write-Output ""
Write-Output "For PowerShell Gallery, the module should be in a version subfolder." -ForegroundColor Cyan
Write-Output "The output\PSParquet folder already has the correct structure for Gallery." -ForegroundColor Cyan
Write-Output ""

# Verify package contents
Write-Output "=== Package Contents Verification ===" -ForegroundColor Cyan
try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead($zipFilePath)
    
    Write-Output "Files in package:"
    $zip.Entries | Where-Object { $_.Name } | Select-Object -First 10 | ForEach-Object {
        Write-Output "  - $($_.FullName)"
    }
    
    if ($zip.Entries.Count -gt 10) {
        Write-Output "  ... and $($zip.Entries.Count - 10) more files"
    }
    
    $zip.Dispose()
    Write-Output ""
} catch {
    Write-Warning "Could not verify package contents: $_"
}

Write-Output "Package ready for Azure Automation upload!" -ForegroundColor Green
Write-Output "Package location: $zipFilePath" -ForegroundColor Cyan