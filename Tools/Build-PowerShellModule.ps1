<#
.SYNOPSIS
    Builds PSParquet module and creates a module package for local installation. It includes the version number unlike the Azure Automation package.
.DESCRIPTION
    This script builds the PSParquet module using psake and creates a .zip package
    that can be installed locally as a PowerShell module.
.PARAMETER OutputPath
    The path where the .zip package will be created. Defaults to ./output
.PARAMETER SkipBuild
    Skip the psake build process and package existing output folder.
.PARAMETER ImportModule
    Install the rebuilt module into the current user's PowerShell module path and import it.
.EXAMPLE
    .\Build-PowerShellModule.ps1
.EXAMPLE        
    .\Build-PowerShellModule.ps1 -OutputPath "C:\Packages"
.EXAMPLE
    .\Build-PowerShellModule.ps1 -SkipBuild

#>

[CmdletBinding()]
param(
    [Parameter()]
    [string]$OutputPath = (Join-Path (Split-Path $PSScriptRoot -Parent) "output"),
    
    [Parameter()]
    [switch]$SkipBuild,

    [Parameter()]
    [switch]$ImportModule
)
$ErrorActionPreference = 'Stop'
# Define paths
$repoRoot = Split-Path $PSScriptRoot -Parent
$moduleName = "PSParquet"
$moduleSourcePath = Join-Path $repoRoot "output\$moduleName"
Write-Host "=== PSParquet PowerShell Module Package Builder ===" -ForegroundColor Cyan
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
    $psakePath = Join-Path $repoRoot "PSParquet.psake.ps1"
    
    if (-not (Test-Path $psakePath)) {
        throw "psake build script not found at: $psakePath"
    }
    
    $psakeCommand = @"
Import-Module psake
Invoke-psake -buildFile `"$psakePath`" -task Build
"@
    Invoke-Expression -Command $psakeCommand
}
# Verify module source exists after build
if (-not (Test-Path $moduleSourcePath)) {
    throw "Module source not found at: $moduleSourcePath. Ensure psake Build task copied the module output."
}

$moduleManifestPath = Join-Path $moduleSourcePath "$moduleName.psd1"
if (-not (Test-Path $moduleManifestPath)) {
    throw "Module manifest not found at: $moduleManifestPath"
}

$moduleVersion = (Test-ModuleManifest -Path $moduleManifestPath).Version.ToString()
$zipFileName = "$moduleName-$moduleVersion.zip"
$zipFilePath = Join-Path $OutputPath $zipFileName
Write-Host "Creating PowerShell module package: $zipFilePath" -ForegroundColor Yellow
# Create output directory if it doesn't exist
if (-not (Test-Path $OutputPath)) {
    New-Item -Path $OutputPath -ItemType Directory -Force | Out-Null
}
# Remove existing package if it exists
if (Test-Path $zipFilePath) {
    Remove-Item -Path $zipFilePath -Force
}
# Create the .zip package
Compress-Archive -Path (Join-Path $moduleSourcePath "*") -DestinationPath $zipFilePath -Force
Write-Host "Package created successfully at: $zipFilePath" -ForegroundColor Green

if ($ImportModule)
{
    $userModulesRoot = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'PowerShell\Modules'
    if (-not (Test-Path $userModulesRoot))
    {
        New-Item -Path $userModulesRoot -ItemType Directory -Force | Out-Null
    }

    $moduleInstallRoot = Join-Path $userModulesRoot $moduleName
    if (-not (Test-Path $moduleInstallRoot))
    {
        New-Item -Path $moduleInstallRoot -ItemType Directory -Force | Out-Null
    }

    $versionedInstallPath = Join-Path $moduleInstallRoot $moduleVersion
    if (Test-Path $versionedInstallPath)
    {
        Write-Host "Removing existing installed version at $versionedInstallPath" -ForegroundColor Yellow
        Remove-Item -Path $versionedInstallPath -Recurse -Force
    }

    Write-Host "Installing module to $versionedInstallPath" -ForegroundColor Yellow
    New-Item -Path $versionedInstallPath -ItemType Directory -Force | Out-Null
    Copy-Item -Path (Join-Path $moduleSourcePath '*') -Destination $versionedInstallPath -Recurse -Force

    $moduleImportPath = $versionedInstallPath
    Write-Host "Importing module from $moduleImportPath" -ForegroundColor Yellow
    try
    {
        Import-Module $moduleImportPath -Force -ErrorAction Stop
        Write-Host "Module imported successfully from user scope (version $moduleVersion)." -ForegroundColor Green
    }
    catch
    {
        Write-Error "Failed to import module from $moduleImportPath"
        throw
    }
}
Write-Host ""
Write-Host "Build and packaging process completed." -ForegroundColor Cyan
Write-Host ""