Describe "Extended PSParquet Tests" {
    BeforeAll {
        $ModulePath = Join-Path (Join-Path $(Split-Path $PSScriptRoot) 'output') 'PSParquet'
        # Remove existing module if loaded
        Get-Module PSParquet | Remove-Module -Force -ErrorAction SilentlyContinue
        Import-Module $ModulePath -Force
    }

    Context "Data Type Coverage" {
        BeforeAll {
            $tempFile = New-TemporaryFile
            $testData = 1..5 | ForEach-Object {
                [PSCustomObject]@{
                    Boolean     = ($_ % 2 -eq 0)
                    Guid        = [Guid]::NewGuid()
                    Byte        = [byte]$_
                    Decimal     = [decimal]($_ * 1.5)
                    Float       = [float]($_ * 2.5)
                    Double      = [double]($_ * 3.5)
                    Int64       = [int64]($_ * 1000000)
                    String      = "Test $_"
                    EmptyString = ""
                    NullString  = if ($_ -eq 3) { $null } else { "Value $_" }
                }
            }
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            $imported = Import-Parquet -FilePath $tempFile.FullName
        }

        It "Preserves Boolean values" {
            $imported[0].Boolean | Should -Be $false
            $imported[1].Boolean | Should -Be $true
        }

        It "Preserves Guid values" {
            $imported[0].Guid | Should -BeOfType [Guid]
            $imported[0].Guid.ToString().Length | Should -Be 36
        }

        It "Preserves Byte values" {
            $imported[0].Byte | Should -Be 1
            $imported[0].Byte | Should -BeOfType [byte]
        }

        It "Preserves Decimal values" {
            $imported[0].Decimal | Should -BeGreaterThan 0
        }

        It "Preserves Float values" {
            $imported[0].Float | Should -BeGreaterThan 0
        }

        It "Preserves Double values" {
            $imported[0].Double | Should -BeGreaterThan 0
        }

        It "Preserves Int64 values" {
            $imported[0].Int64 | Should -BeGreaterThan 999999
        }

                It "Preserves empty strings" {
                        $imported[0].EmptyString | Should -BeExactly ""
                        $imported[0].EmptyString.Length | Should -Be 0
                        $imported[0].EmptyString | Should -Not -Be $null -Because "empty string should not be cast to null"
                }

        It "Distinguishes null from empty string" {
            $imported[2].NullString | Should -BeNullOrEmpty
            $null -eq $imported[2].NullString | Should -Be $true
        }

        AfterAll {
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
        }
    }

    Context "Special Characters in Strings" {
        BeforeAll {
            $tempFile = New-TemporaryFile
            $testData = @(
                [PSCustomObject]@{
                    Unicode      = "Hello 世界 🌍"
                    Newlines     = "Line1`nLine2`r`nLine3"
                    Quotes       = "Single 'quotes' and `"double quotes`""
                    Tabs         = "Tab`tseparated`tvalues"
                    SpecialChars = "!@#$%^&*()_+-=[]{}|;:,.<>?"
                }
            )
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            $imported = Import-Parquet -FilePath $tempFile.FullName
        }

        It "Preserves Unicode characters" {
            $imported[0].Unicode | Should -Be "Hello 世界 🌍"
        }

        It "Preserves newlines" {
            $imported[0].Newlines | Should -Match "Line1"
            $imported[0].Newlines | Should -Match "Line2"
        }

        It "Preserves quotes" {
            $imported[0].Quotes | Should -Match "quotes"
        }

        It "Preserves special characters" {
            $imported[0].SpecialChars.Length | Should -BeGreaterThan 10
        }

        AfterAll {
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
        }
    }

    Context "Compression Methods" {
        BeforeAll {
            $testData = 1..50 | ForEach-Object {
                [PSCustomObject]@{
                    Id = $_
                    Data = "Test data string $_" * 10
                }
            }
        }

        It "Exports with Gzip compression" {
            $tempFile = New-TemporaryFile
            { Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -CompressionMethod Gzip -Force } | Should -Not -Throw
            $tempFile.Length | Should -BeGreaterThan 0
            Remove-Item $tempFile -Force
        }

        It "Exports with Snappy compression" {
            $tempFile = New-TemporaryFile
            { Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -CompressionMethod Snappy -Force } | Should -Not -Throw
            Remove-Item $tempFile -Force
        }

        It "Exports with Brotli compression" {
            $tempFile = New-TemporaryFile
            { Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -CompressionMethod Brotli -Force } | Should -Not -Throw
            Remove-Item $tempFile -Force
        }

        It "Exports with Zstd compression" {
            $tempFile = New-TemporaryFile
            { Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -CompressionMethod Zstd -Force } | Should -Not -Throw
            Remove-Item $tempFile -Force
        }

        It "Exports with no compression" {
            $tempFile = New-TemporaryFile
            { Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -CompressionMethod None -Force } | Should -Not -Throw
            Remove-Item $tempFile -Force
        }

        It "Compares compression levels" {
            $fastFile = New-TemporaryFile
            $optimalFile = New-TemporaryFile
            
            Export-Parquet -FilePath $fastFile.FullName -InputObject $testData -CompressionLevel Fastest -Force
            Export-Parquet -FilePath $optimalFile.FullName -InputObject $testData -CompressionLevel Optimal -Force
            
            $fastFile.Length | Should -BeGreaterThan 0
            $optimalFile.Length | Should -BeGreaterThan 0
            
            Remove-Item $fastFile, $optimalFile -Force
        }
    }

    Context "Edge Cases" {
        It "Handles empty array gracefully" {
            # PowerShell parameter binding doesn't allow passing empty arrays explicitly to array parameters
            # This is a PowerShell limitation, not a module bug
            # Instead, test that empty pipeline collection results in a warning
            $tempFile = New-TemporaryFile
            $data = @([PSCustomObject]@{ Test = "Value" })
            $data | Where-Object { $false } | Export-Parquet -FilePath $tempFile.FullName -Force -WarningVariable warnVar -WarningAction SilentlyContinue
            $warnVar | Should -Match "No objects to export"
            Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
        }

        It "Handles single object" {
            $tempFile = New-TemporaryFile
            $singleData = @([PSCustomObject]@{ Id = 1; Name = "Single" })
            Export-Parquet -FilePath $tempFile.FullName -InputObject $singleData -Force
            $imported = Import-Parquet -FilePath $tempFile.FullName
            $imported.Count | Should -Be 1
            Remove-Item $tempFile -Force
        }

        It "Handles all null values in column" {
            $tempFile = New-TemporaryFile
            $nullData = 1..5 | ForEach-Object {
                [PSCustomObject]@{
                    Id = $_
                    AlwaysNull = $null
                }
            }
            Export-Parquet -FilePath $tempFile.FullName -InputObject $nullData -Force
            $imported = Import-Parquet -FilePath $tempFile.FullName
            $imported[0].AlwaysNull | Should -BeNullOrEmpty
            $imported[4].AlwaysNull | Should -BeNullOrEmpty
            Remove-Item $tempFile -Force
        }

        It "Handles very large strings" {
            $tempFile = New-TemporaryFile
            $largeData = @([PSCustomObject]@{
                Id = 1
                LargeText = "X" * 100000  # 100KB string
            })
            { Export-Parquet -FilePath $tempFile.FullName -InputObject $largeData -Force } | Should -Not -Throw
            $imported = Import-Parquet -FilePath $tempFile.FullName
            $imported[0].LargeText.Length | Should -Be 100000
            Remove-Item $tempFile -Force
        }

        It "Handles property names with spaces" {
            $tempFile = New-TemporaryFile
            $spaceData = @([PSCustomObject]@{
                'First Name' = "John"
                'Last Name' = "Doe"
                'Age in Years' = 30
            })
            Export-Parquet -FilePath $tempFile.FullName -InputObject $spaceData -Force
            $imported = Import-Parquet -FilePath $tempFile.FullName
            $imported[0].'First Name' | Should -Be "John"
            Remove-Item $tempFile -Force
        }
    }

    Context "Error Handling" {
        It "Handles non-existent file for import" {
            $nonExistent = "C:\NonExistent\File.parquet"
            { Import-Parquet -FilePath $nonExistent -ErrorAction Stop } | Should -Throw
        }

        It "Handles non-existent file for Get-ParquetFileInfo" {
            $nonExistent = "C:\NonExistent\File.parquet"
            { Get-ParquetFileInfo -FilePath $nonExistent -ErrorAction Stop } | Should -Throw
        }

        It "Handles invalid directory for export" {
            $invalidPath = "C:\NonExistent\Folder\File.parquet"
            $testData = @([PSCustomObject]@{ Id = 1 })
            { Export-Parquet -FilePath $invalidPath -InputObject $testData -ErrorAction Stop } | Should -Throw
        }
    }

    Context "Parameter Testing" {
        It "PassThru parameter returns objects" {
            $tempFile = New-TemporaryFile
            $testData = 1..3 | ForEach-Object {
                [PSCustomObject]@{ Id = $_; Name = "Test $_" }
            }
            $result = Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -PassThru -Force
            $result.Count | Should -Be 3
            $result[0].Id | Should -Be 1
            Remove-Item $tempFile -Force
        }

        It "Force parameter overwrites existing file" {
            $tempFile = New-TemporaryFile
            $data1 = @([PSCustomObject]@{ Value = 1 })
            $data2 = @([PSCustomObject]@{ Value = 2 })
            
            Export-Parquet -FilePath $tempFile.FullName -InputObject $data1 -Force
            Export-Parquet -FilePath $tempFile.FullName -InputObject $data2 -Force
            
            $imported = Import-Parquet -FilePath $tempFile.FullName
            $imported[0].Value | Should -Be 2
            Remove-Item $tempFile -Force
        }

        It "FirstNGroups limits imported row groups" {
            $tempFile = New-TemporaryFile
            $testData = 1..100 | ForEach-Object {
                [PSCustomObject]@{ Id = $_; Value = $_ * 10 }
            }
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            
            # Import all
            $allData = Import-Parquet -FilePath $tempFile.FullName
            $allData.Count | Should -Be 100
            
            # Import with FirstNGroups (may not reduce count if all in one group)
            $limitedData = Import-Parquet -FilePath $tempFile.FullName -FirstNGroups 1
            $limitedData.Count | Should -BeGreaterThan 0
            
            Remove-Item $tempFile -Force
        }

        It "WhatIf prevents file creation" {
            $tempFile = Join-Path $env:TEMP "whatif-test-$(Get-Random).parquet"
            $testData = @([PSCustomObject]@{ Id = 1 })
            
            Export-Parquet -FilePath $tempFile -InputObject $testData -WhatIf
            
            Test-Path $tempFile | Should -Be $false
        }
    }

    Context "Pipeline Operations" {
        It "Accepts FileInfo from pipeline for Import" {
            $tempFile = New-TemporaryFile
            $testData = @([PSCustomObject]@{ Id = 1; Name = "Test" })
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            
            $result = Get-Item $tempFile.FullName | Import-Parquet
            $result[0].Id | Should -Be 1
            
            Remove-Item $tempFile -Force
        }

        It "Accepts FileInfo from pipeline for Get-ParquetFileInfo" {
            $tempFile = New-TemporaryFile
            $testData = @([PSCustomObject]@{ Id = 1 })
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            
            $info = Get-Item $tempFile.FullName | Get-ParquetFileInfo
            $info.RowGroupCount | Should -BeGreaterThan 0
            
            Remove-Item $tempFile -Force
        }
    }

    Context "Verbose Output" {
        It "Export-Parquet produces verbose output" {
            $tempFile = New-TemporaryFile
            $testData = @([PSCustomObject]@{ Id = 1 })
            
            $output = Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force -Verbose 4>&1
            $output | Should -Not -BeNullOrEmpty
            
            Remove-Item $tempFile -Force
        }

        It "Import-Parquet produces verbose output" {
            $tempFile = New-TemporaryFile
            $testData = @([PSCustomObject]@{ Id = 1 })
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            
            $output = Import-Parquet -FilePath $tempFile.FullName -Verbose 4>&1
            $output | Should -Not -BeNullOrEmpty
            
            Remove-Item $tempFile -Force
        }
    }

    Context "Round-trip Data Integrity" {
        It "Maintains data integrity through export/import cycle" {
            $tempFile = New-TemporaryFile
            $original = @(
                [PSCustomObject]@{
                    Id = 1
                    Name = "Alice"
                    Age = 30
                    IsActive = $true
                    Balance = 1234.56
                    JoinDate = Get-Date "2020-01-15"
                    Notes = $null
                }
                [PSCustomObject]@{
                    Id = 2
                    Name = "Bob"
                    Age = 25
                    IsActive = $false
                    Balance = 7890.12
                    JoinDate = Get-Date "2021-06-20"
                    Notes = "Has notes"
                }
            )
            
            Export-Parquet -FilePath $tempFile.FullName -InputObject $original -Force
            $imported = Import-Parquet -FilePath $tempFile.FullName
            
            $imported.Count | Should -Be 2
            $imported[0].Name | Should -Be "Alice"
            $imported[0].Age | Should -Be 30
            $imported[0].IsActive | Should -Be $true
            $imported[0].Balance | Should -BeGreaterThan 1234
            $imported[0].Notes | Should -BeNullOrEmpty
            $imported[1].Notes | Should -Be "Has notes"
            
            Remove-Item $tempFile -Force
        }
    }

    Context "Schema Information" {
        It "Get-ParquetFileInfo returns complete schema" {
            $tempFile = New-TemporaryFile
            $testData = @([PSCustomObject]@{
                IntField = 42
                StringField = "Test"
                BoolField = $true
                DateField = Get-Date
                NullableField = $null
            })
            
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            $info = Get-ParquetFileInfo -FilePath $tempFile.FullName
            
            $info.Schema.Count | Should -Be 5
            $info.Schema.Name | Should -Contain "IntField"
            $info.Schema.Name | Should -Contain "StringField"
            $info.Schema.Name | Should -Contain "BoolField"
            $info.Schema.Name | Should -Contain "DateField"
            
            Remove-Item $tempFile -Force
        }

        It "Schema reports correct types" {
            $tempFile = New-TemporaryFile
            $testData = @([PSCustomObject]@{
                IntField = 42
                StringField = "Test"
                DateField = Get-Date
            })
            
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            $info = Get-ParquetFileInfo -FilePath $tempFile.FullName
            
            $stringField = $info.Schema | Where-Object { $_.Name -eq "StringField" }
            $stringField.Type | Should -Match "String"
            
            Remove-Item $tempFile -Force
        }

        It "Schema exposes CLR type names for nullable columns" {
            $tempFile = New-TemporaryFile
            $testData = @(
                [PSCustomObject]@{
                    NullableInt  = $null
                    NullableBool = $null
                    GuidField    = [Guid]::NewGuid()
                }
                [PSCustomObject]@{
                    NullableInt  = 7
                    NullableBool = $true
                    GuidField    = [Guid]::NewGuid()
                }
            )
            
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            $info = Get-ParquetFileInfo -FilePath $tempFile.FullName
            
            $nullableIntField = $info.Schema | Where-Object { $_.Name -eq "NullableInt" }
            $nullableBoolField = $info.Schema | Where-Object { $_.Name -eq "NullableBool" }
            $guidField = $info.Schema | Where-Object { $_.Name -eq "GuidField" }
            
            $nullableIntField.Type | Should -Be "System.Int32"
            $nullableBoolField.Type | Should -Be "System.Boolean"
            $guidField.Type | Should -Be "System.Guid"
            $nullableIntField.PSTypeNames[0] | Should -Be "PSParquet.DataFieldInfo"
            
            Remove-Item $tempFile -Force
        }

        It "Schema diagnostics match runtime CLR types" {
            $tempFile = New-TemporaryFile
            $baseDate = Get-Date
            $testData = @(
                [PSCustomObject]@{
                    IntValue      = 123
                    NullableInt   = $null
                    DoubleValue   = 12.34
                    BoolValue     = $true
                    DateValue     = $baseDate
                    GuidValue     = [Guid]::NewGuid()
                    TextValue     = "alpha"
                }
                [PSCustomObject]@{
                    IntValue      = 456
                    NullableInt   = 789
                    DoubleValue   = 98.76
                    BoolValue     = $false
                    DateValue     = $baseDate.AddDays(1)
                    GuidValue     = [Guid]::NewGuid()
                    TextValue     = "beta"
                }
            )
            
            Export-Parquet -FilePath $tempFile.FullName -InputObject $testData -Force
            $info = Get-ParquetFileInfo -FilePath $tempFile.FullName
            $imported = Import-Parquet -FilePath $tempFile.FullName
            
            $schemaLookup = @{}
            foreach ($field in $info.Schema)
            {
                $schemaLookup[$field.Name] = $field.Type
            }
            
            $schemaLookup["IntValue"] | Should -Be $imported[0].IntValue.GetType().FullName
            $schemaLookup["DoubleValue"] | Should -Be $imported[0].DoubleValue.GetType().FullName
            $schemaLookup["BoolValue"] | Should -Be $imported[0].BoolValue.GetType().FullName
            $schemaLookup["DateValue"] | Should -Be $imported[0].DateValue.GetType().FullName
            $schemaLookup["GuidValue"] | Should -Be $imported[0].GuidValue.GetType().FullName
            $schemaLookup["TextValue"] | Should -Be $imported[0].TextValue.GetType().FullName
            $schemaLookup["NullableInt"] | Should -Be ((($imported | Where-Object { $_.NullableInt -ne $null }) | Select-Object -First 1).NullableInt.GetType().FullName)
            
            Remove-Item $tempFile -Force
        }
    }
}
