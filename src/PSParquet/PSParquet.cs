using Parquet.Schema;
using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Threading.Tasks;
using PSParquet.Classes;
using System.Collections;

namespace PSParquet
{
    public class PSParquet
    {
        string FilePath { get; set; }

        private static Type DetermineColumnType(string propertyName, PSObject[] inputObjects)
        {
            // Sample multiple objects to determine the type, skipping nulls
            var sampleSize = Math.Min(100, inputObjects.Length);
            Type detectedType = null;

            for (int i = 0; i < sampleSize; i++)
            {
                var value = inputObjects[i].Properties[propertyName]?.Value;
                if (value == null)
                {
                    continue;
                }

                // Unwrap PSObject
                if (value is PSObject pso)
                {
                    value = pso.BaseObject;
                }

                if (value == null)
                {
                    continue;
                }

                var valueType = value.GetType();

                // If we find a DateTime, use it
                if (valueType == typeof(DateTime))
                {
                    return typeof(DateTime);
                }

                // If it's a string, check if it's actually a DateTime
                if (valueType == typeof(string))
                {
                    if (DateTime.TryParse(value.ToString(), out _))
                    {
                        // Check more samples to confirm it's consistently a date
                        bool isDateTime = true;
                        for (int j = i + 1; j < Math.Min(i + 10, sampleSize); j++)
                        {
                            var testValue = inputObjects[j].Properties[propertyName]?.Value;
                            if (testValue != null)
                            {
                                if (testValue is PSObject testPso)
                                {
                                    testValue = testPso.BaseObject;
                                }
                                if (testValue != null && !DateTime.TryParse(testValue.ToString(), out _))
                                {
                                    isDateTime = false;
                                    break;
                                }
                            }
                        }
                        if (isDateTime)
                        {
                            return typeof(DateTime);
                        }
                    }
                }

                // Use the detected type if we haven't found one yet
                if (detectedType == null)
                {
                    detectedType = valueType;
                }
                // If types differ and one is Int32, promote to Double
                else if (detectedType != valueType)
                {
                    if ((detectedType == typeof(int) || detectedType == typeof(Int32)) && 
                        (valueType == typeof(double) || valueType == typeof(Double)))
                    {
                        detectedType = typeof(double);
                    }
                    else if ((valueType == typeof(int) || valueType == typeof(Int32)) && 
                             (detectedType == typeof(double) || detectedType == typeof(Double)))
                    {
                        // Keep double
                    }
                }
            }

            // Default to string if no type was detected
            return detectedType ?? typeof(string);
        }

        public static async Task<PSObject> GetParquetFileInfo(string FilePath)
        {
            PSObject pso = new();
            pso.TypeNames.Insert(0, "PSParquet.ParquetFileInfo");
            Stream fileStream = File.OpenRead(FilePath);
            using (ParquetReader reader = await ParquetReader.CreateAsync(fileStream, leaveStreamOpen: false))
            {
                pso.Properties.Add(new PSNoteProperty("RowGroupCount", reader.RowGroupCount));
                pso.Properties.Add(new PSNoteProperty("ElementsInFirstGroup", reader.RowGroups[0].RowCount));
                pso.Properties.Add(new PSNoteProperty("ElementsInLastGroup", reader.RowGroups[reader.RowGroupCount - 1].RowCount));
                List<PSObject> schemaFields = reader.Schema.DataFields.Select(field => 
                {
                    PSObject fieldObj = new();
                    fieldObj.TypeNames.Insert(0, "PSParquet.DataFieldInfo");
                    fieldObj.Properties.Add(new PSNoteProperty("Name", field.Name));
                    fieldObj.Properties.Add(new PSNoteProperty("Type", field.ClrType.FullName));
                    return fieldObj;
                }).ToList();
                
                pso.Properties.Add(new PSNoteProperty("Schema", schemaFields));
            }
            return pso;
        }

        public static async Task<List<PSObject>> GetParquetObjects(string FilePath, int? FirstNGroups)
        {
            List<PSObject> objects = new List<PSObject>();
            Stream fileStream = File.OpenRead(FilePath);
            using (ParquetReader reader = await ParquetReader.CreateAsync(fileStream, leaveStreamOpen: false))
            {

                int iterator;
                if (!FirstNGroups.HasValue || FirstNGroups.Value ==0 || FirstNGroups.Value > reader.RowGroupCount)
                {
                    iterator = reader.RowGroupCount;
                }
                else
                {
                    iterator = FirstNGroups.Value;
                }

                for (int rg =0; rg < iterator; rg++)
                {
                    var all = await reader.ReadEntireRowGroupAsync(rg);

                    string[] headers = new string[all.Length];
                    for (int i =0; i < all.Length; i++)
                    {
                        headers[i] = all[i].Field.Name.ToString();
                    }

                    for (int rowIndex =0; rowIndex < all[0].Data.Length; rowIndex++)
                    {
                        PSObject pso = new();
                        for (int j =0; j < headers.Length; j++)
                        {
                            pso.Properties.Add(new PSNoteProperty(headers[j], all[j].Data.GetValue(rowIndex)));
                        }
                        objects.Add(pso);
                    }
                }
            }
            return objects;
        }

        public static object GetTypedValue(Type type, dynamic value = null)
        {
            // Handle null values - return null instead of default values
            if (value is null)
            {
                return null;
            }

            dynamic valueResult = value;
            
            // Unwrap PSObject to get the base object
            if (valueResult is PSObject)
            {
                valueResult = ((PSObject)value).BaseObject;
            }

            // Handle null after unwrapping PSObject
            if (valueResult is null)
            {
                return null;
            }

            // For string types, preserve empty strings (don't convert to null)
            if (type == typeof(string) && valueResult is string strValue)
            {
                return strValue; // Return empty string as-is
            }

            // If value is already the correct type, return it
            if (valueResult.GetType() == type)
            {
                return valueResult;
            }

            // Handle DateTime specifically
            if (type == typeof(DateTime) || type == typeof(DateTime?))
            {
                if (valueResult is DateTime)
                {
                    return valueResult;
                }
                if (DateTime.TryParse(valueResult.ToString(), out DateTime parsedDate))
                {
                    return parsedDate;
                }
            }

            // Handle Boolean
            if (type == typeof(bool) || type == typeof(Boolean))
            {
                if (valueResult is bool)
                {
                    return valueResult;
                }
                if (bool.TryParse(valueResult.ToString(), out bool parsedBool))
                {
                    return parsedBool;
                }
            }

            // Handle Guid
            if (type == typeof(Guid))
            {
                if (valueResult is Guid)
                {
                    return valueResult;
                }
                if (Guid.TryParse(valueResult.ToString(), out Guid parsedGuid))
                {
                    return parsedGuid;
                }
            }

            // Try standard conversion
            try
            {
                return Convert.ChangeType(valueResult, type);
            }
            catch
            {
                // If conversion fails, try ToString as last resort for string types
                if (type == typeof(string))
                {
                    return valueResult.ToString();
                }
                throw;
            }
        }

        public static async Task<bool> WriteParquetFile(PSObject[] inputObject, string filePath, string compressionMethod, string compressionLevel, PSCmdlet cmdlet)
        {
            if (inputObject == null || inputObject.Length == 0)
            {
                cmdlet.WriteError(new ErrorRecord(
                    new ArgumentException("InputObject cannot be null or empty"),
                    "EmptyInputObject",
                    ErrorCategory.InvalidArgument,
                    inputObject));
                return false;
            }

            var properties = inputObject[0].Members.Where(w => w.GetType() == typeof(PSNoteProperty)).ToList();
            
            if (properties.Count == 0)
            {
                cmdlet.WriteError(new ErrorRecord(
                    new ArgumentException("InputObject must have at least one property"),
                    "NoProperties",
                    ErrorCategory.InvalidData,
                    inputObject[0]));
                return false;
            }

            List<ParquetData> parquetData = properties.Select(s => new ParquetData
            {
                Parameter = s.Name,
                Type = DetermineColumnType(s.Name, inputObject),
                Data = (from o in inputObject select o.Properties[s.Name].Value).ToArray()
            }).ToList();

            ParquetSchema schema;

            try
            {
                schema = new ParquetSchema(
                    parquetData.Select(s => new DataField(s.Parameter, s.Type, true))
                );
            }
            catch (Exception ex)
            {
                cmdlet.WriteError(new ErrorRecord(
                    ex,
                    "SchemaCreationFailed",
                    ErrorCategory.InvalidData,
                    inputObject));
                return false;
            }


            using (Stream fileStream = File.OpenWrite(filePath))
            {
                using (ParquetWriter parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream))
                {
                    // Set compression method
                    parquetWriter.CompressionMethod = compressionMethod switch
                    {
                        "None" => CompressionMethod.None,
                        "Snappy" => CompressionMethod.Snappy,
                        "Gzip" => CompressionMethod.Gzip,
                        "Lzo" => CompressionMethod.Lzo,
                        "Brotli" => CompressionMethod.Brotli,
                        "Zstd" => CompressionMethod.Zstd,
                        _ => CompressionMethod.Gzip
                    };

                    // Set compression level
                    parquetWriter.CompressionLevel = compressionLevel switch
                    {
                        "Fastest" => System.IO.Compression.CompressionLevel.Fastest,
                        "NoCompression" => System.IO.Compression.CompressionLevel.NoCompression,
                        "SmallestSize" => System.IO.Compression.CompressionLevel.SmallestSize,
                        "Optimal" => System.IO.Compression.CompressionLevel.Optimal,
                        _ => System.IO.Compression.CompressionLevel.Optimal
                    };

                    // create a new row group in the file
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        try
                        {
                            for (int i = 0; i < parquetData.Count; i++)
                            {
                                Type type = parquetData[i].Type;
                                Int64 count = parquetData[i].Data.Count();
                                var rawData = parquetData[i].Data;
                                
                                // Since schema marks fields as nullable, we need to use nullable types for value types
                                Type arrayElementType = type;
                                if (type.IsValueType)
                                {
                                    arrayElementType = typeof(Nullable<>).MakeGenericType(type);
                                }
                                
                                Array arr = Array.CreateInstance(arrayElementType, count);
                                var data = rawData.Select(s => GetTypedValue(type, s)).ToArray();
                                Array.Copy(data, arr, parquetData[i].Data.Count());
                                await groupWriter.WriteColumnAsync(new DataColumn(schema.DataFields[i], arr));
                            }
                            return true;
                        }
                        catch (Exception ex)
                        {
                            cmdlet.WriteError(new ErrorRecord(
                                ex,
                                "DataWriteFailed",
                                ErrorCategory.WriteError,
                                filePath));
                            return false;
                        }
                    }
                }
            }
        }
    }
}
