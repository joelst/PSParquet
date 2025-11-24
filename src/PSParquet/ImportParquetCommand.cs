using System;
using System.Collections.Generic;
using System.Management.Automation;
using System.IO;


namespace PSParquet
{
    [Cmdlet("Import", "Parquet")]
    [OutputType(typeof(PSCustomObject))]
    public class ImportParquetCommand : PSCmdlet
    {
        [Parameter(
            Mandatory = true,
            Position = 0,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        [Alias("FullName")]
        public FileInfo FilePath { get; set; }

        [Parameter(Mandatory = false)]
        [ValidateRange(1, int.MaxValue)]
        public int FirstNGroups { get; set; } = 0;

        private readonly List<FileInfo> filesToProcess = new();

        protected override void BeginProcessing()
        {
            filesToProcess.Clear();
        }

        protected override void ProcessRecord()
        {
            if (FilePath == null)
            {
                WriteError(new ErrorRecord(
                    new ArgumentNullException(nameof(FilePath)),
                    "FilePathNull",
                    ErrorCategory.InvalidArgument,
                    null));
                return;
            }
            
            if (!FilePath.Exists)
            {
                WriteError(new ErrorRecord(
                    new FileNotFoundException($"File not found: {FilePath}"),
                    "FileNotFound",
                    ErrorCategory.ObjectNotFound,
                    FilePath));
                return;
            }
            
            WriteVerbose($"Queued file for import: {FilePath.FullName}");
            filesToProcess.Add(FilePath);
        }


        protected override void EndProcessing()
        {
            foreach (var file in filesToProcess)
            {
                try
                {
                    var objs = PSParquet.GetParquetObjects(file.FullName, FirstNGroups).GetAwaiter().GetResult();
                    
                    if (objs.Count == 0)
                    {
                        WriteVerbose($"No objects found in Parquet file: {file.FullName}");
                    }
                    else
                    {
                        WriteVerbose($"Imported {objs.Count} object(s) from {file.FullName}");
                    }
                    
                    objs.ForEach(obj => WriteObject(obj));
                }
                catch (System.Exception ex)
                {
                    WriteError(new ErrorRecord(
                        ex,
                        "ImportFailed",
                        ErrorCategory.ReadError,
                        file));
                }
            }
        }
    }
}
