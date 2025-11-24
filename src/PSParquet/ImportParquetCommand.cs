using System;
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

        protected override void BeginProcessing()
        {
            // Pipeline values aren't available yet, validation happens in ProcessRecord
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
            
            WriteVerbose($"Importing from: {FilePath.FullName}");
        }


        protected override void EndProcessing()
        {
            if (FilePath == null || !FilePath.Exists)
            {
                return;
            }
            
            try
            {
                var objs = PSParquet.GetParquetObjects(FilePath.FullName, FirstNGroups).GetAwaiter().GetResult();
                
                if (objs.Count == 0)
                {
                    WriteVerbose("No objects found in Parquet file.");
                }
                else
                {
                    WriteVerbose($"Imported {objs.Count} object(s) from {FilePath.FullName}");
                }
                
                objs.ForEach(obj => WriteObject(obj));
            }
            catch (System.Exception ex)
            {
                WriteError(new ErrorRecord(
                    ex,
                    "ImportFailed",
                    ErrorCategory.ReadError,
                    FilePath));
            }
        }
    }
}
