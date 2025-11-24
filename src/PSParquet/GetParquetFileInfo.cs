using System;
using System.Management.Automation;
using System.IO;


namespace PSParquet
{
    [Cmdlet("Get", "ParquetFileInfo")]
    [OutputType(typeof(PSCustomObject))]
    public class GetParquetFileInfoCommand : PSCmdlet
    {
        [Parameter(
            Mandatory = true,
            Position = 0,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        [Alias("FullName")]
        public FileInfo FilePath { get; set; }

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
            
            try
            {
                WriteVerbose($"Reading file info: {FilePath.FullName}");
                var fileInfo = PSParquet.GetParquetFileInfo(FilePath.FullName).GetAwaiter().GetResult();
                WriteObject(fileInfo);
            }
            catch (System.Exception ex)
            {
                WriteError(new ErrorRecord(
                    ex,
                    "GetFileInfoFailed",
                    ErrorCategory.ReadError,
                    FilePath));
            }
        }
        protected override void EndProcessing()
        {

        }
    }
}