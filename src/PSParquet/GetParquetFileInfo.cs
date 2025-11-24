using System;
using System.Collections.Generic;
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

        private readonly List<FileInfo> filesToInspect = new();

        protected override void BeginProcessing()
        {
            filesToInspect.Clear();
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
            
            WriteVerbose($"Queued file for info retrieval: {FilePath.FullName}");
            filesToInspect.Add(FilePath);
        }

        protected override void EndProcessing()
        {
            foreach (var file in filesToInspect)
            {
                try
                {
                    WriteVerbose($"Reading file info: {file.FullName}");
                    var fileInfo = PSParquet.GetParquetFileInfo(file.FullName).GetAwaiter().GetResult();
                    WriteObject(fileInfo);
                }
                catch (System.Exception ex)
                {
                    WriteError(new ErrorRecord(
                        ex,
                        "GetFileInfoFailed",
                        ErrorCategory.ReadError,
                        file));
                }
            }
        }
    }
}