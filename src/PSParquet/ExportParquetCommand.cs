using System.Management.Automation;
using System.Collections.Generic;
using System.IO;

namespace PSParquet
{
    [Cmdlet("Export", "Parquet", SupportsShouldProcess = true)]
    [OutputType(typeof(PSCustomObject))]
    public class ExportParquetCommand : PSCmdlet
    {
        [Parameter(
            Mandatory = true,
            Position = 0,
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false)]
        public FileInfo FilePath { get; set; }
        [Parameter(
        Mandatory = true,
        Position = 1,
        ValueFromPipeline = true,
        ValueFromPipelineByPropertyName = false)]
        [AllowEmptyCollection]
        public PSObject[] InputObject { get; set; }
        [Parameter(
        Mandatory = false,
        Position = 2,
        ValueFromPipeline = false,
        ValueFromPipelineByPropertyName = false)]
        public SwitchParameter PassThru { get; set; }
        [Parameter(
        Mandatory = false,
        Position = 3,
        ValueFromPipeline = false,
        ValueFromPipelineByPropertyName = false)]
        public SwitchParameter Force { get; set; }
        [Parameter(
        Mandatory = false,
        ValueFromPipeline = false,
        ValueFromPipelineByPropertyName = false)]
        [ValidateSet("None", "Gzip", "Snappy", "Lzo", "Brotli", "Zstd")]
        public string CompressionMethod { get; set; } = "Gzip";
        [Parameter(
        Mandatory = false,
        ValueFromPipeline = false,
        ValueFromPipelineByPropertyName = false)]
        [ValidateSet("Optimal", "Fastest", "NoCompression", "SmallestSize")]
        public string CompressionLevel { get; set; } = "Optimal";

        private readonly List<PSObject> inputObjects = new List<PSObject>();


        protected override void BeginProcessing()
        {
            WriteVerbose("Using: " + FilePath.FullName);
        }

        protected override void ProcessRecord()
        {
            WriteDebug("Adding to List");
            if (InputObject != null && InputObject.Length > 0)
            {
                inputObjects.AddRange(InputObject);
            }
        }

        protected override void EndProcessing()
        {
            if (inputObjects.Count == 0)
            {
                WriteWarning("No objects to export.");
                return;
            }

            bool fileExists = FilePath.Exists;

            if (fileExists && !Force)
            {
                if (!ShouldContinue(FilePath.FullName, "Overwrite the existing file?"))
                {
                    WriteVerbose("Export cancelled by user.");
                    return;
                }
            }

            string operation = fileExists ? "Overwrite Parquet file" : "Create Parquet file";
            if (!ShouldProcess(FilePath.FullName, operation))
            {
                WriteVerbose("Operation skipped (WhatIf/Confirm).");
                return;
            }

            if (fileExists)
            {
                WriteVerbose($"Deleting: {FilePath}");
                FilePath.Delete();
            }

            var collectedObjects = inputObjects.ToArray();
            
            WriteVerbose($"Writing {collectedObjects.Length} objects to {FilePath.FullName}");
            var result = PSParquet.WriteParquetFile(collectedObjects, FilePath.FullName, CompressionMethod, CompressionLevel, this);
            if (!result.Result)
            {
                // Error already written by WriteParquetFile
                return;
            }
            else
            {
                WriteVerbose($"InputObject has been exported to {FilePath}");
            }

            if (PassThru)
            {
                WriteObject(collectedObjects);
            }
        }
    }
}
