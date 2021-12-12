import sys.process._
import scala.language.postfixOps
import scala.sys.process.ProcessLogger

var download_path: String = "/home/data"
var input_path: String = "s3://data-ingestion-landing-zone/jobs/publish-data-to-S3/16/data/"

"mkdir -p $download_path" !!

Seq("aws", "s3", "cp", input_path, download_path, "--recursive").!
