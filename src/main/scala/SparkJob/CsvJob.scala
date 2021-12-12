package SparkJob

import SparkJob.Domain._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.mllib._
import org.apache.spark.ml.feature
import java.io.File
import sys.process._
import scala.language.postfixOps
import scala.sys.process.ProcessLogger


object CsvJob extends DataJob[Array[DataFrame], DataFrame] {



    override def read(params:SparkParams)(implicit spark: SparkSession) = {

        // Function to get list of files in a directory
        def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
            dir.listFiles.filter(_.isFile).toList.filter { file =>
                extensions.exists(file.getName.endsWith(_))
            }
        }

        // Make directory to store folder from S3
        var s3_path: String = params.inPath
        var csv_dir: String = "/home/hadoop/data/"

        "mkdir -p $csv_dir" !!

        println("Made new directory")

        // Download the csv files from s3 path
        Seq("aws", "s3", "cp", s3_path, csv_dir, "--recursive").!
        println("Downloaded folder from S3")

        val csvFileExtensions = List("csv")
        val files = getListOfFiles(new File(csv_dir), csvFileExtensions)
        println(files)
        
        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        var dataReader = spark.read
         params.inOptions.toSeq.foreach{
            op => dataReader = dataReader.option(op._1, op._2)
        }

        // Function to check if file list contains Connections, Invitations, Positions, Profile, messages then read them
        def read_dataframe(text: List[File], chr: String): DataFrame = text match{
            case _ if text.toString.contains(chr) => dataReader.csv(chr)
            case _ => spark.emptyDataFrame
        }

        // Read corresponding dataframes
        var connections = read_dataframe(files, csv_dir + "Connections.csv")
        var invitations = read_dataframe(files,csv_dir + "Invitations.csv")
        var positions = read_dataframe(files, csv_dir + "Positions.csv")
        var profile = read_dataframe(files, csv_dir + "Profile.csv")
        var messages = read_dataframe(files, csv_dir + "messages.csv")

        val inputDFs = Array(connections, invitations, positions, profile,  messages)

        connections.show

        inputDFs
    }

    override def transform(inputData: Array[DataFrame])(implicit spark:SparkSession, sparkParams:SparkParams) = {
        import spark.implicits._


        // Parse Read dataframe Array
        var connections = inputData(0)
        var invitations = inputData(1)
        var positions = inputData(2)
        var profile = inputData(3)
        var messages = inputData(4)

        connections.show
        invitations.show
        positions.show
        profile.show
        messages.show


        // Working on connections data
        // Convert "Connected On" to date, get day of week and month
        // Get all the companies and positions of connections each day and order it by "Date"
        connections = connections
            .withColumn("Date_c", to_date($"Connected On", "dd-MMM-yy"))
            .drop("Connected On")
            .groupBy("Date_c")
            .agg(count(lit(1)).alias("No_of_Connections"),
                concat_ws(" ", collect_list("Company")) as "Connections_Companies",
                concat_ws(" ", collect_list("Position")) as "Connections_Positions")
            //.orderBy("Date_c")


        // Working on invitations data
        // Convert "Sent At" to datetime, get day of week and month
        // Get all the companies and positions of connections each day and order it by "Date"
        invitations = invitations
            .withColumn("Date_i", split($"Sent At", ", ").getItem(0))
            .withColumn("Time", split($"Sent At", ", ").getItem(1))
            .withColumn("Date_i", to_date($"Date_i", "MM/dd/yy"))
            .withColumn("Outgoing", $"Direction" === "OUTGOING")
            .withColumn("Incoming", $"Direction" === "INCOMING")
            .groupBy("Date_i")
            .agg(sum($"Outgoing".cast("long")).alias("Outgoing_Invites"),
                sum($"Incoming".cast("long")).alias("Incoming_Invites"))
            //.orderBy("Date_i")
            
            
        // Working on positions data
        var years_of_experience: String = positions
            .withColumn("Time Spent", ($"Finished On" - $"Started On"))
            .withColumn("Time Spent", $"Time Spent".cast(IntegerType))
            .agg(sum("Time Spent").alias("Years_of_experience"))//.first.get(0)
            .select("Years_of_experience")
            .first().mkString("")
            

        // Working on profile data
        // Get username
        var user_name: String = profile
            .select("First Name", "Last Name")
            .select(concat($"First Name", lit(" "), $"Last Name"))
            .first()
            .mkString("")

        
        // Working on messages data 
        // Initialize stop words remover to remove stop words from subject and content
        val subject_remover = new feature.StopWordsRemover()
            .setInputCol("SUBJECT")
            .setOutputCol("subject_transformed")
        val content_remover = new feature.StopWordsRemover()
            .setInputCol("CONTENT")
            .setOutputCol("content_transformed")
        // Function to convert null values to empty array
        val array_ = udf(() => Array.empty[Int])
        
        // Convert "Connected On" to date, get day of week and month
        // Get all the companies and positions of connections each day and order it by "Date"
        messages = messages
            .withColumn("Date_m", split($"DATE", " ").getItem(0))
            .withColumn("Date_m", to_date($"Date", "yyyy-MM-dd"))
            .filter($"TO" === user_name)
            // Get the first message in a conversation
            .groupBy("Date_m", "CONVERSATION ID")
            .agg(first("SUBJECT").alias("SUBJECT"), first("CONTENT").alias("CONTENT"))
            // Replace characters that are not letters  with empty strings and lowercase the remaining characters
            .withColumn("SUBJECT", lower(regexp_replace($"SUBJECT", "[-+.^:,&;-<>!%|{}()@#*$^~?/\''=0-9�👍]", "")))
            .withColumn("CONTENT", lower(regexp_replace($"CONTENT", "[-+.^:,&;-<>!%|{}()@#*$^~?/\''=0-9�👍]", "")))
            .withColumn("SUBJECT", split($"SUBJECT", " "))
            .withColumn("CONTENT", split($"CONTENT", " "))
            // Convert null values to empty array
            .withColumn("SUBJECT", when($"SUBJECT".isNull, array_()).otherwise($"SUBJECT"))
            .withColumn("SUBJECT", coalesce($"SUBJECT", array_()))
            .withColumn("CONTENT", when($"CONTENT".isNull, array_()).otherwise($"CONTENT"))
            .withColumn("CONTENT", coalesce($"CONTENT", array_()))

        // Remove stop words from subject and content
        messages = subject_remover.transform(messages)
        messages = content_remover.transform(messages)

        // Combine all the conversations each day
        messages = messages 
            .withColumn("subject_transformed", concat_ws(" ", $"subject_transformed"))
            .withColumn("content_transformed", concat_ws(" ", $"content_transformed"))
            .groupBy("Date_m")
            .agg(count(lit(1)).alias("No_of_Conversations"),
                concat_ws(" ", collect_list("subject_transformed")) as "Subject",
                concat_ws(" ", collect_list("content_transformed")) as "Content")


        // Join connnections, messages, invitations by Date
        // Create seperate columns for username and years of experience
        var connect_invite_df = connections.join(invitations, 
            connections("Date_c") === invitations("Date_i"), 
            "Outer")
            .withColumn("Date_c", when($"Date_i".isNull, $"Date_c").otherwise($"Date_i"))

        val outputDF = connect_invite_df.join(messages, 
            connect_invite_df("Date_c") === messages("Date_m"), 
            "Outer")
            // Replace NULLs
            .withColumn("Date_c", when($"Date_m".isNull, $"Date_c").otherwise($"Date_m"))
            .withColumn("No_of_Connections", when($"No_of_Connections".isNull, 0).otherwise($"No_of_Connections"))
            .withColumn("Outgoing_Invites", when($"Outgoing_Invites".isNull, 0).otherwise($"Outgoing_Invites"))
            .withColumn("Incoming_Invites", when($"Incoming_Invites".isNull, 0).otherwise($"Incoming_Invites"))
            .withColumn("No_of_Conversations", when($"No_of_Conversations".isNull, 0).otherwise($"No_of_Conversations"))
            .withColumn("Connections_Companies", when($"Connections_Companies".isNull, "").otherwise($"Connections_Companies"))
            .withColumn("Connections_Positions", when($"Connections_Positions".isNull, "").otherwise($"Connections_Positions"))
            .withColumn("Subject", when($"Subject".isNull, "").otherwise($"Subject"))
            .withColumn("Content", when($"Content".isNull, "").otherwise($"Content"))
            // Drop columns not needed
            .drop($"Date_m")
            .drop($"Date_i")
            .withColumnRenamed("Date_c","Date")
            .withColumn("Day", date_format($"Date", "EEEE"))
            .withColumn("Month", date_format($"Date", "MMMM"))
            .withColumn("Year", date_format($"Date", "yyyy"))
            .withColumn("User_Name", lit(user_name))
            .withColumn("Experience", lit(years_of_experience))
            // Reorder
            .select($"User_Name", $"Experience", $"Date", $"Year", $"Month", $"Day", $"No_of_Connections", 
                    $"Outgoing_Invites", $"Incoming_Invites", $"No_of_Conversations", 
                    $"Connections_Companies", $"Connections_Positions", $"Subject", $"Content")
            .orderBy("Date")

        outputDF.show

        SaveParameters(outputDF,sparkParams)

    }

    override def save(p:SaveParameters) {
        p.df.write
        .partitionBy(p.params.partitionColumn)
        .options(p.params.outOptions)
        .format(p.params.outFormat)
        .mode(p.params.saveMode)
        .save(p.params.outPath)
    }


   


}