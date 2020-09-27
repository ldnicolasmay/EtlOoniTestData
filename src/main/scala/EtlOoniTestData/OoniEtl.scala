package EtlOoniTestData

import java.io.{File, FileNotFoundException, IOException}

import AwsConfig.{awsAccessKeyId, awsSecretAccessKey}
import OoniConfig.{ooniBucketName, ooniPrefixDates, ooniTargetTestNames}
import PersistentS3Client.s3Client
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.model.{S3Object, S3ObjectInputStream}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
//import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.annotation.tailrec
//import scala.collection.JavaConversions._
import scala.io.Source

object OoniEtl {

  /**
   * ETLs base data for each OONI test following star schema data model
   *
   * @param spark          Spark session
   * @param s3Client       S3 client for interacting with AWS S3
   * @param targetBucketName     Target S3 bucket name
   * @param keyPrefix      Target S3 key prefix, e.g., "parquet/ooni"
   * @param ooniBucketName OONI S3 bucket name
   * @param testName       OONI test name, e.g., "http_requests"
   * @param date           Date, e.g., "2020-01"
   * @param s3Keys         S3 keys loaded from S3 bucket
   */
  def etlTargetTestData(
                         spark: SparkSession,
                         s3Client: SerializableAmazonS3Client,
                         targetBucketName: String,
                         keyPrefix: String,
                         ooniBucketName: String
                       )(testName: String,
                         date: String,
                         s3Keys: List[String]): Unit = {
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext

    /*=============================================*/
    /*=== EXTRACT: Get Data from OONI S3 Bucket ===*/

    // Filter incoming OONI S3 keys for target test name
    val targetTestS3Keys: List[String] = s3Keys.filter(_.contains("-" + testName + "-"))
    println(s"${testName} ${date} targetTestS3Keys length: ${targetTestS3Keys.length}")

    // Using OONI S3 keys, read data within the JSONL files at those S3 keys to a Spark RDD
    val targetTestRdd: RDD[String] = sc.parallelize(targetTestS3Keys)
      .mapPartitions { it =>
        it.flatMap { key =>
          val s3Object: S3Object = s3Client.getObject(ooniBucketName, key)
          val s3ObjectInputStream: S3ObjectInputStream = s3Object.getObjectContent
          try {
            Source.fromInputStream(s3ObjectInputStream, "ISO-8859-1").getLines
          }
          catch {
            case e: AmazonServiceException =>
              println(e)
              List()
          }
        }
      }

    // Convert Spark RDD to Spark DataSet
    val targetTestDs: Dataset[Row] = spark.sqlContext.read
      .schema(ooniSchemas(testName))
      .json(targetTestRdd.toDS)
      .as(ooniEncoders(testName))

    /*================================================================*/
    /*=== TRANSFORM: Filter Data, Create Fact and Dimension Tables ===*/

    // Filter DataSet rows to ensure only target test data remain
    val filteredTargetTestDs: Dataset[Row] = targetTestDs
      .filter(col("test_name").equalTo(testName))
      .persist()
    println(s"${testName} ${date} Dataset Row Count: ${filteredTargetTestDs.count()}")

    // Create fact table: Test Table
    val targetOoniTestTable: Dataset[Row] = filteredTargetTestDs
      .select("id",
        "measurement_start_time",
        "test_start_time",
        "probe_cc",
        "probe_asn",
        "report_id")
      .dropDuplicates()

    // Part of dimension table: Measurement Start Time Table - First part of Start Time Table (see union below)
    val targetOoniMeasurementStartTimeTable = filteredTargetTestDs
      .select("measurement_start_time")
      .withColumnRenamed("measurement_start_time", "start_time")
      .dropDuplicates()

    // Part of dimension table: Test Start Time Table - Second part of Start Time Table (see union below)
    val targetOoniTestStartTimeTable = filteredTargetTestDs
      .select("test_start_time")
      .withColumnRenamed("test_start_time", "start_time")
      .dropDuplicates()

    // Create dimension table: Start Time Table - Combine Measurement Start Time Table and Test Start Time Table
    val targetOoniStartTimeTable = targetOoniMeasurementStartTimeTable.union(targetOoniTestStartTimeTable)
      .dropDuplicates()
      .withColumn("hour", hour(col("start_time")))
      .withColumn("day", dayofmonth(col("start_time")))
      .withColumn("weekday", dayofweek(col("start_time")))
      .withColumn("week", weekofyear(col("start_time")))
      .withColumn("month", month(col("start_time")))
      .withColumn("year", year(col("start_time")))

    // Create dimension table: Report Table
    val targetOoniReportTable: Dataset[Row] = filteredTargetTestDs
      .select("report_id", "report_filename")
      .dropDuplicates()

    /*====================================*/
    /*=== LOAD: Write as parquet to S3 ===*/

    // Write Test Table (fact) as parquet to target S3
    writeTableToS3Parquet(
      table = targetOoniTestTable,
      tableName = "test_table",
      bucket = targetBucketName,
      keyPrefix = keyPrefix,
      testName = testName,
      date = date,
      mode = "overwrite",
      partitions = Seq(),
      retries = 0)

    // Write Start Time Table (dimension) as parquet to S3
    writeTableToS3Parquet(
      table = targetOoniStartTimeTable,
      tableName = "start_time_table",
      bucket = targetBucketName,
      keyPrefix = keyPrefix,
      testName = testName,
      date = date,
      mode = "overwrite",
      partitions = Seq("year", "month"),
      retries = 0)


    // Write Report Table (dimension) as parquet to S3
    writeTableToS3Parquet(
      table = targetOoniReportTable,
      tableName = "report_table",
      bucket = targetBucketName,
      keyPrefix = keyPrefix,
      testName = testName,
      date = date,
      mode = "overwrite",
      partitions = Seq(),
      retries = 0)

    filteredTargetTestDs.unpersist()
  }

  /**
   * Writes Spark Dataset in parquet format to S3 bucket
   *
   * @param table      Spark Dataset table
   * @param tableName  Name of Dataset table, e.g., "report_table"
   * @param bucket     Target S3 bucket
   * @param keyPrefix  Target key prefix, e.g., "parquet/ooni"
   * @param testName   Name of test, e.g., "http_requests"
   * @param date       Date, e.g., "2020-01"
   * @param mode       Write mode, "overwrite"/"append"
   * @param partitions Parquet partitions, e.g., Seq("year", "month")
   * @param retries    Current number of retries for exponential backoff
   */
  @tailrec
  def writeTableToS3Parquet(
                             table: Dataset[Row],
                             tableName: String,
                             bucket: String,
                             keyPrefix: String,
                             testName: String,
                             date: String,
                             mode: String,
                             partitions: Seq[String],
                             retries: Int): Unit = {
    val MAX_RETRIES: Int = 5

    if (retries < MAX_RETRIES) { // implement exponential backoff
      val sleepTime: Long = (Math.pow(2, retries) * 500).toLong
      Thread.sleep(sleepTime)

      try {
        if (partitions.isEmpty) {
          table.write
            .mode(mode)
            .parquet(s"s3a://${bucket}/${keyPrefix}/${testName}/${tableName}/${date}.parquet")
        } else {
          table.write
            .partitionBy(partitions: _*)
            .mode(mode)
            .parquet(s"s3a://${bucket}/${keyPrefix}/${testName}/${tableName}/${date}.parquet")
        }
        println(s"Written: s3a://${bucket}/${keyPrefix}/${testName}/${tableName}/${date}.parquet")
      } catch {
        case e: Exception =>
          System.err.println(s"Error writing to S3 bucket: tableName=${tableName} testName=${testName} date=${date}")
          writeTableToS3Parquet(table, tableName, bucket, keyPrefix, testName, date, mode, partitions, retries + 1)
      }
    }
    else {
      ()
    }
  }

  /**
   * Retrieve relevant S3 key names
   *
   * @param bucketName    Source S3 bucket, e.g., "udacity-ooni-project"
   * @param s3Client      S3 client for interacting with AWS S3
   * @param keyPrefix     Source key prefix, e.g., "keys"
   * @param keySuffixRoot Source key suffix root, e.g., "ooni_s3_keys"
   * @param retries       Current number of retries for exponential backoff
   * @param filterString  String to filter on S3 key names
   * @param keySuffixStem Key suffix stem, e.g., "ooni_s3_keys_"
   * @return
   */
  def getS3KeyLines(
                     bucketName: String,
                     s3Client: SerializableAmazonS3Client,
                     keyPrefix: String,
                     keySuffixRoot: String,
                     retries: Int
                   )(filterString: String,
                     keySuffixStem: String): List[String] = {
    val MAX_RETRIES: Int = 5

    val key = s"$keyPrefix/$keySuffixRoot$keySuffixStem.dat"
    println(s"${filterString} -- s3://${bucketName}/${key}")

    if (retries < MAX_RETRIES) { // implement exponential backoff
      val sleepTime: Long = (Math.pow(2, retries) * 500).toLong
      Thread.sleep(sleepTime)

      val s3Object: S3Object = s3Client.getObject(bucketName, key)
      val s3ObjectInputStream: S3ObjectInputStream = s3Object.getObjectContent

      try {
        Source.fromInputStream(s3ObjectInputStream)
          .getLines()
          .filter(_.contains(filterString))
          .toList
      }
      catch {
        case e: AmazonServiceException => {
          System.err.println(s"Error getting s3 key: " +
            s"bucketName=${bucketName} keyPrefix=${keyPrefix} " +
            s"keySuffixRoot=${keySuffixRoot} keySuffixStem=${keySuffixStem}")
          getS3KeyLines(bucketName, s3Client, keyPrefix, keySuffixRoot, retries + 1)(filterString, keySuffixStem)
          // getS3KeyLines(bucketName, keyPrefix, keySuffixRoot, retries + 1)(filterString, keySuffixStem)
        }
        case e: FileNotFoundException => {
          System.err.println(s"Error getting s3 key: " +
            s"bucketName=${bucketName} keyPrefix=${keyPrefix} " +
            s"keySuffixRoot=${keySuffixRoot} keySuffixStem=${keySuffixStem}")
          e.printStackTrace()
          System.exit(1)
          List()
        }
        case e: IOException => {
          System.err.println(s"Error getting s3 key: " +
            s"bucketName=${bucketName} keyPrefix=${keyPrefix} " +
            s"keySuffixRoot=${keySuffixRoot} keySuffixStem=${keySuffixStem}")
          getS3KeyLines(bucketName, s3Client, keyPrefix, keySuffixRoot, retries + 1)(filterString, keySuffixStem)
          // getS3KeyLines(bucketName, keyPrefix, keySuffixRoot, retries + 1)(filterString, keySuffixStem)
        }
      }
      finally {
        s3ObjectInputStream.close()
        s3Object.close()
      }
    }
    else {
      List()
    }
  }

  def main(args: Array[String]): Unit = {

    // Logger
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Get S3 client
    val s3Client = PersistentS3Client.s3Client

    // Read S3 key names from dat files from S3 bucket into one list: s3://udacity-ooni-project/keys/
    val targetBucketName: String = "udacity-ooni-project"

    // 1. Filter the list of key names to only those of target
    // 2. Pass S3 bucket name + list of S3 filtered key names to Spark to read them all as an RDD
    // 3. For each target test RDD, transform to Dataset

    val testDateKeys: List[(String, String, List[String])] = for {
      test <- ooniTargetTestNames
      date <- ooniPrefixDates
       s3Keys = getS3KeyLines(targetBucketName, s3Client, "keys", "ooni_s3_keys_", 0)(test, date)
    } yield (test, date, s3Keys)

    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    testDateKeys.foreach {
      case (test, date, s3Keys) =>
         etlTargetTestData(spark, s3Client, targetBucketName, "parquet/ooni", ooniBucketName)(test, date, s3Keys)
    }

    spark.stop()
  }
}