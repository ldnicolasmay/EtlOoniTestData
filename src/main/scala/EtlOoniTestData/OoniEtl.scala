package EtlOoniTestData

import EtlOoniTestData.AwsConfig.{awsAccessKeyId, awsSecretAccessKey}
import EtlOoniTestData.OoniConfig.{ooniBucketName, ooniPrefixDates, ooniTargetTestNames}
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{S3Object, S3ObjectInputStream}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

object OoniEtl {

  /**
   * ETLs base data for each OONI test following star schema data model
   *
   * @param spark            Spark session
   * @param s3Client         S3 client for interacting with AWS S3
   * @param targetBucketName Target S3 bucket name
   * @param keyPrefix        Target S3 key prefix, e.g., "parquet/ooni"
   * @param ooniBucketName   OONI S3 bucket name
   * @param testName         OONI test name, e.g., "http_requests"
   * @param date             Date, e.g., "2020-01"
   * @param s3Keys           S3 keys loaded from S3 bucket
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
   * Driver method for OONI ETL Spark app
   *
   * @param args Arguments passed to main method
   */
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
      s3Keys = getS3KeyLines(
        targetBucketName,
        s3Client,
        "keys",
        "ooni_s3_keys_",
        0)(
        test,
        date)
    } yield (test, date, s3Keys)

    val spark: SparkSession = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    testDateKeys.foreach {
      case (test, date, s3Keys) =>
        etlTargetTestData(
          spark,
          s3Client,
          targetBucketName,
          "parquet/ooni",
          ooniBucketName)(
          test,
          date,
          s3Keys)
    }

    spark.stop()
  }
}