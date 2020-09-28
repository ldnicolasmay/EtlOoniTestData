import java.io.{FileNotFoundException, IOException}

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{S3Object, S3ObjectInputStream}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.annotation.tailrec
import scala.io.Source

package object EtlOoniTestData {

  /**
   * Create a return a Spark session for the request app to use
   *
   * @param awsAccessKeyId AWS access key ID
   * @param awsSecretAccessKey AWS secret access key
   * @return
   */
  def createSparkSession(awsAccessKeyId: String, awsSecretAccessKey: String): SparkSession = {
    println("Creating Spark session...")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("EtlOoniTestData")
      .config("fs.s3.awsAccessKeyId", awsAccessKeyId)
      .config("fs.s3.awsSecretAccessKey", awsSecretAccessKey)
      .config("fs.s3a.access.key", awsAccessKeyId)
      .config("fs.s3a.secret.key", awsSecretAccessKey)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", awsAccessKeyId)
      .config("spark.hadoop.fs.s3a.secret.key", awsSecretAccessKey)
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      // .master("local[*]") // comment out when running with spark-submit
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
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
      }
      catch {
        case e: Exception =>
          System.err.println(s"Error writing to S3 bucket: tableName=${tableName} testName=${testName} date=${date}")
          e.printStackTrace()
          writeTableToS3Parquet(table, tableName, bucket, keyPrefix, testName, date, mode, partitions, retries + 1)
      }
    }
    else {
      ()
    }
  }

  /**
   * Writes Spark Dataset in CSV format to S3 bucket
   *
   * @param table Spark Dataset table
   * @param bucket Target S3 bucket
   * @param key Target S3 key
   * @param mode Write mode, e.g., "overwrite", "append"
   * @param retries Current number of retries for exponential backoff
   */
  @tailrec
  def writeTableToS3Csv(
                       table: Dataset[Row],
                       bucket: String,
                       key: String,
                       mode: String,
                       retries: Int): Unit = {
    val MAX_RETRIES: Int = 5

    if (retries < MAX_RETRIES) { // implement exponential backoff
      val sleepTime: Long = (Math.pow(2, retries) * 500).toLong
      Thread.sleep(sleepTime)

      try {
        table
          .repartition(1)
          .write
          .option("header", true)
          .mode(mode)
          .csv(s"s3://${bucket}/${key}")
      }
      catch {
        case e: Exception =>
          System.err.println(s"Error writing to S3 bucket: bucket=${bucket} key=${key}")
          e.printStackTrace()
          writeTableToS3Csv(table, bucket, key, mode, retries + 1)
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
        case e: AmazonServiceException =>
          System.err.println(s"Error getting s3 key: " +
            s"bucketName=${bucketName} keyPrefix=${keyPrefix} " +
            s"keySuffixRoot=${keySuffixRoot} keySuffixStem=${keySuffixStem}")
          e.printStackTrace()
          getS3KeyLines(bucketName, s3Client, keyPrefix, keySuffixRoot, retries + 1)(filterString, keySuffixStem)
        case e: FileNotFoundException =>
          System.err.println(s"Error getting s3 key: " +
            s"bucketName=${bucketName} keyPrefix=${keyPrefix} " +
            s"keySuffixRoot=${keySuffixRoot} keySuffixStem=${keySuffixStem}")
          e.printStackTrace()
          System.exit(1)
          List()
        case e: IOException =>
          System.err.println(s"Error getting s3 key: " +
            s"bucketName=${bucketName} keyPrefix=${keyPrefix} " +
            s"keySuffixRoot=${keySuffixRoot} keySuffixStem=${keySuffixStem}")
          e.printStackTrace()
          getS3KeyLines(bucketName, s3Client, keyPrefix, keySuffixRoot, retries + 1)(filterString, keySuffixStem)
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

  // Schema for OONI Base Data
  val baseSchema: StructType = StructType(List(
    StructField("id", StringType, nullable = false),
    StructField("bucket_date", DateType, nullable = true),
    StructField("test_name", StringType, nullable = true),
    StructField("measurement_start_time", TimestampType, nullable = true),
    StructField("test_start_time", TimestampType, nullable = true),
    StructField("test_runtime", DoubleType, nullable = true),
    StructField("test_version", StringType, nullable = true),
    StructField("probe_cc", StringType, nullable = true),
    StructField("probe_asn", StringType, nullable = true),
    StructField("report_filename", StringType, nullable = true),
    StructField("report_id", StringType, nullable = true)
  ))

  // Schema extension for OONI HTTP Requests data
  val httpRequestsSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("body_length_match", BooleanType, nullable = true),
      StructField("body_proportion", DoubleType, nullable = true),
      StructField("factor", DoubleType, nullable = true)
    )), nullable = true)
  ))

  // Schema extension for OONI Meek Fronted Requests data
  val meekFrontedRequestsTestSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("success", BooleanType, nullable = true)
    )), nullable = true)
  ))

  // Schema extension for OONI Psiphon data
  val psiphonSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("failure", StringType, nullable = true)
    )), nullable = true)
  ))

  // Schema extension for OONI Tor data
  val torSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("dir_port_total", IntegerType, nullable = true),
      StructField("dir_port_accessible", IntegerType, nullable = true),
      StructField("obfs4_total", IntegerType, nullable = true),
      StructField("obfs4_accessible", IntegerType, nullable = true),
      StructField("or_port_dirauth_total", IntegerType, nullable = true),
      StructField("or_port_dirauth_accessible", IntegerType, nullable = true),
      StructField("or_port_total", IntegerType, nullable = true),
      StructField("or_port_accessible", IntegerType, nullable = true)
    )), nullable = true)
  ))

  // Schema extension for OONI Vanilla Tor data
  val vanillaTorSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("error", StringType, nullable = true),
      StructField("success", BooleanType, nullable = true),
      StructField("timeout", LongType, nullable = true),
      StructField("tor_log", StringType, nullable = true),
      StructField("tor_progress", LongType, nullable = true),
      StructField("tor_progress_summary", StringType, nullable = true),
      StructField("tor_progress_tag", StringType, nullable = true),
      StructField("tor_version", StringType, nullable = true),
      StructField("transport_name", StringType, nullable = true)
    )), nullable = true)
  ))

  // Create map of OONI test schema extensions
  val ooniSchemaExtensions: Map[String, StructType] = Map(
    "http_requests" -> httpRequestsSchemaExtension,
    "meek_fronted_requests_test" -> meekFrontedRequestsTestSchemaExtension,
    "psiphon" -> psiphonSchemaExtension,
    "tor" -> torSchemaExtension,
    "vanilla_tor" -> vanillaTorSchemaExtension
  )

  // Combine the OONI Base Data schema with the schema extensions
  val ooniSchemas: Map[String, StructType] = for {
    (targetTestName, targetTestSchemaExtension) <- ooniSchemaExtensions
  } yield (targetTestName, StructType(baseSchema ++ targetTestSchemaExtension))

  // Create Spark Dataset Row encodings from the extended OONI schemas
  val ooniEncoders: Map[String, ExpressionEncoder[Row]] = for {
    (targetTestName, targetTestSchema) <- ooniSchemas
  } yield (targetTestName, RowEncoder(targetTestSchema))

}