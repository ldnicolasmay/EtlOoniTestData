package EtlOoniTestData

import java.io.{FileNotFoundException, IOException}
import java.nio.charset.{Charset, CharsetDecoder, CodingErrorAction}

import EtlOoniTestData.AwsConfig.{awsAccessKeyId, awsSecretAccessKey}
import EtlOoniTestData.OoniConfig.{ooniBucketName, ooniPrefixDates, ooniTargetTestNames}
import EtlOoniTestData.PersistentS3Client.s3Client
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{S3Object, S3ObjectInputStream}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.collection.JavaConversions
import scala.collection.parallel.ParMap


object OoniEtl {

  /**
   * ETLs base data for each OONI test following to star schema data model
   *
   * @param spark      Spark session
   * @param s3Keys     Filtered S3 keys loaded from S3 bucket
   * @param bucketName Target S3 bucket name
   * @param keyPrefix  Target S3 key prefix, e.g., "parquet/ooni"
   * @param testName   OONI test name, e.g., "http_requests", "vanilla_tor"
   */
  // case (test, date, s3Keys) => etlTargetTestData(spark, bucketName, "parquet/ooni_temp")(test, date, s3Keys)
  def etlTargetTestData(
                         spark: SparkSession,
                         //s3Keys: List[String],
                         bucketName: String,
                         keyPrefix: String
                       )(testName: String,
                         date: String,
                         s3Keys: List[String]): Unit = {

    import spark.implicits._
    val sc: SparkContext = spark.sparkContext

    /*=== EXTRACT: Get Data from OONI S3 Bucket ===*/

//    println(s3Keys.length)
//    s3Keys.foreach(println)
//    println("\n ========================== \n")

    // Filter incoming OONI S3 keys for target test name
    val targetTestS3Keys: List[String] = s3Keys.filter(_.contains("-" + testName + "-"))
    println(s"${testName} ${date} targetTestS3Keys length: ${targetTestS3Keys.length}")

//    println(targetTestS3Keys.length)
//    targetTestS3Keys.foreach(println)
//    println("\n ++++++++++++++++++++++++++ \n")

    // Using OONI S3 keys, read data within the JSONL files at those S3 keys to a Spark RDD
    val targetTestRdd: RDD[String] = sc.parallelize(targetTestS3Keys)
      .mapPartitions { it =>
        it.flatMap { key =>
          val s3Object = s3Client.getObject(ooniBucketName, key)
          val s3ObjectInputStream = s3Object.getObjectContent
          // val decoder: CharsetDecoder = Charset.forName("UTF-8").newDecoder()
          // decoder.onMalformedInput(CodingErrorAction.IGNORE)
          try {
            Source.fromInputStream(s3ObjectInputStream, "ISO-8859-1").getLines
          }
          catch {
            case e: AmazonServiceException =>
              println(e)
              List()
          }
          finally {
            // s3ObjectInputStream.close()
            // s3Object.close()
          }
        }
      }

    // Convert Spark RDD to Spark DataSet
    val targetTestDs = spark.sqlContext.read
      .schema(ooniSchemas(testName))
      .json(targetTestRdd.toDS)
      .as(ooniEncoders(testName))
//    println(s"${testName}: ${targetTestDs.select("test_name").distinct().collect().mkString("Array(", ", ", ")")}")

    /*=== TRANSFORM: Filter Data, Create Fact and Dimension Tables ===*/

    // Filter DataSet rows to ensure only target test data remain
    val filteredTargetTestDs = targetTestDs
      .filter(col("test_name").equalTo(testName))
      .persist()
//     println(s"${testName} ${date} Dataset created")
//    println(s"${testName} ${date} Dataset Row Count: ${filteredTargetTestDs.count()}")

    // Create fact table: Test Table
    val targetOoniTestTable = filteredTargetTestDs
      .select("id",
        "measurement_start_time",
        "test_start_time",
        "probe_cc",
        "probe_asn",
        "report_id")
      .dropDuplicates()
//     println(s"${testName}/${date}_test_table rows: ${targetOoniTestTable.count()}")
//    println(s"${testName} ${date} Dataset filtered")

    // Part of dimension table: Measurement Start Time Table - First part of Start Time Table (see union below)
    val targetOoniMeasurementStartTimeTable = filteredTargetTestDs
      .select("measurement_start_time")
      .withColumnRenamed("measurement_start_time", "start_time")
      .dropDuplicates()
    // targetOoniMeasurementStartTimeTable.printSchema()
//     println(s"${testName}/${date}_measurement_start_time_table rows: ${targetOoniMeasurementStartTimeTable.count()}")
    // targetOoniMeasurementStartTimeTable.show(10, 50)

    // Part of dimension table: Test Start Time Table - Second part of Start Time Table (see union below)
    val targetOoniTestStartTimeTable = filteredTargetTestDs
      .select("test_start_time")
      .withColumnRenamed("test_start_time", "start_time")
      .dropDuplicates()
    // targetOoniTestStartTimeTable.printSchema()
//     println(s"${testName}/${date}_test_start_time_table rows: ${targetOoniTestStartTimeTable.count()}")
    // targetOoniTestStartTimeTable.show(10, 50)

    // Create dimension table: Start Time Table - Combine Measurement Start Time Table and Test Start Time Table
    val targetOoniStartTimeTable = targetOoniMeasurementStartTimeTable.union(targetOoniTestStartTimeTable)
      .dropDuplicates()
      .withColumn("hour", hour(col("start_time")))
      .withColumn("day", dayofmonth(col("start_time")))
      .withColumn("weekday", dayofweek(col("start_time")))
      .withColumn("week", weekofyear(col("start_time")))
      .withColumn("month", month(col("start_time")))
      .withColumn("year", year(col("start_time")))
    // targetOoniStartTimeTable.printSchema()
//     println(s"${testName}/${date}_start_time_table rows: ${targetOoniStartTimeTable.count()}")
    // targetOoniStartTimeTable.show(10, 50)
    // println(s"${testName}: ${targetOoniStartTimeTable.select("year").distinct().collect().mkString("Array(", ", ", ")")}")
    println(s"${testName} ${date} targetOoniStartTimeTable created")

    // Create dimension table: Report Table
    val targetOoniReportTable = filteredTargetTestDs
      .select("report_id", "report_filename")
      .dropDuplicates()
//     println(s"${testName}/${date}_report_table rows: ${targetOoniReportTable.count()}")

    /*=== LOAD: Write as parquet to S3 ===*/

    // Write Test Table (fact) as parquet to target S3
    targetOoniTestTable.write
      .mode("overwrite")
      .parquet(s"s3a://${bucketName}/${keyPrefix}/${testName}/${date}_test_table.parquet")
    println(s"Written: s3a://${bucketName}/${keyPrefix}/${testName}/${date}_test_table.parquet")

    // Write Start Time Table (dimension) as parquet to S3
    targetOoniStartTimeTable.write
      .partitionBy("year", "month")
      .mode("overwrite")
      .parquet(s"s3a://${bucketName}/${keyPrefix}/${testName}/${date}_start_time_table.parquet")
    println(s"Written: s3a://${bucketName}/${keyPrefix}/${testName}/${date}_start_time_table.parquet")

    // Write Report Table (dimension) as parquet to S3
    targetOoniReportTable.write
      .mode("overwrite")
      .parquet(s"s3a://${bucketName}/${keyPrefix}/${testName}/${date}_report_table.parquet")
    println(s"Written: s3a://${bucketName}/${keyPrefix}/${testName}/${date}_report_table.parquet")

    filteredTargetTestDs.unpersist()
  }

  /**
   *
   * @param bucketName    Source S3 bucket, e.g., "udacity-ooni-project"
   * @param keyPrefix     Source key prefix, e.g., "keys"
   * @param keySuffixRoot Source key suffix root, e.g., "ooni_s3_keys_"
   * @param keySuffixStem Source key suffix stem, e.g., "2020-01"
   * @return
   */
  def getS3KeyLines(
                     bucketName: String,
                     keyPrefix: String,
                     keySuffixRoot: String
                   )(filterString: String,
                     keySuffixStem: String): List[String] = {

    val key = s"$keyPrefix/$keySuffixRoot$keySuffixStem.dat"
    println(s"${filterString} -- s3://${bucketName}/${key}")

    val s3Object: S3Object = s3Client.getObject(bucketName, key)
    val s3ObjectInputStream: S3ObjectInputStream = s3Object.getObjectContent

    try {
      Source.fromInputStream(s3ObjectInputStream)
        .getLines()
        .filter(_.contains(filterString))
        .toList
    }
      // TODO: Insert means to retry reading S3 key with text if something goes sideways
    catch {
      case e: AmazonServiceException =>
        e.printStackTrace()
        List()
      case e: FileNotFoundException =>
        e.printStackTrace()
        System.exit(1)
        List()
      case e: IOException =>
        e.printStackTrace()
        System.exit(1)
        List()
    }
    finally {
      s3ObjectInputStream.close()
      s3Object.close()
    }
  }

  def main(args: Array[String]): Unit = {

    // Read S3 key names from dat files from S3 bucket into one list: s3://udacity-ooni-project/keys/
    val bucketName = "udacity-ooni-project"

    // 1. filter the list of key names to only those of target
    // 2. pass S3 bucket name + list of S3 filtered key names to Spark context to read them all as an RDD
    // 3. For each target test RDD, transform to Dataset

    val testDateKeys = for {
      test <- ooniTargetTestNames
      date <- ooniPrefixDates
    } yield (test, date, getS3KeyLines(bucketName, "keys", "ooni_s3_keys_")(test, date))

    val spark = createSparkSession(awsAccessKeyId, awsSecretAccessKey)

    testDateKeys.par.foreach {
      case (test, date, s3Keys) => etlTargetTestData(spark, bucketName, "parquet/ooni_temp")(test, date, s3Keys)
    }

    spark.stop()
  }
}
