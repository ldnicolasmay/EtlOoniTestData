package EtlOoniTestData

import java.io.{FileNotFoundException, IOException}

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
  def etlTargetTestData(
                         spark: SparkSession,
                         s3Keys: List[String],
                         bucketName: String,
                         keyPrefix: String
                       )(testName: String): Unit = {
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext

    // Filter incoming OONI S3 keys for target test name
    val targetTestS3Keys: List[String] = s3Keys.filter(_.contains(testName))

    // Using OONI S3 keys, read data within the JSONL files at those S3 keys to a Spark RDD
    val targetTestRdd: RDD[String] = sc.parallelize(targetTestS3Keys)
      .mapPartitions { it =>
        it.flatMap { key =>
          Source.fromInputStream(s3Client.getObject(ooniBucketName, key).getObjectContent).getLines
        }
      }

    // Convert Spark RDD to Spark DataSet
    val targetTestDs = spark.sqlContext.read
      .schema(ooniSchemas(testName))
      .json(targetTestRdd.toDS)
      .as(ooniEncoders(testName))

    // Filter DataSet rows to ensure only target test data remain
    val filteredTargetTestDs = targetTestDs
      .filter(col("test_name").equalTo(testName))
      .persist()
    //println(s"$testName Dataset Row Count: ${filteredTargetTestDs.count()}")


    // Create fact table: Test Table
    val targetOoniTestTable = filteredTargetTestDs
      .select("id",
        "measurement_start_time",
        "test_start_time",
        "probe_cc",
        "probe_asn",
        "report_id")
      .dropDuplicates()
    println()
    println(s"$testName Test Table Row Count: ${targetOoniTestTable.count()}")

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
    println(s"$testName Start Time Table Row Count: ${targetOoniStartTimeTable.count()}")

    // Create dimension table: Report Table
    val targetOoniReportTable = filteredTargetTestDs
      .select("report_id", "report_filename")
      .dropDuplicates()
    println(s"$testName Report Table Row Count: ${targetOoniReportTable.count()}")


    // Write Test Table (fact) as parquet to target S3
    targetOoniTestTable.write
      .mode("overwrite")
      .parquet(s"s3a://${bucketName}/${keyPrefix}/${testName}_test_table.parquet")
    println(s"Written: s3a://${bucketName}/${keyPrefix}/${testName}_test_table.parquet")

    // Write Start Time Table (dimension) as parquet to S3
    targetOoniStartTimeTable.write
      .mode("overwrite")
      .parquet(s"s3a://${bucketName}/${keyPrefix}/${testName}_start_time_table.parquet")
    println(s"Written: s3a://${bucketName}/${keyPrefix}/${testName}_start_time_table.parquet")

    // Write Report Table (dimension) as parquet to S3
    targetOoniReportTable.write
      .mode("overwrite")
      .parquet(s"s3a://${bucketName}/${keyPrefix}/${testName}_report_table.parquet")
    println(s"Written: s3a://${bucketName}/${keyPrefix}/${testName}_report_table.parquet")

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
                   )(keySuffixStem: String): List[String] = {
    val key = s"$keyPrefix/$keySuffixRoot$keySuffixStem.dat"

    val s3Obj: S3Object = s3Client.getObject(bucketName, key)
    val s3ObjIS: S3ObjectInputStream = s3Obj.getObjectContent

    try {
      scala.io.Source.fromInputStream(s3ObjIS).getLines().toList
    }
      // TODO: Insert means to retry reading S3 key with text if something goes sideways
    catch {
      case e: AmazonServiceException =>
        e.printStackTrace()
        List("")
      case e: FileNotFoundException =>
        e.printStackTrace()
        System.exit(1)
        List("")
      case e: IOException =>
        e.printStackTrace()
        System.exit(1)
        List("")
    }
    finally {
      s3ObjIS.close()
      s3Obj.close()
    }
  }

  def main(args: Array[String]): Unit = {

    // Read S3 key names from dat files from S3 bucket into one list: s3://udacity-ooni-project/keys/
    val bucketName = "udacity-ooni-project"
    val s3Keys: List[String] = ooniPrefixDates.flatMap { date =>
      getS3KeyLines(bucketName, "filtered_ooni_s3_keys", "ooni_s3_keys_")(date)
    }

    // For each target test name,
    //   1. filter the list of key names to only those of target
    //   2. pass S3 bucket name + list of S3 filtered key names to Spark context to read them all as an RDD
    //   3. For each target test RDD, transform to Dataset
    val spark = createSparkSession(awsAccessKeyId, awsSecretAccessKey)
    ooniTargetTestNames.par.foreach { testName =>
      etlTargetTestData(spark, s3Keys, bucketName, "parquet/ooni_temp")(testName)
    }
    spark.stop()

  }
}
