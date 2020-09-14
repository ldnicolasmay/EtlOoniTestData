import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


package object EtlOoniTestData {

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
      .master("local[*]") // comment out when running with spark-submit
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

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

  val httpRequestsSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("body_length_match", BooleanType, nullable = true),
      StructField("body_proportion", DoubleType, nullable = true),
      StructField("factor", DoubleType, nullable = true)
    )), nullable = true)
  ))

  val meekFrontedRequestsTestSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("success", BooleanType, nullable = true)
    )), nullable = true)
  ))

  val psiphonSchemaExtension: StructType = StructType(List(
    StructField("test_keys", StructType(List(
      StructField("failure", StringType, nullable = true)
    )), nullable = true)
  ))

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

  val ooniSchemaExtensions: Map[String, StructType] = Map(
    "http_requests" -> httpRequestsSchemaExtension,
    "meek_fronted_requests_test" -> meekFrontedRequestsTestSchemaExtension,
    "psiphon" -> psiphonSchemaExtension,
    "tor" -> torSchemaExtension,
    "vanilla_tor" -> vanillaTorSchemaExtension
  )

  val ooniSchemas: Map[String, StructType] = for {
    (targetTestName, targetTestSchemaExtension) <- ooniSchemaExtensions
  } yield (targetTestName, StructType(baseSchema ++ targetTestSchemaExtension))

  val ooniEncoders: Map[String, ExpressionEncoder[Row]] = for {
    (targetTestName, targetTestSchema) <- ooniSchemas
  } yield (targetTestName, RowEncoder(targetTestSchema))

}
