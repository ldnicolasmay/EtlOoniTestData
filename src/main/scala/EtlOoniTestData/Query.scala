package EtlOoniTestData

import EtlOoniTestData.AwsConfig.{awsAccessKeyId, awsSecretAccessKey}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Query {

  /**
   * Get OONI Start Time Table for a given test, year, month
   *
   * @param spark    Spark session
   * @param testName OONI test name, e.g., "http_requests"
   * @param year     Year, e.g., "2019", "2020"
   * @param month    Month, e.g., "01", "02"
   * @return Spark DataFrame
   */
  def getYearMonthStartTimeTable(
                                  spark: SparkSession,
                                  testName: String,
                                  year: String,
                                  month: String): DataFrame = {
    val bucket: String = "s3a://udacity-ooni-project"
    val keyPrefix: String = "/parquet/ooni"

    spark.read
      .option("basePath", s"${bucket}${keyPrefix}/${testName}/start_time_table/")
      .parquet(s"${bucket}${keyPrefix}/${testName}/start_time_table/${year}-${month}.parquet/year=*/month=*")
  }

  /**
   * Driver method for Query Spark app
   *
   * @param args Arguments passed to main method
   */
  def main(args: Array[String]): Unit = {

    // Logger
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder()
      .appName("UdacityOoniProject")
      .config("fs.s3a.endpoint", "s3.amazonaws.com")
      .config("fs.s3a.access.key", awsAccessKeyId)
      .config("fs.s3a.secret.key", awsSecretAccessKey)
      // .master("local[*]") // comment out when running with spark-submit
      .getOrCreate()

    import spark.implicits._


    /*=== EXTRACT ===*/

    // World Bank Indicator Data - Total Population by Country and Year
    val wbTotalPopulation: DataFrame = spark.read
      .parquet("s3a://udacity-ooni-project/parquet/worldbank/SP.POP.TOTL.parquet")

    // Retrieve 10 most populous countries in 2019
    val wbTotalPopulation2019DescOrder: DataFrame = wbTotalPopulation
      .filter($"year".equalTo(2019))
      .orderBy(desc("value"))

    // Country Table (dimension)
    val countryCodeTable: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("s3a://udacity-ooni-project/csv/country_codes.csv")
      .distinct()

    val wbTotalPopulation2019DescOrderJoin: DataFrame = wbTotalPopulation2019DescOrder
      .join(countryCodeTable, $"iso_code_3" === $"alpha-3")

    val tenMostPopulousCountries: List[String] = wbTotalPopulation2019DescOrderJoin
      .select("alpha-2")
      .rdd
      .map(_.getString(0))
      .collect()
      .take(10)
      .toList

    // Add unknown country of origin "ZZ" to list
    val tenMostPopulousCountriesPlusZZ: List[String] = tenMostPopulousCountries ++ List("ZZ")

    // OONI HTTP Requests Test Table (Fact)
    val httpRequestsTestTable: DataFrame = spark.read
      .parquet("s3a://udacity-ooni-project/parquet/ooni/http_requests/test_table/*.parquet")
      .distinct()

    // OONI HTTP Requests Start Time Table (dimension)
    // Define months with OONI HTTP Requests data
    val httpRequestsYearsMonths1: List[(String, String)] = for {
      year <- List("2013")
      month <- List("01", "05", "09", "10", "11", "12")
    } yield (year, month)

    val httpRequestsYearsMonths2: List[(String, String)] = for {
      year <- List("2014", "2015", "2016", "2017", "2018", "2019")
      month <- List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    } yield (year, month)

    val httpRequestsYearsMonths3: List[(String, String)] = for {
      year <- List("2020")
      month <- List("01", "02", "03", "04", "05", "06")
    } yield (year, month)

    val httpRequestsYearsMonths: List[(String, String)] =
      httpRequestsYearsMonths1 ++
        httpRequestsYearsMonths2 ++
        httpRequestsYearsMonths3

    // Retrieve DataFrames for defined date ranges
    val httpRequestsStartTimeTables: List[DataFrame] = httpRequestsYearsMonths.map {
      case (year, month) => getYearMonthStartTimeTable(spark, "http_requests", year, month)
    }

    // Union DataFrames collection into one DataFrame for each test type
    val httpRequestsStartTimeTable: DataFrame = httpRequestsStartTimeTables.reduce(_.union(_))
      .distinct()


    /*=== TRANSFORM ===*/

    val httpRequestsTestTableFiltered: DataFrame = httpRequestsTestTable
      .filter($"probe_cc".isin(tenMostPopulousCountriesPlusZZ: _*))

    val httpRequestsTestStartTimeTable: DataFrame = httpRequestsTestTableFiltered
      .join(httpRequestsStartTimeTable.select($"start_time", $"year"))
      .where($"measurement_start_time" === $"start_time")


    /*=== LOAD ===*/

    val querySchema: StructType = StructType(List(
      StructField("probe_cc", StringType, nullable = true),
      StructField("year", IntegerType, nullable = true),
      StructField("count", IntegerType, nullable = true)
    ))

    val queryResult: Dataset[Row] = httpRequestsTestStartTimeTable
      .groupBy($"probe_cc", $"year")
      .count()
      .orderBy($"probe_cc", $"year")

    writeTableToS3Csv(
      queryResult,
      "udacity-ooni-project",
      "csv/query/population_http_requests.csv",
      "overwrite",
      0)

  }
}