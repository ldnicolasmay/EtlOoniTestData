package EtlOoniTestData

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Query {

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

  def main(args: Array[String]): Unit = {

    // Logger
    Logger.getLogger(classOf[RackResolver]).getLevel
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Config: Get AWS credentials from aws.conf
    val awsConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/aws.conf"))
    val awsAccessKeyId: String = awsConfig.getString("aws.awsAccessKeyId")
    val awsSecretAccessKey: String = awsConfig.getString("aws.awsSecretAccessKey")

    val spark: SparkSession = SparkSession.builder()
      .appName("UdacityOoniProject")
      .config("fs.s3a.endpoint", "s3.amazonaws.com")
      .config("fs.s3a.access.key", awsAccessKeyId)
      .config("fs.s3a.secret.key", awsSecretAccessKey)
      // .master("local[*]") // comment out when running with spark-submit
      .getOrCreate()

    import spark.implicits._

    /*=== EXTRACT ===*/

    // Country Table (dimension)
    val countryCodeTable: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("s3a://udacity-ooni-project/csv/country_codes.csv")
      .distinct()

    //    countryCodeTable.printSchema()
    //    countryCodeTable.show(10, 50)

    // Autonomous System Table (dimension)
    //    val autonomousSystemTable: DataFrame = spark.read
    //      .option("header", true)
    //      .option("inferSchema", true)
    //      .option("delimiter", ";")
    //      .csv("s3a://udacity-ooni-project/csv/asn.csv")
    //      .distinct()

    //    autonomousSystemTable.printSchema()
    //    autonomousSystemTable.show(10, 50)

    // World Bank Indicator Data - Most populous countries
    val wbTotalPopulation: DataFrame = spark.read
      .parquet("s3a://udacity-ooni-project/parquet/worldbank/SP.POP.TOTL.parquet")

    //    wbTotalPopulation.printSchema()
    //    wbTotalPopulation.show(10, 50)

    val wbTotalPopulation2019DescOrder: DataFrame = wbTotalPopulation
      .filter($"year".equalTo(2019))
      .orderBy(desc("value"))

    val wbTotalPopulation2019DescOrderJoin: DataFrame = wbTotalPopulation2019DescOrder
      .join(countryCodeTable, $"iso_code_3" === $"alpha-3")

    //    wbTotalPopulation2019DescOrderJoin.printSchema()
    //    wbTotalPopulation2019DescOrderJoin.show(10, 50)

    //    wbTotalPopulation2019DescOrder.show(10, 50)

    val tenMostPopulousCountries: List[String] = wbTotalPopulation2019DescOrderJoin
      .select("alpha-2")
      .rdd
      .map(_.getString(0))
      .collect()
      .take(10)
      .toList
    //    println(tenMostPopulousCountries)

    val tenMostPopulousCountriesPlusZZ: List[String] = tenMostPopulousCountries ++ List("ZZ")

    // Test Table (Fact)
    val httpRequestsTestTable: DataFrame = spark.read
      .parquet("s3a://udacity-ooni-project/parquet/ooni/http_requests/test_table/*.parquet")
      .distinct()

    //    httpRequestsTestTable.printSchema()
    //    httpRequestsTestTable.show(10, 50)

    val httpRequestsTestTableFiltered: DataFrame = httpRequestsTestTable
      .filter($"probe_cc".isin(tenMostPopulousCountriesPlusZZ: _*))

//    println("\n\nhttpRequestsTestTableFiltered")
//    httpRequestsTestTableFiltered.show(10, 50)
//    println("\n\n")

    // Start Time Table (dimension)

    // HTTP Requests
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

    //    // Meek Fronted Requests
    //    val meekFrontedRequestsYearsMonths1: List[(String, String)] = List(("2015", "10"), ("2015", "12"))
    //
    //    val meekFrontedRequestsYearsMonths2: List[(String, String)] = for {
    //      year <- List("2016")
    //      month <- List("01", "02", "03", "04", "05", "07", "08", "09", "10", "11", "12")
    //    } yield (year, month)
    //
    //    val meekFrontedRequestsYearsMonths3: List[(String, String)] = for {
    //      year <- List("2017", "2018", "2019")
    //      month <- List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    //    } yield (year, month)
    //
    //    val meekFrontedRequestsYearsMonths4: List[(String, String)] = for {
    //      year <- List("2020")
    //      month <- List("01", "02", "03", "04", "05", "06")
    //    } yield (year, month)
    //
    //    val meekFrontedRequestsYearsMonths: List[(String, String)] =
    //      meekFrontedRequestsYearsMonths1 ++
    //        meekFrontedRequestsYearsMonths2 ++
    //        meekFrontedRequestsYearsMonths3 ++
    //        meekFrontedRequestsYearsMonths4
    //
    //    // Psiphon
    //    val psiphonYearsMonths1: List[(String, String)] = List(("2019", "09"))
    //
    //    val psiphonYearsMonths2: List[(String, String)] = for {
    //      year <- List("2020")
    //      month <- List("01", "02", "03", "04", "05", "06")
    //    } yield (year, month)
    //
    //    val psiphonYearsMonths: List[(String, String)] =
    //      psiphonYearsMonths1 ++
    //        psiphonYearsMonths2
    //
    //    // Tor
    //    val torYearsMonths: List[(String, String)] = for {
    //      year <- List("2020")
    //      month <- List("01", "02", "03", "04", "05", "06")
    //    } yield (year, month)
    //
    //    // Vanilla Tor
    //    val vanillaTorYearsMonths1: List[(String, String)] = for {
    //      year <- List("2016")
    //      month <- List("03", "05", "06", "07", "08", "09", "10", "11", "12")
    //    } yield (year, month)
    //
    //    val vanillaTorYearsMonths2: List[(String, String)] = for {
    //      year <- List("2017", "2018", "2019")
    //      month <- List("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")
    //    } yield (year, month)
    //
    //    val vanillaTorYearsMonths3: List[(String, String)] = for {
    //      year <- List("2020")
    //      month <- List("01", "02", "03", "04", "05", "06")
    //    } yield (year, month)
    //
    //    val vanillaTorYearsMonths: List[(String, String)] =
    //      vanillaTorYearsMonths1 ++
    //        vanillaTorYearsMonths2 ++
    //        vanillaTorYearsMonths3

    // Retrieve DataFrames for defined date ranges
    val httpRequestsStartTimeTables: List[DataFrame] = httpRequestsYearsMonths.map {
      case (year, month) => getYearMonthStartTimeTable(spark, "http_requests", year, month)
    }

    //    val meekFrontedRequestsStartTimeTables: ParSeq[DataFrame] = meekFrontedRequestsYearsMonths.par.map {
    //      case (year, month) => getYearMonthStartTimeTable(spark, "meek_fronted_requests_test", year, month)
    //    }
    //
    //    val psiphonStartTimeTables: ParSeq[DataFrame] = psiphonYearsMonths.par.map {
    //      case (year, month) => getYearMonthStartTimeTable(spark, "psiphon", year, month)
    //    }
    //
    //    val torStartTimeTables: ParSeq[DataFrame] = torYearsMonths.par.map {
    //      case (year, month) => getYearMonthStartTimeTable(spark, "tor", year, month)
    //    }
    //
    //    val vanillaTorStartTimeTables: ParSeq[DataFrame] = vanillaTorYearsMonths.par.map {
    //      case (year, month) => getYearMonthStartTimeTable(spark, "vanilla_tor", year, month)
    //    }

    // Union DataFrames collection into one DataFrame for each test type
    val httpRequestsStartTimeTable: DataFrame = httpRequestsStartTimeTables.reduce(_.union(_))
      .distinct()
    //    val meekFrontedRequestsStartTimeTable: DataFrame = meekFrontedRequestsStartTimeTables.reduce(_.union(_))
    //    val psiphonStartTimeTable: DataFrame = psiphonStartTimeTables.reduce(_.union(_))
    //    val torStartTimeTable: DataFrame = torStartTimeTables.reduce(_.union(_))
    //    val vanillaTorStartTimeTable: DataFrame = vanillaTorStartTimeTables.reduce(_.union(_))

//    println("\n\nhttpRequestsStartTimeTable")
//    httpRequestsStartTimeTable.show(10, 50)
//    println(s"HTTP Requests Start Time Table Count: ${httpRequestsStartTimeTable.count()}")
//    println("\n\n")
    //    println(s"Meek Fronted Requests Start Time Table Count: ${meekFrontedRequestsStartTimeTable.count()}")
    //    println(s"Psiphon Start Time Table Count: ${psiphonStartTimeTable.count()}")
    //    println(s"Tor Start Time Table Count: ${torStartTimeTable.count()}")
    //    println(s"Vanilla Tor Start Time Table Count: ${vanillaTorStartTimeTable.count()}")


    // Report Table (dimension)
    val httpRequestsReportTable: DataFrame = spark.read
      .parquet("s3a://udacity-ooni-project/parquet/ooni/http_requests/report_table/*.parquet")
      .distinct()

    //        report_table.printSchema()
    //        report_table.show(10, 50)

    val httpRequestsTestStartTimeTable: DataFrame = httpRequestsTestTableFiltered
      .join(httpRequestsStartTimeTable.select($"start_time", $"year"))
      .where($"measurement_start_time" === $"start_time")

    httpRequestsTestStartTimeTable
      .groupBy($"probe_cc", $"year")
      .count()
      .orderBy($"probe_cc", $"year")
      .show(100, 10)

  }
}