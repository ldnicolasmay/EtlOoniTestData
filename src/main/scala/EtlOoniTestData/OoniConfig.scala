package EtlOoniTestData

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

object OoniConfig {

  // Config: Get OONI S3 bucket and prefix from ooni.conf
  val ooniConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/ooni.conf"))

  @transient lazy val ooniBucketName: String = ooniConfig.getString("ooni.bucketName")
  @transient lazy val ooniPrefixRoot: String = ooniConfig.getString("ooni.prefixRoot")
  @transient lazy val ooniPrefixDates: List[String] = ooniConfig.getStringList("ooni.prefixStems").toList
  @transient lazy val ooniTargetTestNames: List[String] = ooniConfig.getStringList("ooni.targetTestNames").toList
}