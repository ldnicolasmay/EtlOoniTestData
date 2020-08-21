package EtlOoniTestData

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

object AwsConfig {

  // Get AWS credentials from src/main/resources/config/aws.conf
  val awsConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/aws.conf"))
  val awsAccessKeyId: String = awsConfig.getString("aws.awsAccessKeyId")
  val awsSecretAccessKey: String = awsConfig.getString("aws.awsSecretAccessKey")

}
