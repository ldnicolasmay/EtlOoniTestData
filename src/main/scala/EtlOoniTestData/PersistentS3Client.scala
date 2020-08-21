package EtlOoniTestData

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.config.{Config, ConfigFactory}

object PersistentS3Client {

  // Get AWS credentials from aws.conf
  private val awsConfig: Config = ConfigFactory.parseFile(new File("src/main/resources/config/aws.conf"))
  private val awsAccessKeyId: String = awsConfig.getString("aws.awsAccessKeyId")
  private val awsSecretAccessKey: String = awsConfig.getString("aws.awsSecretAccessKey")

  // Define values for S3 client
  val usEast1: Region = Region.getRegion(Regions.US_EAST_1)
  private val awsCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey)

  // Build S3 client
  val s3Client = new AmazonS3Client(awsCredentials)
  s3Client.setRegion(usEast1)

}
