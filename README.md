1. **Local Terminal**: Package Spark app

   ```shell script
   sbt package
   ```

2. **Remote Terminal**: Login to Amazon EMR cluster; Prepare home folder

   ```shell script
   EMR_URL="ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com" && \
     EMR_PEM="/home/hynso/aws_pems/spark-cluster-emr-us-east-1.pem"
   ```

   ```shell script
   ssh -i "${EMR_PEM}" "hadoop@${EMR_URL}"
   ```
   
   ```shell script
   mkdir EtlOoniTestData && cd EtlOoniTestData
   ```

3. **Local Terminal**: Sync app project to remote cluster

   ```shell script
   EMR_URL="ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com" && \
     EMR_PEM="/home/hynso/aws_pems/spark-cluster-emr-us-east-1.pem"
   ```

   ```shell script
   rsync -au -e "ssh -i ${EMR_PEM}" \
     /home/hynso/Documents/Learning/DataEngineering/EtlOoniTestData/ \
     "hadoop@${EMR_URL}:/home/hadoop/EtlOoniTestData/"
   ```

4. **Remote Terminal**: 

   ```shell script
   spark-submit \
     --class EtlOoniTestData.OoniEtl \
     --master yarn \
     --jars lib/aws-java-sdk-1.7.4.jar,lib/config-1.3.0.jar,lib/geny_2.11-0.1.6.jar,lib/os-lib_2.11-0.2.9.jar \
     target/scala-2.11/etloonitestdata_2.11-0.1.jar 
   ```