package EtlOoniTestData;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.Serializable;

public class SerializableAmazonS3Client extends AmazonS3Client implements Serializable {

    public SerializableAmazonS3Client(BasicAWSCredentials credentials, Region region) {

        super(credentials);
        super.setRegion(region);
    }
}
