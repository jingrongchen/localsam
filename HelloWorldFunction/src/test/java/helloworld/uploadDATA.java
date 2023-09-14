package helloworld;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class uploadDATA {

    public static void main(String[] args) throws IOException {
        String accessKey = "AKIATDJWUDPA3BLGVHWC";
        String secretKey = "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";
        
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();

        int[] objectSizesMb = {8, 16, 32, 64};  // Object sizes in MB
        String bucketName = "jingrong-lambda-test"; // Replace with your bucket name

        for (int sizeMb : objectSizesMb) {
            String objectKey = sizeMb + "mb_file.txt";
            byte[] testData = generateTestData(sizeMb * 1024 * 1024);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(testData.length);

            InputStream inputStream = new ByteArrayInputStream(testData);

            URI s3Uri = URI.create("s3://" + bucketName + "/" + objectKey);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, inputStream, metadata);


            try {
                s3.putObject(putObjectRequest);
                System.out.println("Uploaded " + objectKey);
            } catch (AmazonServiceException e) {
                System.err.println(e.getMessage());
            }
        }
    }

    private static byte[] generateTestData(int dataSize) {
        byte[] data = new byte[dataSize];
        for (int i = 0; i < dataSize; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }
}
