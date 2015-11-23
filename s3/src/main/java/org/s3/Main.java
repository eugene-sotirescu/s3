package org.s3;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Main {

	// <bucket>/<app>/<env>-<subenv>/<key>
	private final static String BUCKET = "1p-static";
	private final static String ENV = "local";
	private final static String APP = "1p-profile";
	private final static String KEY1 = APP + "/" + ENV + "/catman.jpg";
	private final static String IMAGE_TYPE = "full";
	private final static String base64Image = "iVBORw0KGgoAAAANSUhEUgAAADAAAAAwCAYAAABXAvmHAAACxUlEQVRoge3YUauqQBAA4P7/79LVLbGyDH2IyiArElnTRE3nPhz0arfU1XU9wR0YmPCgze7naGciiiJ8c04EQYBvzokoiiAIQtFRuX53LP9MW9ddp089GZvAf0K8CA1VV3ag/Ad1SXuxwXfg04q/S9qbbPB7gPbibVcdYwyqqsJ8PofpdPodhObzOZxOJwjDELIsq2QURXA+n2G5XDLlxYQQxhgulwtkWQZNkWUZOI4Ds9nsdxBSVbVY8fwLtqnjOAZN09gSok1FUSCKon+4tM0kSWCxWLAl1DYRQnC/3xvJNEUYhiBJ0nCEXo/ln7fbLTWbT/XxeKy9x+rqToREUXw7afpQkiSJHyFN03rTeQ3DMPgROhwOvdm81qfTiR+hfOazTNd1+RG63W7MCXmex4/Q9XplTsh1XX6ELMtiTsi2bX6E1us1c0KmafIjhBAqXiFYEHo+nyDLcjdCTQ28Zn58v98z45OPUKYNND2JJUmCx+PRm04URYAxrlyTpq5toK57Qfj5AZMkSWdCaZrCarWi/tKtGmgilJ9I0zSI45iazfP5hM1m04kNE0LlWlEUcF23WOGm8DwPVFXtvOrMCOW1aZpACKmscB0hQkgxNgdroA0hXdchCILO0ycIAtB1nT8hhBDYtt2aTF3kT2GEEB9CGOMKF9rJ86kmhADGeFhCGGPwfZ/5e1Cevu8XzwTmhBBCQAjpTaYpCCFUnFoTsm272HJWbD7V5TdTJoR0XR+MzadsO50aCSGEIAzDwem8RhiGgBDqT2i321XGJQ9Ceb3b7foRKr/3j5FRFFV2gZqQYRhDS2kMwzC6E3IcBwD+sinXPAhl2c+/4js1IMsypGk6Gp880zQFWZbpCem6PigNmiiP1NY7YFlWcYIxCQEAWJZF34DjOKPzeb0PqAgFQTCMhw4RBAH9DsRxXJxgbEJxHH9s4A+OnBP0TIYJ+wAAAABJRU5ErkJggg==";

	public static void main(String[] args) {
		AmazonS3Client s3Client = null;

		if (args.length == 2) {
			String s3AccessKey = args[0];
			String s3secretKey = args[1];
			BasicAWSCredentials awsCredentials = new BasicAWSCredentials(s3AccessKey, s3secretKey);
			s3Client = new com.amazonaws.services.s3.AmazonS3Client(awsCredentials);
		} else { // IAM roles
			AWSCredentialsProvider provider = new InstanceProfileCredentialsProvider();
			s3Client = new com.amazonaws.services.s3.AmazonS3Client(provider);
		}

/*		listBuckets(s3Client);
		listObjectsFromBucket(s3Client);
		try {
			getObjectFromBucket(s3Client);
		} catch (IOException e) {
			e.printStackTrace();
		}*/

		putObjectIntoBucket(s3Client);
	}

	private static void listBuckets(AmazonS3 s3) {
		System.out.println("Listing all buckets");
		System.out.println("------------------------------");

		for (Bucket bucket : s3.listBuckets()) {
			System.out.println(bucket.getName());
		}
		System.out.println("-----------------------------");

	}

	private static void listObjectsFromBucket(AmazonS3 s3) {

		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(BUCKET));
		System.out.println("Summaries of objects in bucket " + BUCKET);
		System.out.println("------------------------------");
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			System.out.println("key:" + objectSummary.getKey());
			System.out.println("size:" + objectSummary.getSize());
			System.out.println("last modified:" + objectSummary.getLastModified());
			System.out.println("bucket:" + objectSummary.getBucketName());
		}
		System.out.println("-----------------------------");
	}

	private static void getObjectFromBucket(AmazonS3 s3) throws IOException {
		S3Object pulled_object = s3.getObject(BUCKET, KEY1);
		System.out.println("Printing out bucket");
		System.out.println("-------------------------");
		System.out.println(pulled_object.toString());
		System.out.println(pulled_object.getObjectMetadata().getContentType());
		System.out.println("-------------------------");
		// displayTextInputStream(pulled_object.getObjectContent());
	}

	private static void displayTextInputStream(InputStream input) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			System.out.println(line);
		}
		System.out.println();
	}

	private static void putObjectIntoBucket(AmazonS3 s3) {
		// InputStream inStream = IOUtils.toInputStream(base64Image);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("image/jpeg");
		InputStream inputStream = convertBase64ToInputStream(base64Image);
		System.out.println("Starting upload");
		String uuid = UUID.randomUUID().toString();
		String subDir = uuid.substring(0, 5);
		String key = APP + "/" + ENV + "/" + IMAGE_TYPE + "/" + subDir + "/" + uuid + ".jpeg";
		PutObjectRequest putObjectRequest = new PutObjectRequest(BUCKET, key, inputStream, metadata);
		putObjectRequest.setCannedAcl(CannedAccessControlList.PublicRead);
		if (putObjectRequest.getMetadata() == null) {
			putObjectRequest.setMetadata(new ObjectMetadata());
		}
		System.out.println("Uploading file " + key + ", file length " + base64Image.length());
		// putObjectRequest.getMetadata().setContentType("image/jpeg");
		s3.putObject(putObjectRequest);
		System.out.println("Done");
	}

	private static InputStream convertBase64ToInputStream(String base64String) {
		byte[] bytes = Base64.decodeBase64(base64String.getBytes());
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		return byteArrayInputStream;
	}
}
