import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.util.EC2MetadataUtils;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AwsBundle {
    public static final int MAX_INSTANCES = 19;
    public static final long maxFileSize = 10000000;
    private final AmazonEC2 ec2;
    private final AmazonS3 s3;
    private final AmazonSQS sqs;


    public static String localAndManagerQueueName = "localToManagerQueueName";

    public static final String bucketName = "dspassignment1andreypalmans3bucket";

    public static final String ami = "ami-00e95a9222311e8ed";

    //message from local
    static final String Delimiter = "__";


    private static final AwsBundle instance = new AwsBundle();

    private AwsBundle() {
        ec2 = AmazonEC2ClientBuilder.defaultClient();
        s3 = AmazonS3ClientBuilder.defaultClient();
        sqs = AmazonSQSClientBuilder.defaultClient();
    }

    public static AwsBundle getInstance() {
        return instance;
    }

    public boolean checkIfInstanceExist(String name) {
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest();
        DescribeInstancesResult describeInstancesResult = this.ec2.describeInstances(describeInstancesRequest);
        for (Reservation r : describeInstancesResult.getReservations()) {
            for (Instance i : r.getInstances()) {
                if (!i.getState().getName().equals("running"))
                    continue;
                for (Tag t : i.getTags()) {
                    if (t.getKey().equals("Name") && t.getValue().equals(name)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public String getMangerStatus() {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult response = this.ec2.describeInstances(request);
        Tag tagName = new Tag("Name", "Manager");
        for (Reservation reservation : response.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                if (!instance.getState().getName().equals("running"))
                    continue;
                List<Tag> tags = instance.getTags();
                if (tags.contains(tagName)) {
                    for (Tag t : tags) {
                        if (t.getKey().equals("Status")) {
                            return t.getValue();
                        }
                    }
                }
            }
        }
        return null;
    }

    //region S3

    public void createBucketIfNotExists(String bucketName) {
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            if (!s3Client.doesBucketExistV2(bucketName)) {
                // Because the CreateBucketRequest object doesn't specify a region, the
                // bucket is created in the region specified in the client.
                s3Client.createBucket(new CreateBucketRequest(bucketName));
            }
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void uploadFileToS3(String bucketName, String keyName, File fileToUpload) {
        try {
            this.s3.putObject(bucketName, keyName, fileToUpload);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public List<String> getAllFilesFromS3(String bucketName) {
        List<String> files = new ArrayList<>();
        ObjectListing objectListing = s3.listObjects(bucketName);
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            files.add(objectSummary.getKey());
        }
        return files;
    }

    public InputStream downloadFileFromS3(String bucketName, String keyName) {
        try {
            return this.s3.getObject(new GetObjectRequest(bucketName, keyName)).getObjectContent();
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    public void deleteFileFromS3(String bucketName, String key) {
        this.s3.deleteObject(bucketName, key);
    }

    //endregion

    //region SQS

    public void sendMessage(String queueUrl, String msg) {
        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody(msg);
        this.sqs.sendMessage(send_msg_request);
    }

    public String createMsgQueue(String queueName) {
        CreateQueueRequest create_request = new CreateQueueRequest(queueName);
        try {
            this.sqs.createQueue(create_request);
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        return this.sqs.getQueueUrl(queueName).getQueueUrl();
    }

    public void deleteQueue(String queueName) {
        try {
            this.sqs.deleteQueue(new DeleteQueueRequest(queueName));
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    // get all sqs from the account
    public List<String> getAllSQS() {
        return new LinkedList<>(sqs.listQueues().getQueueUrls());
    }

    public String getQueueUrl(String queueUrl) {
        while (true) {
            try {
                return this.sqs.getQueueUrl(queueUrl).getQueueUrl();
            } catch (QueueDoesNotExistException e) {
                continue;
            }
        }
    }

    public void cleanQueue(String queueName) {
        PurgeQueueRequest request = new PurgeQueueRequest(queueName);
        this.sqs.purgeQueue(request);
    }

    public List<Message> fetchNewMessages(String queueUrl) {
        ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withVisibilityTimeout(0);

        return this.sqs.receiveMessage(receiveRequest).getMessages();
    }

    public void deleteMessageFromQueue(String queueUrl, Message message) {
        this.sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
    }

    public String createMessage(String type, String filePath){
        return type + AwsBundle.Delimiter + filePath;
    }

    //endregion

    public void createInstance(String name, String imageId, String userDataScript) {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        DescribeInstancesResult ec2Response = this.ec2.describeInstances(request);
        int numInstances = ec2Response.getReservations().size();
        if (numInstances >= MAX_INSTANCES)
            return;

        IamInstanceProfileSpecification role = new IamInstanceProfileSpecification().withName("LabInstanceProfile");
        RunInstancesRequest run_request = new RunInstancesRequest()
                .withImageId(imageId)
                .withKeyName("vockey")
                .withIamInstanceProfile(role)
                .withInstanceType(InstanceType.M4Large)
                .withSecurityGroupIds("sg-02541f450554a7c98")
                .withUserData(Base64.getEncoder().encodeToString(userDataScript.getBytes(UTF_8)))
                .withMaxCount(1)
                .withMinCount(1);

        RunInstancesResult run_response = this.ec2.runInstances(run_request);
        String reservation_id = run_response.getReservation().getInstances().get(0).getInstanceId();
        Tag tagName = new Tag()
                .withKey("Name")
                .withValue(name);
        Tag tagStatus = new Tag()
                .withKey("Status")
                .withValue("Active");
        CreateTagsRequest tag_request = new CreateTagsRequest()
                .withResources(reservation_id)
                .withTags(tagName, tagStatus);
        CreateTagsResult tag_response = this.ec2.createTags(tag_request);
        System.out.printf(
                "Successfully started EC2 instance %s based on AMI %s",
                reservation_id, "yourAMI");
    }

    public void terminateCurrentInstance() {
        String instanceId = EC2MetadataUtils.getInstanceId();
        List<String> instanceIds = new ArrayList<>();
        instanceIds.add(instanceId);
        TerminateInstancesRequest request = new TerminateInstancesRequest(instanceIds);
        this.ec2.terminateInstances(request);
    }

    public final AmazonSQS getSqs() {
        return this.sqs;
    }

    public final AmazonEC2 getEc2() {
        return this.ec2;
    }
}
