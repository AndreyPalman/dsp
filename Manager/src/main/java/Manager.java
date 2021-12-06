import com.amazonaws.services.sqs.model.Message;
import org.javatuples.Triplet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class Manager {

    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public static ConcurrentHashMap<String, Integer> LocalRequests = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, List<Triplet<String, String, String>>> LocalDoneTasks = new ConcurrentHashMap<>();


    public static boolean shouldTerminate = false;
    public static int numberOfMessagesPerWorker = 100;
    public static int activeWorkers = 1;

    public static void main(String[] args){
        try {
            numberOfMessagesPerWorker = Integer.parseInt(args[0]);
        }catch (Exception e){
            numberOfMessagesPerWorker = 30;
        }

        String ManagerAndWorkerQueueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);
        String LocalAndManagerQueueUrl = awsBundle.getQueueUrl(awsBundle.localAndManagerQueueName);
        while(!shouldTerminate) {
            List<Message> messages = awsBundle.fetchNewMessages(ManagerAndWorkerQueueUrl);
            try {
                messages.addAll(awsBundle.fetchNewMessages(LocalAndManagerQueueUrl));
            } catch (Exception ignored) {}
            for (Message message : messages) {
                // If the message is that of a new task it
                String messageType = message.getBody().split(AwsBundle.Delimiter)[0];
                switch (messageType) {
                    case "NewTask":
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                // Downloads the input file from S3.
                                String localAppName = message.getBody().split(AwsBundle.Delimiter)[1];
                                // check if the localAppName is already in the map
                                LocalDoneTasks.computeIfAbsent(localAppName, k -> new LinkedList<>());
                                String fileUrlInS3 = message.getBody().split(AwsBundle.Delimiter)[2].split("//")[1];
                                String bucketName = fileUrlInS3.split("/")[0];
                                String fileName = fileUrlInS3.split("/")[1];
                                InputStream downloadedFileStream = awsBundle.downloadFileFromS3(bucketName, fileName);
                                try {
                                    Files.copy(downloadedFileStream, Paths.get(fileName), StandardCopyOption.REPLACE_EXISTING);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                // Creates an SQS message for each URL in the input file together with the operation
                                // that should be performed on it
                                createSqsMessagesForEachUrl(fileName, localAppName);
                                // Checks the SQS message count and starts Worker processes (nodes) accordingly.
                                // The manager should create a worker for every n messages, if there are no running workers.
                                // If there are k active workers, and the new job requires m workers, then the
                                // manager should create m-k new workers, if possible
                                startWorkersAccordinglyToSqsMessageCount();
                                awsBundle.deleteMessageFromQueue(LocalAndManagerQueueUrl, message);
                            }
                        }).start();
                        break;
                    case "Terminate":
                        // If the message is a termination message, then the manager:
                        // Does not accept any more input files from local applications.
                        shouldTerminate = true;
                        // Waits for all the workers to finish their job, and then terminates them.
                        // Creates response messages for the jobs, if needed.
                        // Terminates
                        awsBundle.deleteMessageFromQueue(LocalAndManagerQueueUrl, message);
                        break;
                    case "DonePdfTask":

                        String localAppNameToSendDone = message.getBody().split(AwsBundle.Delimiter)[1];
                        String s3FileUrl = message.getBody().split(AwsBundle.Delimiter)[2];
                        String operation = message.getBody().split(AwsBundle.Delimiter)[3];
                        String OriginalFileName = message.getBody().split(AwsBundle.Delimiter)[4];

                        List<Triplet<String, String, String>> allTasks = LocalDoneTasks.get(localAppNameToSendDone);
                        allTasks.add(new Triplet<>(s3FileUrl, operation, OriginalFileName));

                        awsBundle.deleteMessageFromQueue(ManagerAndWorkerQueueUrl, message);

                        LocalRequests.replace(localAppNameToSendDone, LocalRequests.get(localAppNameToSendDone) - 1);
                        if (LocalRequests.get(localAppNameToSendDone) == 0) {
                            createSummaryReport(localAppNameToSendDone);
                        }
                        break;
                }
            }
        }
    }

    private static void createSummaryReport(String localAppName){
        String text = "";
        List<Triplet<String,String,String>> doneTasks = LocalDoneTasks.get(localAppName);
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("./" +"summery" + ".txt", "UTF-8");
            for (Triplet<String,String,String> task : doneTasks) {
                String s3FileUrl = task.getValue0();
                String operation = task.getValue1();
                String originalFileName = task.getValue2();
                text = s3FileUrl + AwsBundle.Delimiter + operation + AwsBundle.Delimiter + originalFileName;
                writer.println(text);
            }
            writer.close();
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        try {

            File f = new File("./" +"summery" + ".txt");
            awsBundle.uploadFileToS3(AwsBundle.bucketName,"summery.txt",f);
            try {
                boolean res = f.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        awsBundle.sendMessage(
                awsBundle.localAndManagerQueueName + localAppName,
                awsBundle.createMessage("DoneTask","summery.txt"));
    }

    private static void startWorkersAccordinglyToSqsMessageCount() {
        List<Message> messages = awsBundle.fetchNewMessages(awsBundle.managerAndWorkerQueueName);
        int numberOfMessages = messages.size();
        int numberOfWorkersNeeded = Math.min(AwsBundle.MAX_INSTANCES, Math.max(1,numberOfMessages / numberOfMessagesPerWorker));
        if (numberOfWorkersNeeded > activeWorkers) {
            for (int i = 0; i < numberOfWorkersNeeded - activeWorkers; i++) {
                // Start Worker
                if(activeWorkers < AwsBundle.MAX_INSTANCES - 1) {
                    //createWorker(activeWorkers);
                    activeWorkers++;
                }
            }
        }
    }

    private static void createWorker(int activeWorkers){
        String managerScript = String.format("#! /bin/bash\n" +
                "sudo yum update -y\n" +
                "mkdir WorkerFile\n" +
                "aws s3 cp s3://%s/Worker.jar ./WorkerFile\n" +
                "java -jar /WorkerFile/Worker.jar\n", AwsBundle.bucketName);

        awsBundle.createInstance(String.format("Worker%d", activeWorkers),AwsBundle.ami,managerScript);
    }

    public static void createSqsMessagesForEachUrl(String fileName, String localAppName){
        File inputFile = new File(fileName);
        int numberOfTasks = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line = "";
            while (true) {
                try {
                    if ((line = br.readLine()) == null) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String[] urlAndOperation = line.split("\t");
                String operation = urlAndOperation[0];
                String url = urlAndOperation[1];
                awsBundle.sendMessage(
                        awsBundle.managerAndWorkerQueueName,
                        awsBundle.createMessage("PdfTask", operation + AwsBundle.Delimiter + url + AwsBundle.Delimiter + localAppName));

                numberOfTasks++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            boolean res = inputFile.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

        LocalRequests.put(localAppName, numberOfTasks);
    }
}
