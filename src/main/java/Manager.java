import com.amazonaws.services.sqs.model.Message;
import org.javatuples.Triplet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class Manager extends Thread  {


    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public HashMap<String, Integer> LocalRequests = new HashMap<>();
    public HashMap<String, String> LocalQueues = new HashMap<>();
    public HashMap<String, List<Triplet<String, String, String>>> LocalDoneTasks = new HashMap<>();


    public boolean shouldTerminate = false;
    public int numberOfMessagesPerWorker = 100;
    public int activeWorkers = 0;
    public Worker worker = new Worker();

    public void run() {


        String ManagerAndWorkerQueueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);
        String LocalAndManagerQueueUrl = awsBundle.getQueueUrl(awsBundle.localAndManagerQueueName);
        String localAppName = "";
        while(!shouldTerminate) {
            List<Message> messages = awsBundle.fetchNewMessages(ManagerAndWorkerQueueUrl);
            try {
                messages.addAll(awsBundle.fetchNewMessages(LocalAndManagerQueueUrl));
            } catch (Exception ignored) {}
            for (Message message : messages) {
                // If the message is that of a new task it
                String messageType = message.getBody().split(AwsBundle.Delimiter)[0];
                if (messageType.equals("NewTask")) {
                    // Downloads the input file from S3.
                    localAppName = message.getBody().split(AwsBundle.Delimiter)[1];
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
                    createSqsMessagesForEachUrl(fileName,localAppName);
                    // Checks the SQS message count and starts Worker processes (nodes) accordingly.
                    // The manager should create a worker for every n messages, if there are no running workers.
                    // If there are k active workers, and the new job requires m workers, then the
                    // manager should create m-k new workers, if possible
                    startWorkersAccordinglyToSqsMessageCount();
                    awsBundle.deleteMessageFromQueue(LocalAndManagerQueueUrl, message);
                } else if (messageType.equals("Terminate")) {
                    // If the message is a termination message, then the manager:
                    // Does not accept any more input files from local applications.
                    shouldTerminate = true;
                    // Waits for all the workers to finish their job, and then terminates them.
                    // Creates response messages for the jobs, if needed.
                    // Terminates
                    awsBundle.deleteMessageFromQueue(LocalAndManagerQueueUrl, message);
                } else if(messageType.equals("DonePdfTask")){

                    String s3FileUrl = message.getBody().split(AwsBundle.Delimiter)[1];
                    String operation = message.getBody().split(AwsBundle.Delimiter)[2];
                    String OriginalFileName = message.getBody().split(AwsBundle.Delimiter)[3];

                    List<Triplet<String, String, String>> allTasks = LocalDoneTasks.get(localAppName);
                    allTasks.add(new Triplet<>(s3FileUrl, operation, OriginalFileName));

                    awsBundle.deleteMessageFromQueue(ManagerAndWorkerQueueUrl, message);

                    LocalRequests.replace(localAppName, LocalRequests.get(localAppName) - 1);
                    if(LocalRequests.get(localAppName) == 0) {
                        worker.shouldTerminate = true;
                        createSummaryReport(localAppName);
                    }
                }
            }
        }
    }


    private void createSummaryReport(String localAppName){
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
                awsBundle.localAndManagerQueueName,
                awsBundle.createMessage("DoneTask","summery.txt"));
    }

    private void startWorkersAccordinglyToSqsMessageCount() {
        List<Message> messages = awsBundle.fetchNewMessages(awsBundle.managerAndWorkerQueueName);
        int numberOfMessages = messages.size();
        int numberOfWorkersNeeded = Math.min(AwsBundle.MAX_INSTANCES, Math.max(1,numberOfMessages / numberOfMessagesPerWorker));
        if (numberOfWorkersNeeded > activeWorkers) {
            for (int i = 0; i < numberOfWorkersNeeded - activeWorkers; i++) {
                // Start Worker
                worker.start();
            }
        }

    }

    public void createSqsMessagesForEachUrl(String fileName, String localAppName){
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
                        awsBundle.createMessage("PdfTask", operation + AwsBundle.Delimiter + url));

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
