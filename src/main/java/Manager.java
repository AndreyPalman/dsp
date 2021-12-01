import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Manager {
    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public boolean shouldTerminate = false;
    public int numberOfMessagesPerWorker = 100;
    public int activeWorkers = 0;
    public void StartManager() {

        String queueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);

        while(!shouldTerminate) {
            List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
            for (Message message : messages) {
                // If the message is that of a new task it
                String messageType = message.getBody().split(AwsBundle.Delimiter)[0];
                if (messageType.equals("NewTask")) {
                    // Downloads the input file from S3.
                    String fileUrlInS3 = message.getBody().split(AwsBundle.Delimiter)[1].split("//")[1];
                    String bucketName = fileUrlInS3.split("/")[0];
                    String fileName = fileUrlInS3.split("/")[1];
                    InputStream downloadedFileStream = awsBundle.downloadFileFromS3(bucketName, fileName);
                    try {
                        Files.copy(downloadedFileStream, Paths.get(fileName));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    // Creates an SQS message for each URL in the input file together with the operation
                    // that should be performed on it
                    createSqsMessagesForEachUrl(fileName);
                    // Checks the SQS message count and starts Worker processes (nodes) accordingly.
                    // The manager should create a worker for every n messages, if there are no running workers.
                    // If there are k active workers, and the new job requires m workers, then the
                    // manager should create m-k new workers, if possible
                    startWorkersAccordinglyToSqsMessageCount();
                } else if (messageType.equals("Terminate")) {
                    // If the message is a termination message, then the manager:
                    // Does not accept any more input files from local applications.
                    shouldTerminate = true;
                    // Waits for all the workers to finish their job, and then terminates them.
                    // Creates response messages for the jobs, if needed.
                    // Terminates
                }
            }
        }
    }

    private void startWorkersAccordinglyToSqsMessageCount() {
        List<Message> messages = awsBundle.fetchNewMessages(awsBundle.managerAndWorkerQueueName);
        int numberOfMessages = messages.size();
        int numberOfWorkersNeeded = Math.min(AwsBundle.MAX_INSTANCES, numberOfMessages / numberOfMessagesPerWorker);
        if (numberOfWorkersNeeded > activeWorkers) {
            for (int i = 0; i < numberOfWorkersNeeded - activeWorkers; i++) {
                // Start Worker
            }
        }

    }

    public void createSqsMessagesForEachUrl(String fileName){
        File inputFile = new File(fileName);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line = "";
            while (true) {
                try {
                    if ((line = br.readLine()) == null) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String[] urlAndOperation = line.split(" ");
                String operation = urlAndOperation[0];
                String url = urlAndOperation[1];
                awsBundle.sendMessage(
                        awsBundle.managerAndWorkerQueueName,
                        awsBundle.createMessage("PdfTask", operation + AwsBundle.Delimiter + url));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
