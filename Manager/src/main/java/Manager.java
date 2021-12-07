import com.amazonaws.services.sqs.model.Message;
import org.javatuples.Triplet;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Manager {

    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public static ConcurrentHashMap<String, Integer> LocalRequests = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, List<Triplet<String, String, String>>> LocalDoneTasks = new ConcurrentHashMap<>();


    public static AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    public static int numberOfMessagesPerWorker = 10;
    public static AtomicInteger activeWorkers = new AtomicInteger(1);

    public static void main(String[] args){
        try {
            numberOfMessagesPerWorker = Integer.parseInt(args[0]);
        }catch (Exception e){
            numberOfMessagesPerWorker = 30;
        }

        String ManagerAndWorkerQueueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);
        String LocalAndManagerQueueUrl = awsBundle.getQueueUrl(awsBundle.localAndManagerQueueName);
        while(!shouldTerminate.get()) {
            try {
                List<Message> messages = awsBundle.fetchNewMessages(ManagerAndWorkerQueueUrl);
                try {
                    messages.addAll(awsBundle.fetchNewMessages(LocalAndManagerQueueUrl));

                    for (Message message : messages) {
                        // If the message is that of a new task it
                        String messageType = message.getBody().split(AwsBundle.Delimiter)[0];
                        switch (messageType) {
                            case "NewTask":
                                if (!shouldTerminate.get()) {
                                    new Thread(new Runnable() {
                                        @Override
                                        public void run() {
                                            // Downloads the input file from S3.
                                            String localAppName = message.getBody().split(AwsBundle.Delimiter)[1];
                                            System.out.println("New task received: " + localAppName);
                                            // check if the localAppName is already in the map
                                            LocalDoneTasks.computeIfAbsent(localAppName, k -> new LinkedList<>());
                                            LocalRequests.computeIfAbsent(localAppName, k -> 0);
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
                                            awsBundle.deleteMessageFromQueue(LocalAndManagerQueueUrl, message);
                                            int numberOfNewTasks = createSqsMessagesForEachUrl(fileName, localAppName);
                                            // Checks the SQS message count and starts Worker processes (nodes) accordingly.
                                            // The manager should create a worker for every n messages, if there are no running workers.
                                            // If there are k active workers, and the new job requires m workers, then the
                                            // manager should create m-k new workers, if possible
                                            startWorkersAccordinglyToSqsMessageCount(numberOfNewTasks);
                                        }
                                    }).start();
                                }
                                break;
                            case "Terminate":
                                // If the message is a termination message, then the manager:
                                // Does not accept any more input files from local applications.
                                shouldTerminate.set(true);
                                System.out.println("Terminate Manager");
                                // Waits for all the workers to finish their job, and then terminates them.
                                // Creates response messages for the jobs, if needed.
                                // Terminates
                                System.out.println("Waiting for all workers to finish their job");
                                while(!LocalRequests.isEmpty()){
                                    Thread.sleep(20000);
                                }
                                System.out.println("All workers finished their job");
                                System.out.println("Sending Terminate messages to workers");
                                // send to workers queue terminate message
                                for (int i = 0 ; i < activeWorkers.get() ; i++){
                                    awsBundle.sendMessage(awsBundle.managerAndWorkerQueueName, "Terminate" + AwsBundle.Delimiter + "Terminate" + AwsBundle.Delimiter + "Terminate" + AwsBundle.Delimiter + "Terminate");
                                }
                                // Sleep for 1 minute
                                Thread.sleep(60000);
                                try{
                                    awsBundle.deleteQueue(LocalAndManagerQueueUrl);
                                    awsBundle.deleteQueue(awsBundle.managerAndWorkerQueueName);
                                }catch (Exception ignored){}
                                break;
                            case "DonePdfTask":
                                new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                        String localAppNameToSendDone = message.getBody().split(AwsBundle.Delimiter)[1];
                                        String s3FileUrl = message.getBody().split(AwsBundle.Delimiter)[2];
                                        String operation = message.getBody().split(AwsBundle.Delimiter)[3];
                                        String OriginalFileName = message.getBody().split(AwsBundle.Delimiter)[4];

                                        System.out.println("Done task received: " + localAppNameToSendDone + "  " + s3FileUrl + " " + operation);
                                        LocalDoneTasks.computeIfAbsent(localAppNameToSendDone, k -> new LinkedList<>());
                                        List<Triplet<String, String, String>> allTasks = LocalDoneTasks.get(localAppNameToSendDone);
                                        allTasks.add(new Triplet<>(s3FileUrl, operation, OriginalFileName));

                                        awsBundle.deleteMessageFromQueue(ManagerAndWorkerQueueUrl, message);

                                        LocalRequests.replace(localAppNameToSendDone, LocalRequests.get(localAppNameToSendDone), LocalRequests.get(localAppNameToSendDone) - 1);
                                        if (LocalRequests.get(localAppNameToSendDone) == 0) {
                                            createSummaryReport(localAppNameToSendDone);
                                            LocalRequests.remove(localAppNameToSendDone);
                                        }
                                    }
                                }).start();
                                break;
                        }
                    }
                } catch (Exception ignored) {}
            }catch (Exception e) {
                // Sleep for 10 seconds, and then try again
                try {
                Thread.sleep(10000);
                } catch (InterruptedException ignored) {}
            }

        }
    }

    public static void createSummaryReport(String localAppName){
        System.out.println("Creating summary report for " + localAppName);
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
            //e.printStackTrace();
        }

        try {

            File f = new File("./" +"summery" + ".txt");
            awsBundle.uploadFileToS3(AwsBundle.bucketName,"summery.txt",f);
            try {
                boolean res = f.delete();
            } catch (Exception e) {
                //e.printStackTrace();
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }

        awsBundle.sendMessage(
                awsBundle.localAndManagerQueueName + localAppName,
                awsBundle.createMessage("DoneTask","summery.txt"));
    }


    private static void startWorkersAccordinglyToSqsMessageCount(int numberOfNewTasks) {
        int numberOfWorkersNeeded = Math.min(AwsBundle.MAX_INSTANCES, Math.max(1, numberOfNewTasks / numberOfMessagesPerWorker));
        System.out.println("Number of workers needed: " + numberOfWorkersNeeded);
        if (numberOfWorkersNeeded > activeWorkers.get()) {
            for (int i = 0; i < numberOfWorkersNeeded - activeWorkers.get(); i++) {
                // Start Worker
                if(activeWorkers.get() < AwsBundle.MAX_INSTANCES - 1) {
                    //createWorker(activeWorkers);
                    activeWorkers.getAndIncrement();
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

    public static int createSqsMessagesForEachUrl(String fileName, String localAppName){
        File inputFile = new File(fileName);
        int numberOfTasks = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line = "";
            while (true) {
                try {
                    if ((line = br.readLine()) == null) break;
                } catch (IOException e) {
                    //e.printStackTrace();
                }
                String[] urlAndOperation = line.split("\t");
                String operation = urlAndOperation[0];
                String url = urlAndOperation[1];
                awsBundle.sendMessage(
                        awsBundle.managerAndWorkerQueueName,
                        awsBundle.createMessage("PdfTask", operation + AwsBundle.Delimiter + url + AwsBundle.Delimiter + localAppName));
                System.out.println("Sending pdt task to worker " + url + " " + operation + " " + localAppName);

                numberOfTasks++;
                LocalRequests.computeIfAbsent(localAppName, k -> 0);
                LocalRequests.replace(localAppName,LocalRequests.get(localAppName),numberOfTasks);
            }
        } catch (IOException e) {
            //e.printStackTrace();
        }

        try {
            boolean res = inputFile.delete();
        } catch (Exception e) {
            //e.printStackTrace();
        }

        System.out.println("Number of new tasks " + numberOfTasks + " added for " + localAppName);
        return LocalRequests.get(localAppName);
    }
}

