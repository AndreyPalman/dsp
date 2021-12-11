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

    public static String region = "";
    public static String aws_access_key_id = "";
    public static String aws_secret_access_key = "";
    public static String aws_session_token = "";

    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public static ConcurrentHashMap<String, AtomicInteger> LocalRequests = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String, List<Triplet<String, String, String>>> LocalDoneTasks = new ConcurrentHashMap<>();


    public static AtomicBoolean shouldTerminate = new AtomicBoolean(false);
    public static AtomicBoolean shouldNotTakeNewTasks = new AtomicBoolean(false);
    public static int numberOfMessagesPerWorker = 100;
    public static AtomicInteger activeWorkers = new AtomicInteger(1);

    public static void main(String[] args) {

        // terminate all workers the somehow are still running
        awsBundle.terminateRunningAllWorkers();

        try {
            region = System.getenv("aws_region");
            aws_access_key_id = System.getenv("aws_access_key_id");
            aws_secret_access_key = System.getenv("aws_secret_access_key");
            aws_session_token = System.getenv("aws_session_token");
        } catch (Exception e) {
            System.out.println("Error reading environment variables");
        }

        try {
            numberOfMessagesPerWorker = Integer.parseInt(args[0]);
        } catch (Exception e) {
            numberOfMessagesPerWorker = 100;
        }

        System.out.println("Number of messages per worker: " + numberOfMessagesPerWorker);

        String ManagerAndWorkerQueueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);
        String ManagerAndWorkerDoneQueueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerDoneQueueName);
        String LocalAndManagerQueueUrl = awsBundle.getQueueUrl(awsBundle.localAndManagerQueueName);


        // Listen to done tasks
        new Thread(new Runnable() {
            @Override
            public void run() {

                while (!shouldTerminate.get()) {
                    List<Message> messages = null;
                    try {
                        messages = awsBundle.fetchNewMessages(ManagerAndWorkerDoneQueueUrl);
                    }
                    catch (Exception e) {
                        //System.out.println("Error fetching messages from ManagerAndWorkerDoneQueueUrl");
                        messages = new LinkedList<>();
                    }
                    for (Message message : messages) {
                        new Thread(() -> {
                            String localAppNameToSendDone = message.getBody().split(AwsBundle.Delimiter)[1];
                            String s3FileUrl = message.getBody().split(AwsBundle.Delimiter)[2];
                            String operation = message.getBody().split(AwsBundle.Delimiter)[3];
                            String OriginalFileName = message.getBody().split(AwsBundle.Delimiter)[4];

                            System.out.println("Done task received: " + localAppNameToSendDone + "  " + s3FileUrl + " " + operation + " Tasks left for local : " + LocalRequests.get(localAppNameToSendDone));
                            LocalDoneTasks.computeIfAbsent(localAppNameToSendDone, k -> new LinkedList<>());
                            List<Triplet<String, String, String>> allTasks = LocalDoneTasks.get(localAppNameToSendDone);
                            allTasks.add(new Triplet<>(s3FileUrl, operation, OriginalFileName));

                            awsBundle.deleteMessageFromQueue(ManagerAndWorkerDoneQueueUrl, message);

                            try {
                                LocalRequests.get(localAppNameToSendDone).decrementAndGet();
                            }catch (Exception ignored){}

                            try {
                                if (LocalRequests.get(localAppNameToSendDone).get() <= 0) {
                                    LocalRequests.remove(localAppNameToSendDone);
                                    createSummaryReport(localAppNameToSendDone);
                                }
                            }catch (Exception e){
                                // ignore
                            }


                        }).start();
                    }
                }
            }
        }).start();
        while (!shouldTerminate.get()) {
            try {
                try {
                    List<Message> messages = new LinkedList<>(awsBundle.fetchNewMessages(LocalAndManagerQueueUrl));
                    for (Message message : messages) {
                        // If the message is that of a new task it
                        String messageType = message.getBody().split(AwsBundle.Delimiter)[0];
                        switch (messageType) {
                            case "NewTask":
                                if (!shouldNotTakeNewTasks.get()) {
                                    new Thread(new Runnable() {
                                        @Override
                                        public void run() {
                                            // Downloads the input file from S3.
                                            String localAppName = message.getBody().split(AwsBundle.Delimiter)[1];
                                            System.out.println("New task received: " + localAppName);
                                            // check if the localAppName is already in the map
                                            LocalDoneTasks.computeIfAbsent(localAppName, k -> new LinkedList<>());
                                            LocalRequests.computeIfAbsent(localAppName, k -> new AtomicInteger(0));
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
                                shouldNotTakeNewTasks.set(true);
                                System.out.println("Terminate Manager");
                                // Waits for all the workers to finish their job, and then terminates them.
                                // Creates response messages for the jobs, if needed.
                                // Terminates
                                System.out.println("Waiting for all workers to finish their job");
                                while (!LocalRequests.isEmpty()) {
                                    Thread.sleep(20000);
                                }
                                System.out.println("All workers finished their job");
                                System.out.println("Sending Terminate messages to workers");
                                // Terminate all workers
                                awsBundle.terminateRunningAllWorkers();
                                // Sleep for 1 minute
                                Thread.sleep(60000);
                                try {
                                    awsBundle.deleteQueue(LocalAndManagerQueueUrl);
                                }catch (Exception e){
                                    e.printStackTrace();
                                }
                                shouldTerminate.set(true);
                                break;
                        }
                    }
                } catch (Exception ignored) {
                }
            } catch (Exception e) {
                // Sleep for 10 seconds, and then try again
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException ignored) {
                }
            }

        }
        try {
            awsBundle.terminateCurrentInstance();
        }catch (Exception e){
            //e.printStackTrace();
        }

    }

    public static void createSummaryReport(String localAppName) {
        System.out.println("Creating summary report for " + localAppName);
        String text = "";
        List<Triplet<String, String, String>> doneTasks = LocalDoneTasks.get(localAppName);
        PrintWriter writer = null;
        try {
            writer = new PrintWriter("./" + "summery" + localAppName + ".txt", "UTF-8");
            for (Triplet<String, String, String> task : doneTasks) {
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

            File f = new File("./" + "summery" + localAppName + ".txt");
            awsBundle.uploadFileToS3(AwsBundle.bucketName, String.format("summery%s.txt", localAppName), f);
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
                awsBundle.createMessage("DoneTask", String.format("summery%s.txt", localAppName)));
    }


    private static void startWorkersAccordinglyToSqsMessageCount(int numberOfNewTasks) {
        System.out.println("Number of new tasks: " + numberOfNewTasks);
        int numberOfWorkersNeeded = Math.min(AwsBundle.MAX_INSTANCES, Math.max(1, numberOfNewTasks / numberOfMessagesPerWorker));
        System.out.println("Number of workers needed: " + numberOfWorkersNeeded);
        System.out.println("Number of workers currently active: " + (activeWorkers.get()-1));
        if (numberOfWorkersNeeded > (activeWorkers.get()-1)) {
            int numberOfWorkersToStart = numberOfWorkersNeeded - activeWorkers.get() + 1;
            for (int i = 0; i < numberOfWorkersToStart; i++) {
                // Start Worker
                if (activeWorkers.get() < AwsBundle.MAX_INSTANCES - 1) {
                    System.out.println("active workers: " + activeWorkers.get());
                    createWorker(activeWorkers.get());
                    activeWorkers.incrementAndGet();
                }
            }
        }
    }

    private static void createWorker(int activeWorkers) {

        String credentials = "[default]\n" + "region=" + region + "\n" + "aws_access_key_id=" + aws_access_key_id + "\n" + "aws_secret_access_key=" + aws_secret_access_key + "\n" + "aws_session_token=" + aws_session_token;
        String managerScript = String.format("#! /bin/bash\n" +
                "sudo yum update -y\n" +
                "aws s3 cp s3://%s/Worker.jar Worker.jar\n" +
                "java -jar Worker.jar %d\n", AwsBundle.bucketName, numberOfMessagesPerWorker);

        awsBundle.createInstance(String.format("Worker%d", activeWorkers), AwsBundle.ami, managerScript);
    }

    public static int createSqsMessagesForEachUrl(String fileName, String localAppName) {
        File inputFile = new File(fileName);
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
                System.out.println("Sending pdf task to worker " + url + " " + operation + " " + localAppName);

                LocalRequests.computeIfAbsent(localAppName, k -> new AtomicInteger(0));
                LocalRequests.get(localAppName).incrementAndGet();
            }
        } catch (IOException e) {
            //e.printStackTrace();
        }

        try {
            boolean res = inputFile.delete();
        } catch (Exception e) {
            //e.printStackTrace();
        }

        System.out.println("Number of new tasks " + LocalRequests.get(localAppName).get() + " added for " + localAppName);
        return LocalRequests.get(localAppName).get();
    }
}

