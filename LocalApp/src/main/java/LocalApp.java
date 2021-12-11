import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class LocalApp {

    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public static String inputFileName = "";
    public static String outputFileName = "";
    public static int numberOfMessagesPerWorker = 0;
    public static boolean shouldTerminate = false;

    public static String fullPathToInputFile = "";
    public static String fullPathToOutputFile = "";
    public static File inputFile;
    public static String filePathInS3 = "";

    public static String bucketName = "";
    public static final String INPUT_DIR_PATH = "C:\\Users\\Andrey\\Desktop\\dsp-ass1\\LocalApp\\src\\Input\\";
    public static final String OUTPUT_DIR_PATH = "C:\\Users\\Andrey\\Desktop\\dsp-ass1\\LocalApp\\src\\Output\\";

    public static boolean ProcessDoneMessageArrive = false;
    public static LocalDateTime startTime;
    public static LocalDateTime endTime;

    public static String region = "";
    public static String aws_access_key_id = "";
    public static String aws_secret_access_key = "";
    public static String aws_session_token = "";

    public static void main(String[] args) {



        parseAwsCredentials();
        UUID localAppUuid = UUID.randomUUID();
        bucketName = AwsBundle.bucketName;
        parseArguments(args);

        // Check if Manager node is active
        // If not, create Manager node
        if(!awsBundle.checkIfInstanceExist("Manager"))
        {
            // Create Manager node
            System.out.println("Creating Manager");
            createManager();
            // delete all active queues in SQS
            awsBundle.deleteAllQueues();
        }

        // Upload input file to S3
        awsBundle.createBucketIfNotExists(bucketName);
        awsBundle.uploadFileToS3(bucketName,inputFileName,inputFile);

        // Send message to an SQS queue, with the location of the file on S3
        String queueUrl = awsBundle.createMsgQueue(AwsBundle.localAndManagerQueueName);
        String localandmanagerqueueUrl = awsBundle.createMsgQueue(AwsBundle.localAndManagerQueueName + localAppUuid.toString().toLowerCase());
        awsBundle.sendMessage(queueUrl,awsBundle.createMessage("NewTask",localAppUuid + AwsBundle.Delimiter +filePathInS3));
        startTime = LocalDateTime.now();

        // Checks an SQS queue for messages indicating the process is done and response ( summery file ) is available on S3
        while (!ProcessDoneMessageArrive) {
            List<Message> messages = null;
            try {
                messages = awsBundle.fetchNewMessages(localandmanagerqueueUrl);
            }catch (Exception e) {
                messages = new LinkedList<>();
            }
            for (Message message : messages) {
                if (message.getBody().split(AwsBundle.Delimiter)[0].equals("DoneTask")) {
                    endTime = LocalDateTime.now();
                    System.out.println("Processing time: " + Math.abs(endTime.getHour() - startTime.getHour()) + ":" + Math.abs(endTime.getMinute() - startTime.getMinute()) + ":" + Math.abs(endTime.getSecond() - startTime.getSecond()));
                    ProcessDoneMessageArrive = true;
                    String fileUrlInS3 = message.getBody().split(AwsBundle.Delimiter)[1];

                    InputStream downloadedFileStream = awsBundle.downloadFileFromS3(bucketName, fileUrlInS3);
                    System.out.printf("Downloading summary file: %s from S3\n",  fileUrlInS3);
                    try {
                        Files.copy(downloadedFileStream, Paths.get(fullPathToOutputFile), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        //e.printStackTrace();
                    }

                    createOutputFile(fullPathToOutputFile,localAppUuid);
                    if(shouldTerminate) {
                        awsBundle.sendMessage(queueUrl, awsBundle.createMessage("Terminate", "Terminate" + AwsBundle.Delimiter + "Terminate"));
                    }
                    try {
                        awsBundle.deleteMessageFromQueue(localandmanagerqueueUrl, message);
                        awsBundle.deleteQueue(localandmanagerqueueUrl);
                    } catch (Exception ignored) {}
                }
            }
            // sleep for 10 seconds
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }
        }
    }

    private static void parseArguments(String[] args) {

        // args[0] = inputFileName is the name of the input file
        // args[1] = outputFileName is the name of the output file
        // args[2] = n is the workers’ files ratio (how many PDF files per worker)
        // args[3] = terminate indicates that the application should terminate the manager at the end

        if (args.length == 3 || args.length == 4) {
            inputFileName = args[0];
            outputFileName = args[1];
            numberOfMessagesPerWorker = Integer.parseInt(args[2]);

            fullPathToInputFile = INPUT_DIR_PATH + inputFileName;
            fullPathToOutputFile = OUTPUT_DIR_PATH + outputFileName;
            filePathInS3 = "S3://" + bucketName + "/" + inputFileName;
            inputFile = new File(fullPathToInputFile);
            if (!isLegalFileSize(inputFile))
            {
                System.out.println("Input file is over maximal size (10MB)");
                System.exit(1);
            }

            if (args.length == 4) {
                if (args[3].equals("true"))
                    shouldTerminate = true;
                else {
                    System.err.println("Invalid command line argument: " + args[3]);
                    System.exit(1);
                }
            }
        } else {
            System.err.println("Invalid number of command line arguments");
            System.exit(1);
        }
    }

    private static void parseAwsCredentials() {
        try {
            File f = new File("C:\\Users\\Andrey\\.aws\\config");
            // iterate over the lines in the file
            Scanner scanner = new Scanner(f);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.contains("region")) {
                    region = line.split("=",2)[1];
                }
                if (line.contains("aws_access_key_id")) {
                    aws_access_key_id = line.split("=",2)[1];
                }
                if (line.contains("aws_secret_access_key")) {
                    aws_secret_access_key = line.split("=",2)[1];
                }
                if (line.contains("aws_session_token")) {
                    aws_session_token = line.split("=",2)[1];
                }
            }
        }catch (Exception e) {
            System.out.println("Error while parsing AWS credentials");
        }
    }

    public static boolean isLegalFileSize(File file) {
        // check if file size is greater than 10 MB return false else true
        return file.length() < AwsBundle.maxFileSize;
    }

    private static void createManager(){

        String credentials = "[default]\n" + "region=" +region + "\n" + "aws_access_key_id=" + aws_access_key_id + "\n" + "aws_secret_access_key=" +aws_secret_access_key + "\n" + "aws_session_token=" + aws_session_token;
        String managerScript = String.format("#! /bin/bash\n" +
                "Starting instance script\n" +
                "sudo yum update -y\n" +
                "aws s3 cp s3://%s/Manager.jar Manager.jar\n" +
                "mkdir .aws\n" +
                "cd .aws\n" +
                "echo $'" + credentials +  "' > credentials\n" +
                "cat credentials\n" +
                "cd ..\n" +
                "echo Starting Manager instance\n" +
                "java -jar Manager.jar %d\n",AwsBundle.bucketName, numberOfMessagesPerWorker);

        awsBundle.createInstance("Manager",AwsBundle.ami,managerScript);
    }

    public static void createOutputFile(String filePath, UUID localAppUuid){
        File file = new File(filePath);
        String htmlPageOpening = "<!DOCTYPE html><html><body><h1>Assignment 1</h1><p><table><tr><th>Operation</th><th>Input file</th><th>Output file</th></tr>";
        String htmlPageEnding = "</table></p></body></html>";
        StringBuilder htmlBody = new StringBuilder();
        Scanner sc = null;
        try {
            sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String[] line = sc.nextLine().split(AwsBundle.Delimiter);
                try {
                    String operation = line[1];
                    String input = line[2];
                    String output = line[0];
                    htmlBody.append("<tr>").append("<th>").append(operation).append("</th>").append("<th>").append(input).append("</th>").append("<th>").append("s3://").append(bucketName).append("/").append(output).append("</th>").append("</tr>");
                }
                catch (ArrayIndexOutOfBoundsException ignored) {}
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        FileWriter myWriter = null;
        try {
            myWriter = new FileWriter("./output.html");
            try {
                myWriter.write(htmlPageOpening + htmlBody + htmlPageEnding);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                myWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Successfully created output file");
    }
}
