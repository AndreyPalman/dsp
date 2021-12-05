import com.amazonaws.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
    public static final String INPUT_DIR_PATH = "C:\\Users\\Andrey\\Desktop\\dsp-ass1\\src\\Input\\";
    public static final String OUTPUT_DIR_PATH = "C:\\Users\\Andrey\\Desktop\\dsp-ass1\\src\\Output\\";

    public static boolean ProcessDoneMessageArrive = false;

    public static void main(String[] args){

        bucketName = AwsBundle.bucketName;
        parseArguments(args);

        // Check if Manager node is active
        // If not, create Manager node
        if(!awsBundle.checkIfInstanceExist("Manager"))
        {
                createManager();
        }
        UUID localAppUuid = UUID.randomUUID();
        bucketName = (AwsBundle.bucketName + localAppUuid.toString().toLowerCase()).toLowerCase();


        // Upload input file to S3
        awsBundle.createBucketIfNotExists(bucketName);
        awsBundle.uploadFileToS3(bucketName,inputFileName,inputFile);

        // Send message to an SQS queue, with the location of the file on S3
        String queueUrl = awsBundle.createMsgQueue(awsBundle.localAndManagerQueueName);
        awsBundle.sendMessage(queueUrl,awsBundle.createMessage("NewTask",localAppUuid + AwsBundle.Delimiter +filePathInS3));

        // Checks an SQS queue for messages indicating the process is done and response ( summery file ) is available on S3
        while (!ProcessDoneMessageArrive) {
            List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
            for (Message message : messages) {
                if (message.getBody().split(AwsBundle.Delimiter)[0].equals("DoneTask")) {
                    ProcessDoneMessageArrive = true;
                    String fileUrlInS3 = message.getBody().split(AwsBundle.Delimiter)[1];

                    InputStream downloadedFileStream = awsBundle.downloadFileFromS3(bucketName, fileUrlInS3);
                    try {
                        Files.copy(downloadedFileStream, Paths.get(fullPathToOutputFile), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    createOutputFile(fullPathToOutputFile);

                    awsBundle.deleteMessageFromQueue(queueUrl, message);
                    awsBundle.deleteQueue(queueUrl);
                }
            }
        }

        // Creates a html file representing the summery results


        // In case of terminate mode, sends a termination message to Manager
        if(shouldTerminate) {
            awsBundle.sendMessage(queueUrl,awsBundle.createMessage("Terminate",""));
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
                if (args[3].equals("terminate"))
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

    public static boolean isLegalFileSize(File file) {
        // check if file size is greater than 10 MB return false else true
        return file.length() < AwsBundle.maxFileSize;
    }

    private static void createManager(){
        String managerScript = "#! /bin/bash\n" +
                "sudo yum update -y\n" +
                "mkdir ManagerFiles\n" +
                "aws s3 cp s3://ocr-assignment1/JarFiles/Manager.jar ./ManagerFiles\n" +
                "java -jar /ManagerFiles/Manager.jar\n";

        awsBundle.createInstance("Manager",AwsBundle.ami,managerScript);
    }

    public static void createOutputFile(String filePath){
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

    }
}
