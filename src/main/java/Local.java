
import com.amazonaws.services.sqs.model.Message;
import com.google.gson.Gson;

import java.io.*;
import java.util.*;

public class Local {

    final static AwsBundle awsBundle = AwsBundle.getInstance();

    public static String inputFileName = "";
    public static String outputFileName = "";
    public static int numberOfWorkers = 0;
    public static boolean shouldTerminate = false;

    public static String fullPathToInputFile = "";
    public static File inputFile;
    public static String filePathInS3 = "";

    public static boolean ProcessDoneMessageArrive = false;


    public static void main(String[] args) {



        // Read Input Folder Path
        parseArguments(args);

        // Check if Manager node is active
        // If not, create Manager node
//        if(!awsBundle.checkIfInstanceExist("Manager"))
//        {
//                createManager();
//        }

        // Upload input file to S3
        awsBundle.createBucketIfNotExists(AwsBundle.bucketName);
        awsBundle.uploadFileToS3(AwsBundle.bucketName,inputFileName,inputFile);

        // Send message to an SQS queue, with the location of the file on S3
        String queueUrl = awsBundle.createMsgQueue(awsBundle.requestsAppsQueueName);
        awsBundle.sendMessage(queueUrl,createMessage("New Task",filePathInS3, AwsBundle.Delimiter));

        // Checks an SQS queue for messages indicating the process is done and response ( summery file ) is available on S3
        while (!ProcessDoneMessageArrive) {
            List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
            for (Message message : messages) {
                if (message.getBody().equals("Process Done")) {
                    ProcessDoneMessageArrive = true;
                }
            }
        }
        // Creates an html file representing the summery results

        // In case of terminate mode, sends a termination message to Manager
        if(shouldTerminate) {
            awsBundle.sendMessage(queueUrl,createMessage("Terminate","",AwsBundle.Delimiter));
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
            numberOfWorkers = Integer.parseInt(args[2]);

            fullPathToInputFile = "C:\\Users\\Andrey\\Desktop\\dsp-ass1\\src\\Input\\" + inputFileName;
            filePathInS3 = "S3://" + AwsBundle.bucketName + "/" + inputFileName;
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

    private static String createMessage(String type, String filePath,String delimiter){
        return type + filePath + delimiter;
    }


}
