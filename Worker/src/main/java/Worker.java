import com.amazonaws.services.sqs.model.Message;
import org.apache.pdfbox.multipdf.Splitter;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Worker {
    final static AwsBundle awsBundle = AwsBundle.getInstance();
    public static boolean shouldTerminate = false;
    static int MAX_T;


    public static void main(String[] args){
        try {
            MAX_T = Integer.parseInt(args[0]);
        } catch (Exception e) {
            MAX_T = 10;
        }
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_T);
        String queueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);

        // Get a message from an SQS queue.
        while (!shouldTerminate) {
            List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
            for (Message message : messages) {
                String messageBody = message.getBody();
                String[] messageParts = messageBody.split(AwsBundle.Delimiter);
                String messageType = messageParts[0];
                String operation = messageParts[1];
                String fileName = messageParts[2];
                String localAppName = messageParts[3];

                if (messageType.equals("terminate")) {
                    shouldTerminate = true;
                }else if (messageType.equals("PdfTask")) {
                    // Download the PDF file indicated in the message.
                    // Perform the operation requested on the file.
                    Task t = new Task(fileName,operation,localAppName);
                    threadPool.execute(t);
                    // Upload the resulting output file to S3.
                    // Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new
                    // image file, and the operation that was performed.
                    // remove the processed message from the SQS queue.

                    awsBundle.deleteMessageFromQueue(queueUrl, message);

                } else {}
            }
        }

    }
}

class Task implements Runnable {
    final static AwsBundle awsBundle = AwsBundle.getInstance();
    public String fileName;
    public String ConvertMethod;
    public String localAppName;

    public Task(String fileName, String ConvertMethod, String localAppName) {
        this.fileName = fileName;
        this.ConvertMethod = ConvertMethod;
        this.localAppName = localAppName;
    }

    @Override
    public void run() {
        String fileNameTrimmed = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.lastIndexOf(".pdf"));
        String saveLocation = "./" + fileNameTrimmed + ".pdf";
        String fileToDeletePath = "";
        IOException exception = new IOException("Exception in preforming operation");
        boolean taskCompleted = false;

        InputStream in = null;
        try {
            in = new URL(fileName).openStream();

            try {
                Files.copy(in, Paths.get(saveLocation), StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                e.printStackTrace();
                exception = e;
            }

        } catch (IOException e) {
            e.printStackTrace();
            exception = e;
        }


        File file = new File(saveLocation);
        PDDocument document = null;
        try {
            document = PDDocument.load(file);
            //Instantiating Splitter class
            Splitter splitter = new Splitter();

            //splitting the pages of a PDF document
            List<PDDocument> Pages = null;
            try {
                Pages = splitter.split(document);
                PDDocument page = Pages.get(0);
                PDFTextStripper pdfStripper = new PDFTextStripper();

                switch (ConvertMethod) {
                    case "ToImage":
                        //Instantiating the PDFRenderer class
                        PDFRenderer renderer = new PDFRenderer(page);

                        //Rendering an image from the PDF document
                        BufferedImage image = renderer.renderImage(0);
                        //Writing the image to a file
                        fileToDeletePath = "./" + fileNameTrimmed + ".png";
                        ImageIO.write(image, "PNG", new File("./" + fileNameTrimmed + ".png"));
                        taskCompleted = true;
                        break;
                    case "ToText": {
                        //Retrieving text from PDF document
                        String text = pdfStripper.getText(page);

                        try {
                            fileToDeletePath = "./" + fileNameTrimmed + ".txt";
                            FileWriter myWriter = new FileWriter("./" + fileNameTrimmed + ".txt");
                            myWriter.write(text);
                            myWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            exception = e;
                        }
                        taskCompleted = true;
                        break;
                    }
                    case "ToHTML": {

                        //Retrieving text from PDF document
                        String text = pdfStripper.getText(page);

                        try {
                            fileToDeletePath = "./" + fileNameTrimmed + ".html";
                            FileWriter myWriter = new FileWriter("./" + fileNameTrimmed + ".html");
                            myWriter.write(text);
                            myWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                            exception = e;
                        }
                        taskCompleted = true;
                        break;
                    }
                }


            } catch (IOException e) {
                e.printStackTrace();
                exception = e;
            }
        } catch (IOException e) {
            e.printStackTrace();
            exception = e;
        }

        try {
            if (document != null) {
                document.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            exception = e;
        }

        if (taskCompleted) {
            awsBundle.uploadFileToS3(AwsBundle.bucketName, fileNameTrimmed, file);

            awsBundle.sendMessage(
                    awsBundle.managerAndWorkerQueueName,
                    awsBundle.createMessage("DonePdfTask", localAppName + AwsBundle.Delimiter + fileNameTrimmed + AwsBundle.Delimiter + ConvertMethod + AwsBundle.Delimiter + fileName));
        } else {
            awsBundle.sendMessage(
                    awsBundle.managerAndWorkerQueueName,
                    awsBundle.createMessage("DonePdfTask", localAppName + AwsBundle.Delimiter +
                            exception.getMessage() + AwsBundle.Delimiter + ConvertMethod + AwsBundle.Delimiter + fileName));
        }

        // delete file from instance
        try {
            boolean res = file.delete();
            File f = new File(fileToDeletePath);
            boolean originalRes = f.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

