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

import static java.lang.System.exit;

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
            try {
                List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
                for (Message message : messages) {
                    if(!shouldTerminate) {
                        String messageBody = message.getBody();
                        String[] messageParts = messageBody.split(AwsBundle.Delimiter);
                        String messageType = messageParts[0];
                        String operation = messageParts[1];
                        String fileName = messageParts[2];
                        String localAppName = messageParts[3];

                        if (messageType.equals("Terminate")) {
                            shouldTerminate = true;
                            awsBundle.deleteQueue(queueUrl);
                            threadPool.shutdown();
                            System.out.println("Terminating worker");
                        } else if (messageType.equals("PdfTask")) {
                            // Download the PDF file indicated in the message.
                            // Perform the operation requested on the file.
                            awsBundle.deleteMessageFromQueue(queueUrl, message);
                            Task t = new Task(fileName, operation, localAppName);
                            threadPool.execute(t);
                        }
                    }
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
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
        IOException exception = new IOException("Exception in preforming operation");
        String fileNameTrimmed = "";
        String fileToDeletePath = "";
        File file = null;
        boolean taskCompleted = false;
        try {
            fileNameTrimmed = fileName.substring(fileName.lastIndexOf("/") + 1, fileName.lastIndexOf(".pdf"));
            String saveLocation = "./" + fileNameTrimmed + ".pdf";
            fileToDeletePath = "";


            System.out.println("Starting task for: " + fileName + " with method: " + ConvertMethod + " and operation: " + localAppName);

            InputStream in = null;
            try {
                in = new URL(fileName).openStream();

                try {
                    Files.copy(in, Paths.get(saveLocation), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    //e.printStackTrace();
                    exception = e;
                }

            } catch (IOException e) {
                //e.printStackTrace();
                exception = e;
            }


            file = new File(saveLocation);
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
                                //e.printStackTrace();
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
                                //e.printStackTrace();
                                exception = e;
                            }
                            taskCompleted = true;
                            break;
                        }
                    }


                } catch (IOException e) {
                    //e.printStackTrace();
                    exception = e;
                }
            } catch (IOException e) {
                //e.printStackTrace();
                exception = e;
            }

            try {
                if (document != null) {
                    document.close();
                }
            } catch (IOException e) {
                //e.printStackTrace();
                exception = e;
            }
        } catch (Exception e) {
            exception =  new IOException("Error: " + e.getMessage());
        }

        if (taskCompleted) {
            awsBundle.uploadFileToS3(AwsBundle.bucketName, fileNameTrimmed, file);
            System.out.println("Task completed for: " + fileName +" uploaded to S3 and sending message to manager");

            awsBundle.sendMessage(
                    awsBundle.managerAndWorkerQueueName,
                    awsBundle.createMessage("DonePdfTask", localAppName + AwsBundle.Delimiter + fileNameTrimmed + AwsBundle.Delimiter + ConvertMethod + AwsBundle.Delimiter + fileName));
        } else {
            System.out.println("Task failed for: " + fileName + "Sending message to manager");
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
            //e.printStackTrace();
        }
    }
}

