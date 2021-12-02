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
import java.util.List;

public class Worker extends Thread {
    final static AwsBundle awsBundle = AwsBundle.getInstance();
    private static final String INPUT_DIR_PATH = "";
    public boolean shouldTerminate = false;

    public void RunWorker() {

        String queueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);

        // Get a message from an SQS queue.
        while (!shouldTerminate) {
            List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
            for (Message message : messages) {
                String messageBody = message.getBody();
                if (messageBody.equals("terminate")) {
                    shouldTerminate = true;
                }else if (messageBody.equals("PdfTask")) {
                    // Download the PDF file indicated in the message.
                    // Perform the operation requested on the file.
                    ConvertFirstPage("","");
                    // Upload the resulting output file to S3.
                    // Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new
                    // image file, and the operation that was performed.
                    // remove the processed message from the SQS queue.
                } else {}
            }
        }
    }

    public void ConvertFirstPage(String fileName,String ConvertMethod){
        File file = new File(INPUT_DIR_PATH + fileName);
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

                if (ConvertMethod.equals("ToImage")) {
                    //Instantiating the PDFRenderer class
                    PDFRenderer renderer = new PDFRenderer(page);

                    //Rendering an image from the PDF document
                    BufferedImage image = renderer.renderImage(0);
                    //Writing the image to a file
                    ImageIO.write(image, "PNG", new File("./example.png"));
                } else if(ConvertMethod.equals("ToText")){
                    //Retrieving text from PDF document
                    String text = pdfStripper.getText(page);

                    try {
                        FileWriter myWriter = new FileWriter("./example.txt");
                        myWriter.write(text);
                        myWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else if(ConvertMethod.equals("ToHTML")){

                    //Retrieving text from PDF document
                    String text = pdfStripper.getText(page);

                    try {
                        FileWriter myWriter = new FileWriter("./example.html");
                        myWriter.write(text);
                        myWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            document.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
