import com.amazonaws.services.sqs.model.Message;

import java.util.List;

public class Worker {
    final static AwsBundle awsBundle = AwsBundle.getInstance();
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
                    StartPreformingTheTask("");
                    // Upload the resulting output file to S3.
                    // Put a message in an SQS queue indicating the original URL of the PDF, the S3 url of the new
                    // image file, and the operation that was performed.
                    // remove the processed message from the SQS queue.
                } else {}
            }
        }
    }

    public void StartPreformingTheTask(String file){

    }
}
