import com.amazonaws.services.sqs.model.Message;

import java.util.List;

public class Worker {
    final static AwsBundle awsBundle = AwsBundle.getInstance();
    public boolean shouldTerminate = false;

    public void RunWorker() {

        String queueUrl = awsBundle.createMsgQueue(awsBundle.managerAndWorkerQueueName);

        while (!shouldTerminate) {
            List<Message> messages = awsBundle.fetchNewMessages(queueUrl);
            for (Message message : messages) {
                String messageBody = message.getBody();
                if (messageBody.equals("terminate")) {
                    shouldTerminate = true;
                }else if (messageBody.equals("PdfTask")) {

                } else {}
            }
        }

    }
}
