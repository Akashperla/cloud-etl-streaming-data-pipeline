import os
import unittest
from unittest.mock import patch

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["PROCESSING_QUEUE_URL"] = "https://sqs.us-east-1.amazonaws.com/123456789012/pipeline"

import lambda_handler


class LambdaHandlerTest(unittest.TestCase):
    @patch.object(lambda_handler.sqs, "send_message")
    def test_s3_event_is_published_to_sqs(self, send_message):
        send_message.return_value = {"MessageId": "msg-1"}

        event = {
            "Records": [
                {
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "bucket": {"name": "raw-data"},
                        "object": {"key": "incoming%2Ftransactions.csv"},
                    },
                }
            ]
        }

        result = lambda_handler.lambda_handler(event, None)

        self.assertEqual(1, result["processed"])
        self.assertEqual("incoming/transactions.csv", result["messages"][0]["key"])
        send_message.assert_called_once()


if __name__ == "__main__":
    unittest.main()
