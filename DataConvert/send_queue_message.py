from azure.storage.queue import (
    QueueClient, 
    TextBase64EncodePolicy
)


def send_message(msg, connect_str, queue_name):
    base64_queue_client = QueueClient.from_connection_string(
                            conn_str=connect_str, queue_name=queue_name,
                            message_encode_policy = TextBase64EncodePolicy()
                        )
    base64_queue_client.send_message(msg)