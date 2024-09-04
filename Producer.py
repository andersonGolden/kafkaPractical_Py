from confluent_kafka import Producer
import json

class CustomerOrdersService:
    def __init__(self, kafka_config, kafka_topic):
        """
        Initialize the CustomerOrdersService with Kafka configuration and topic.

        :param kafka_config: Dictionary containing Kafka configuration.
        :param kafka_topic: Kafka topic name where messages will be produced.
        """
        self.producer = Producer(kafka_config)
        self.kafka_topic = kafka_topic

    def create_order(self, customer_name, order, amount):
        """
        Create an order and produce it to a Kafka topic.

        :param customer_name: Name of the customer.
        :param order: Details of the order.
        :param amount: Amount for the order.
        """
        order_object = {
            "customerName": customer_name,
            "order": order,
            "amount": amount
        }
        
        # Serialize the order object to a JSON string
        order_json = json.dumps(order_object)
        
        # Produce the order to the Kafka topic
        self.producer.produce(self.kafka_topic, value=order_json)
        
        # Wait up to 1 second for events to be delivered
        self.producer.flush()