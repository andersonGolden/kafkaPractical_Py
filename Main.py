if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'localhost:9092'  # Adjust this to your Kafka broker address
    }
    kafka_topic = 'customer_orders'

    service = CustomerOrdersService(kafka_config, kafka_topic)
    service.create_order("John Doe", "Pizza", 1200.00)