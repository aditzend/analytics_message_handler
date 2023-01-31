import pika


class AnalyticsExchange:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )
        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange="analytics", exchange_type="fanout"
        )

    def send(self, message):
        self.channel.basic_publish(
            exchange="analytics", routing_key="", body=message
        )
        print(" [x] Sent %r" % message)
        self.connection.close()


# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host="localhost")
# )
# channel = connection.channel()

# channel.exchange_declare(exchange="logs", exchange_type="fanout")

# message = " ".join(sys.argv[1:]) or "info: Hello World!"
# channel.basic_publish(exchange="logs", routing_key="", body=message)
# print(" [x] Sent %r" % message)
# connection.close()
