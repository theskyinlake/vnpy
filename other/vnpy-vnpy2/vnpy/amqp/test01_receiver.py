from vnpy.amqp.consumer import receiver


if __name__ == '__main__':

    from time import sleep
    c = receiver(user='admin', password='admin')

    c.subscribe()

    while True:
        sleep(1)
