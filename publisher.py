#!/usr/bin/env python
from datetime import time

import stomp
import csv


def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)


def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.connect(login='system', passcode='manager', wait=True)

    with open("C:/Users/User/Desktop/NUI/sem2/Research Topics/Yellow_TripData.csv", 'r') as data_file:
        next(data_file)
        data_file.readline()  # Skip first line
        reader = csv.reader(data_file)
        for row in reader:
            msg = row[1] + '$' + row[7]
            publish(conn, str(msg), '/queue/test')  # 1,7
    publish(conn, str('exit'), '/queue/test')

    # for number in numbers:
    #   print(number)
    # publish(conn, str(number), '/queue/test')
    # publish(conn, str('exit'), '/queue/test')
    conn.disconnect()


if __name__ == '__main__':
    main() 
