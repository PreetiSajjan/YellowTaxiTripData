#!/usr/bin/env python
import time
from collections import defaultdict

import stomp
from datetime import datetime
import pandas as pd

EXIT = False


class MyListener(stomp.ConnectionListener):
    zone_lookup = pd.read_csv("C:/Users/User/Desktop/NUI/sem2/Research Topics/taxi+_zone_lookup.csv")
    zone_lookup['Borough_Zone'] = zone_lookup['Borough'] + ', ' + zone_lookup['Zone']
    zone_lookup = zone_lookup.drop(['Borough', 'Zone', 'service_zone'], axis=1)

    i = 0
    prev_date = 0
    prev_time = 0
    main_window = {}  # superset
    tumbling_window = {}  # daily tumbling

    def on_error(self, headers, message):
        print('received an error "%s"' % message)
 
    def peak_location(self):
        peak_location = max(self.tumbling_window[self.prev_date][self.prev_time],
                            key=self.tumbling_window[self.prev_date][self.prev_time].get)

        location = self.zone_lookup.loc[self.zone_lookup['LocationID'] == int(peak_location), 'Borough_Zone']
        print("Peak location of hour", self.prev_time, "of date", self.prev_date, "is: ", list(location)[0])

    def peak_time(self):
        i_time = []
        self.main_window.update(self.tumbling_window)
        ll = list(self.tumbling_window[self.prev_date].values())
        for x in ll:
            i_time.append(sum(x.values()))
        j = max(i_time)
        element = i_time.index(j)
        print("Peak time of the day ", self.prev_date, " : ", list(self.tumbling_window[self.prev_date])[element], "\n")

    def m_window(self, m_date, m_time, m_location):
        if m_date in self.tumbling_window:
            # self.prev_date = m_date
            if m_time in self.tumbling_window[m_date]:
                if m_location in self.tumbling_window[m_date][m_time]:
                    self.tumbling_window[m_date][m_time][m_location] += 1
                else:
                    self.tumbling_window[m_date][m_time][m_location] = 1
                    # self.prev_time = m_time
            else:
                self.peak_location()

                self.tumbling_window[m_date][m_time] = {}
                self.tumbling_window[m_date][m_time][m_location] = 1
                self.prev_time = m_time
        else:
            if self.i == 0:
                print("\nStarting the Yellow Taxi Trip data\n")
                self.tumbling_window[m_date] = {}
                self.tumbling_window[m_date][m_time] = {}
                self.tumbling_window[m_date][m_time][m_location] = 1
                self.prev_date = m_date
                self.prev_time = m_time
                self.i = 1
            else:
                self.peak_time()
                self.tumbling_window.clear()
                self.tumbling_window[m_date] = {}
                self.tumbling_window[m_date][m_time] = {}
                self.tumbling_window[m_date][m_time][m_location] = 1
                self.prev_date = m_date
                self.prev_time = m_time

    def on_message(self, headers, message):
        message = message.split('$')
        datetime_object = datetime.strptime(message[0], '%Y-%m-%d %H:%M:%S')
        location_id = message[1]
        self.m_window(datetime_object.date(), datetime_object.hour, location_id)

        if 'exit' in message:
            print("EXIT")
            # print("Dictionary: \n", self.hourly_dict)
            global EXIT
            EXIT = True


def main():
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')])
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    conn.subscribe(destination='/queue/test', id=1, ack='auto')
    while not EXIT:
        time.sleep(0.1)
    conn.disconnect()


if __name__ == '__main__':
    main()
