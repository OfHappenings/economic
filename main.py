import datetime
import argparse

from influxdb import InfluxDBClient

import Quandl 

#m = Quandl.get('OPEC/ORB', authtoken='')

class Stats():
    def __init__(self, ip, port):
        self.authtoken = 'bcMJsYVxMBBirQsvkpLH'
        self.client = InfluxDBClient(ip, port, 'root', 'root', 'economic')


    def get_data(self, query, fill=False):
        if fill == True:
            data = Quandl.get(query, authtoken=self.authtoken, returns='numpy')

        else:
            data = Quandl.get(query, authtoken=self.authtoken, returns='numpy', rows=3)



        for row in data:
            print(row)
            body = [
                    {
                        'measurement':query,
                        'time':row[0],
                        'fields':
                        {
                            'value':row[1]
                        }
                    }
                   ]

            self.client.write_points(body)






if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='store economic data into InfluxDB')
    parser.add_argument('--db-ip',   action='store', default='localhost')
    parser.add_argument('--db-port', action='store', default=8086, type=int)
    parser.add_argument('--query',   action='store', required=True)
    parser.add_argument('--fill',    action='store_true')


    args = parser.parse_args()
    stats = Stats(args.db_ip, args.db_port)

    stats.get_data(args.query)
