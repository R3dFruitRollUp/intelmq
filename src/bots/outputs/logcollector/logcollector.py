import sys
import time
import socket
from lib.bot import *
from lib.utils import *
from lib.event import *

try:
    import simplejson as json
except ImportError:
    import json

class LogCollectorBot(Bot):

    def process(self):       
        event = self.pipeline.receive()
        
        if event:
            data = ''
            for key, value in event.items():
                data += key.replace(' ','_') + '=' + json.dumps(value) + ' '
            data += "\n"

            self.send_data(data)
            
        self.pipeline.acknowledge()


    def connect(self):
        address = (self.parameters.ip, int(self.parameters.port))
        self.con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        while True:
            try:
                self.con.connect(address)
                break
            except socket.error, e:
                self.logger.error(e.args[1] + ". Retrying in 10 seconds.")
                time.sleep(10)

        self.logger.info("Connected successfully to %s:%i", address[0], address[1])

        
    def send_data(self, data):
        while True:
            try:
                self.con.send(unicode(data).encode("utf-8"))
                self.con.sendall("")
                break
            except socket.error, e:
                self.logger.error(e.args[1] + ". Reconnecting..")
                self.con.close()
                self.connect()
            except AttributeError:
                self.connect()


if __name__ == "__main__":
    bot = LogCollectorBot(sys.argv[1])
    bot.start()