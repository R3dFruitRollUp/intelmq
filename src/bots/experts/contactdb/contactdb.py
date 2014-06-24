import sys

from lib.bot import *
from lib.utils import *
from lib.event import *

CONTACTDB_LOCATION = 'http://contactdb.cert.pt:8000/'
MINIMUM_BGP_PREFIX_IPV4 = 24
MINIMUM_BGP_PREFIX_IPV6 = 120

def convert_contactdb_result(result):
    pass

class ContactDBExpertBot(Bot):

    def process(self):
        event = self.pipeline.receive()
        if event:
            ip = event.value("ip")
            
            # WRITE A SPECIFIC GENERIC UTIL for this kind of functionality (Cymru, ContactDB use it )
            (int_ip, ip_size, minimum_bgp) = ipstr_to_int(ip)
            binstr_ip = int_to_binstr(int_ip)[:minimum_bgp]

            result = self.cache.get(binstr_ip):
            if result is None:
                result = self.get_cache_result(ip)
                self.cache.set(binstr_ip, result)
            
            event.add("contactdb entity", result['entity']['name'])
            
            self.pipeline.send(event)
        self.pipeline.acknowledge()

    def get_cache_result(self, ip):
        import urllib2

if __name__ == "__main__":
    bot = ContactDBExpertBot(sys.argv[1])
    bot.start()