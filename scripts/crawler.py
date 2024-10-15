import dns.resolver
from datetime import datetime
import os

base_dir = os.path.dirname(os.path.abspath(__file__))


class CrawlerThread:
    def __init__(self, handle_id, query_file_name):

        self.handle_id = handle_id

        self.query_file_name = query_file_name

        self.queries = []

        resolver = "8.8.8.8"

        self.domainErrorDict = set()

        dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
        dns.resolver.default_resolver.nameservers = [resolver]
        dns.resolver.timeout = 5

    def start(self, domain):
        self.getRecordsFromAuth(domain)
        return self.queries

    def save_query(self, query_data):
        self.queries.append(query_data)

    def getSimpleARecord(self, address):
        try:
            answers = dns.resolver.resolve(address, "A")
            records = []
            for rdata in answers:
                records.append(rdata.to_text())
            return records
        except Exception as e:
            self.domainErrorDict.add(address)
            return None

    def getSimpleAAAARecord(self, address):
        try:
            answers = dns.resolver.resolve(address, "AAAA")
            records = []
            for rdata in answers:
                records.append(rdata.to_text())
            return records
        except Exception as e:
            self.domainErrorDict.add(address)
            return None

    def getARecord(self, address):
        try:
            answers = dns.resolver.resolve(address, "A")
            record = []
            for rdata in answers:
                record.append(rdata.to_text())

                now = datetime.now()
                query = {}
                query["domain"] = address
                query["version_id"] = 1
                query["query_name"] = address
                query["query_type"] = "A"
                query["ipv4_address"] = rdata.to_text()
                query["ipv6_address"] = None
                query["as_number"] = None
                query["as_name"] = None
                query["bgp_prefix"] = None
                query["worker_id"] = None
                query["created_at"] = now.isoformat()
                query["updated_at"] = now.isoformat()

            self.save_query(query)

            return True
        except Exception as e:
            self.domainErrorDict.add(address)
            return None

    def getAAAARecord(self, address):
        try:
            answers = dns.resolver.resolve(address, "AAAA")
            record = []
            for rdata in answers:
                record.append(rdata.to_text())

                now = datetime.now()
                query = {}
                query["domain"] = address
                query["version_id"] = 1
                query["query_name"] = address
                query["query_type"] = "AAAA"
                query["ipv4_address"] = None
                query["ipv6_address"] = rdata.to_text()
                query["as_number"] = None
                query["as_name"] = None
                query["bgp_prefix"] = None
                query["worker_id"] = None
                query["created_at"] = now.isoformat()
                query["updated_at"] = now.isoformat()

                self.save_query(query)

            return True
        except Exception as e:
            self.domainErrorDict.add(address)
            return None

    def getNSRecord(self, address):
        try:
            answers = dns.resolver.resolve(address, "NS")
            for rdata in answers:
                now = datetime.now()
                query = {}
                query["domain"] = address
                query["version_id"] = 1
                query["query_name"] = rdata.to_text()
                query["query_type"] = "NS"
                query["ipv4_address"] = ",".join(
                    self.getSimpleARecord(rdata.to_text())
                ).join(("[", "]"))
                query["ipv6_address"] = ",".join(
                    self.getSimpleAAAARecord(rdata.to_text())
                ).join(("[", "]"))
                query["as_number"] = None
                query["as_name"] = None
                query["bgp_prefix"] = None
                query["worker_id"] = None
                query["created_at"] = now.isoformat()
                query["updated_at"] = now.isoformat()

                self.save_query(query)

            return True
        except Exception as e:
            self.domainErrorDict.add(address)
            return None

    def getMXRecord(self, address):
        try:
            answers = dns.resolver.resolve(address, "MX")
            for rdata in answers:
                now = datetime.now()
                mx_split = rdata.to_text().split()
                a_records = self.getSimpleARecord(mx_split[1])
                aaaa_records = self.getSimpleAAAARecord(mx_split[1])
                for a_record in a_records:
                    query = {}
                    query["domain"] = address
                    query["version_id"] = 1
                    query["query_name"] = mx_split[1]
                    query["query_type"] = "MX"
                    query["ipv4_address"] = a_record
                    query["ipv6_address"] = None
                    query["as_number"] = None
                    query["as_name"] = None
                    query["bgp_prefix"] = None
                    query["worker_id"] = None
                    query["created_at"] = now.isoformat()
                    query["updated_at"] = now.isoformat()

                    self.save_query(query)

                for aaaa_record in aaaa_records:
                    query = {}
                    query["domain"] = address
                    query["version_id"] = 1
                    query["query_name"] = mx_split[1]
                    query["query_type"] = "MX"
                    query["ipv4_address"] = None
                    query["ipv6_address"] = aaaa_record
                    query["as_number"] = None
                    query["as_name"] = None
                    query["bgp_prefix"] = None
                    query["worker_id"] = None
                    query["created_at"] = now.isoformat()
                    query["updated_at"] = now.isoformat()

                    self.save_query(query)

            return True
        except Exception as e:
            self.domainErrorDict.add(address)
            return None

    def getRecordsFromAuth(self, fqdn):
        self.getARecord(fqdn)
        self.getAAAARecord(fqdn)
        self.getNSRecord(fqdn)
        self.getMXRecord(fqdn)
