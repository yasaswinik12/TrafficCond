import time
import xml.sax
import requests
import logging
import signal
import pathlib, os
import traceback
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, UniqueConstraint, DateTime, Time, desc
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

XML_URL = "https://txdot-its-c2c.txdot.gov/xmldataportal_ut_ctr/api/c2c?networks=AUS&dataTypes=trafficCondData"

# Define the database connection
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = '1612'

UPDATE_TIME = 15

Base = declarative_base()

class TrafficData(Base):
    __tablename__ = 'traffic_cond_data_sep'

    uid = Column("uid", Integer, autoincrement=True, primary_key=True)
    id = Column("id", String)
    typ = Column("type", String)
    volume = Column("volume", Integer)
    speed = Column("speed", Integer)
    occupancy = Column("occupancy", Integer)
    timestamp = Column("timestamp", String)
    timestamp_dt = Column("timestamp_dt", DateTime)
    timestamp_diff = Column("timestamp_difference", Integer, default=0)

_exitFlag = False
def receiveSignal(sig, stackFrame):
    """
    Signal handler for alerting that we should exit.
    """
    global _exitFlag
    if sig == signal.SIGINT:
        print("\nReceived SIGINT signal (Ctrl+C). Exiting gracefully...")
    elif sig == signal.SIGTERM:
        print("\nReceived SIGTERM signal (Ctrl+\). Exiting gracefully...")
    _exitFlag = True

class TrafficHandler(xml.sax.ContentHandler):
    def __init__(self, session):
        xml.sax.ContentHandler.__init__(self)
        self.session = session
        self.current_traffic = None
        self.current_tag = ''
        self.current_data = {}
        self.flag = False

    def startElement(self, name, attrs):
        self.current_tag = name
        if self.current_tag == "trafficCond":
            self.current_traffic = TrafficData()
            self.current_traffic.id = attrs['id']

    def characters(self, content):
        if self.current_tag in ('type', 'volume', 'speed', 'occupancy', 'timestamp'):
            self.current_data[self.current_tag] = content

    def endElement(self, name):
        if name == 'trafficCond':
            # Check if a record with the same id already exists
            existing_record = self.session.query(TrafficData).filter_by(id=self.current_traffic.id).group_by(TrafficData.id, TrafficData.uid).order_by(desc(TrafficData.timestamp)).limit(1).all()
            if existing_record:
                logging.info("---Existing_record---")
                for record in existing_record:
                    if record.timestamp != self.current_data["timestamp"]:
                        try:
                            if "type" in self.current_data:
                                self.current_traffic.typ = self.current_data["type"]
                            if "volume" in self.current_data:
                                self.current_traffic.volume = self.current_data["volume"]
                            if "speed" in self.current_data:
                                self.current_traffic.speed = self.current_data["speed"]
                            if "occupancy" in self.current_data:
                                self.current_traffic.occupancy = self.current_data["occupancy"]
                            if "timestamp" in self.current_data:
                                self.current_traffic.timestamp = self.current_data["timestamp"]
                            self.current_traffic.timestamp_dt = datetime.strptime(self.current_data["timestamp"], "%Y:%m:%d:%H:%M:%S")
                            self.current_traffic.timestamp_diff = int((datetime.strptime(self.current_data["timestamp"], "%Y:%m:%d:%H:%M:%S") - datetime.strptime(record.timestamp, "%Y:%m:%d:%H:%M:%S")).total_seconds())
                            self.session.add(self.current_traffic)
                            self.session.commit()
                            logging.info(f"added updated data for traffic ID: {self.current_traffic.id}")
                        except Exception as e:
                            self.session.rollback()
                            logging.error(f"Failed to add updated data for traffic ID: {self.current_traffic.id}")
                            logging.error(str(e))
                        self.current_traffic = None
                    else:
                        logging.info(f"No update in the record: {self.current_traffic.id}")
            else:
                try:
                    if "type" in self.current_data:
                        self.current_traffic.typ = self.current_data["type"]
                    if "volume" in self.current_data:
                        self.current_traffic.volume = self.current_data["volume"]
                        self.flag = True
                    if "speed" in self.current_data:
                        self.current_traffic.speed = self.current_data["speed"]
                        self.flag = True
                    if "occupancy" in self.current_data:
                        self.current_traffic.occupancy = self.current_data["occupancy"]
                        self.flag = True
                    if "timestamp" in self.current_data:
                        self.current_traffic.timestamp = self.current_data["timestamp"]
                    self.current_traffic.timestamp_dt = datetime.strptime(self.current_data["timestamp"], "%Y:%m:%d:%H:%M:%S")
                    self.current_traffic.timestamp_diff = 0
                    if self.flag:
                        self.session.add(self.current_traffic)
                        self.session.commit()
                        logging.info(f"added data for traffic ID: {self.current_traffic.id}")
                    else:
                        logging.info(f"no volume or speed or occupancy present for traffic ID: {self.current_traffic.id}")
                except Exception as e:
                    self.session.rollback()
                    logging.error(f"Failed to add data for traffic ID: {self.current_traffic.id}")
                    logging.error(str(e))
                self.current_traffic = None

def create_database():
    # Create the engine and connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    Base.metadata.create_all(engine)

    # Create the session
    Session = sessionmaker(bind=engine)
    session = Session()

    return engine, session


def main():
    log_folder = "/home/yk9253/ctr/trafficCond/aus/logs"
    pathlib.Path(log_folder).mkdir(exist_ok=True)
    file_name = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".log"

    # Set log file path
    log_file_path = os.path.join(log_folder, file_name)

    # Set up logging
    logging.basicConfig(filename=log_file_path,level=logging.INFO, filemode='w', format='%(name)s - %(asctime)s - %(levelname)s - %(message)s')

    engine, session = create_database()

    handler = TrafficHandler(session)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)
    signal.signal(signal.SIGINT, receiveSignal) # <-- This handles CTRL-C
    signal.signal(signal.SIGTERM, receiveSignal) # <-- This handles CTRL-\ (quit + core dump)
    while True:
        try:
            if _exitFlag: # This is where we should safely exit.
                break
            logging.info("Parsing XML data...")
            parser.parse(XML_URL)
            logging.info("Parsing completed.")
        except Exception as e:
            logging.error("An error occurred during XML parsing.", str(e))
            logging.error(traceback.format_exc())
        time.sleep(UPDATE_TIME)
    session.close() # KAP: Trying to explicitly close database objects
    engine.dispose()


if __name__ == '__main__':
    main()
