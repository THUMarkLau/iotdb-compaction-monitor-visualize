import configparser
import logging

from iotdb.Session import Session

CONFIG_PATH = "config.ini"
config_parser = configparser.ConfigParser()
config_parser.read(CONFIG_PATH)
session = None
logger = logging.getLogger("Compaction-Monitor-Visualization")


def init_session():
    session = Session(config_parser.get("IoTDB", "host"), config_parser.get("IoTDB", "port"),
                      config_parser.get("IoTDB", "user"), config_parser.get("IoTDB", "password"))
    session.open(False)
    try:
        dataset = session.execute_query_statement("show storage group")
        monitor_sg_found = False
        while dataset.has_next():
            row = dataset.next()
            if row.get_fields()[0].get_string_value() == "root.compaction_monitor":
                monitor_sg_found = True
                break
        dataset.close_operation_handle()
        if not monitor_sg_found:
            logger.error("Cannot find the compaction monitor sg")
            exit(-1)
        else:
            logger.info("Compaction monitor sg found")
    finally:
        session.close()


def main():
    init_session()


if __name__ == '__main__':
    main()
