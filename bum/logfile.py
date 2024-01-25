import logging
from logging.handlers import TimedRotatingFileHandler
import os


logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

logFileName = './log_file/'+'ukiTax.log'

formatter = logging.Formatter('%(asctime)s %(message)s')

#fileHandler = logging.FileHandler(logFileName)

fileHandler = TimedRotatingFileHandler(logFileName, when='midnight', interval=1,backupCount=1000)
fileHandler.suffix = "%Y%m%d%H%M%S"

fileHandler.setFormatter(formatter)

logger.addHandler(fileHandler)

