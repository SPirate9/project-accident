import logging
import os

def setup_logger(name):
    if not os.path.exists("logs"):
        os.makedirs("logs")
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        file_handler = logging.FileHandler(f"logs/{name}.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger