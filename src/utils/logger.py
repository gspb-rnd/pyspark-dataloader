"""
Logging utility module.
"""
import logging
from typing import Optional

def setup_logger(name: str = __name__, 
                level: int = logging.INFO, 
                log_format: Optional[str] = None) -> logging.Logger:
    """
    Set up and configure a logger.
    
    Args:
        name (str): Logger name
        level (int): Logging level
        log_format (str, optional): Custom log format
        
    Returns:
        logging.Logger: Configured logger
    """
    if log_format is None:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    formatter = logging.Formatter(log_format)
    
    console_handler.setFormatter(formatter)
    
    if not logger.handlers:
        logger.addHandler(console_handler)
    
    return logger
