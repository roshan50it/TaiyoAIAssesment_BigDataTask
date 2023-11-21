import logging

logger = logging
logger.basicConfig(level=logging.INFO,
                   format='%(asctime)s '
                          '- %(name)s '
                          '- %(levelname)s '
                          '- %(funcName)s:%(lineno)d '
                          '-%(message)s')