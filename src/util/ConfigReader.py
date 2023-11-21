import yaml
from yaml.loader import SafeLoader
from src.common.base_logger import logger


class ConfigReader:
    def get_config_data(self, path):
        """
        Open Yaml files
        :param path:
        :return:
        """
        try:
            with open(path) as f:
                return yaml.load(f, Loader=SafeLoader)
        except IOError as e:
            logger.error(f'exception occured while loading {e}')
        finally:
            logger.info(f'Existing config file reader')
