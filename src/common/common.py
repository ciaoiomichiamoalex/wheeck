import json
import logging


def decode_json(json_in: str, single: bool = True, **kwargs) -> dict | list | None:
    """
    Read a json file and return the objects which verify the conditions in input.

    :param json_in: The path to the json file.
    :type json_in: str
    :param single: Indicates if returns only the first object or the entire list.
    :type single: bool
    :param kwargs: The attribute conditions to be verified as key=value.
    :return: A list of matching objects in the file or a single object, or None if no one match.
    :rtype: dict | list | None
    """
    with open(json_in, encoding='utf-8') as jin:
        res = list(json.load(jin))

    res = [obj for obj in res
           if all(obj.get(key) == value for key, value in kwargs.items())]
    return res[0] if res and single else res if res else None


def get_logger(fou: str, name: str = 'main', level: str = 'INFO', console: bool = True) -> logging.Logger:
    """
    Initialize a new logger object with custom properties, by creating it if it doesn't already exist.

    :param fou: The path to the log file.
    :type fou: str
    :param name: The name of the logger, defaults to 'main'.
    :type name: str
    :param level: The logging level on file, defaults to INFO.
    :type level: str
    :param console: Enable or disable logging errors also on console, defaults to True.
    :type console: bool
    :return: The logger object.
    :rtype: Logger
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        fou_handler = logging.FileHandler(fou)
        fou_handler.setLevel(level)
        fou_handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s - %(message)s',
            '%d/%m/%Y %H:%M:%S'
        ))
        logger.addHandler(fou_handler)

        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.ERROR)
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s [%(name)s] %(levelname)s - %(message)s',
                '%d/%m/%Y %H:%M:%S'
            ))
            logger.addHandler(console_handler)
    return logger
