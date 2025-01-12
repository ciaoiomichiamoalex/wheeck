import wheeck
from common import get_logger
from common.constants import PATH_LOG

logger = get_logger(PATH_LOG)

if __name__ == '__main__':
    try: wheeck.run()
    except Exception: logger.exception('unhandled exception')
