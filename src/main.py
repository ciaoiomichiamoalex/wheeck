import wheeck
from core import get_logger

logger = get_logger(wheeck.PATH_LOG)

if __name__ == '__main__':
    try: wheeck.run()
    except Exception: logger.exception('unhandled exception')
