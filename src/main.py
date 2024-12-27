import os
import re
from datetime import datetime

import overview_docs
import recording_docs
from common import get_logger
from config_share import *

__version__ = '1.0.0'

logger = get_logger(PATH_LOG)

if __name__ == '__main__':
    # docs: list of documents in PATH_WORKING_DIR (es. [2024_01_DDT_0001_0100.pdf, ...])
    docs = sorted(next(os.walk(PATH_WORKING_DIR), (None, None, []))[2])
    logger.info(f'DDTs dir content {docs}')
    job_begin = None

    # doc: single document from the list (es. 2024_01_DDT_0001_0100.pdf)
    for doc in docs:
        if not re.search(PATTERN_WORKING_DOC, doc):
            logger.warning(f'error on PATTERN_WORKING_DOC for {doc}... skipping doc!')
            continue

        job_begin = datetime.now()
        logger.info(f'JOB START: working on doc {doc}...')
        # working_doc: path to document with suffix '.recording' (es. c:/source/wheeck/DDTs/2024_01_DDT_0001_0100.recording.pdf)
        working_doc = f"{PATH_WORKING_DIR}/{doc.split('.')[0]}.recording.pdf"
        os.rename(f'{PATH_WORKING_DIR}/{doc}', working_doc)

        # worked_pages: total number of pages in working_doc
        # discarded_pages: total number of pages with error in working_doc
        worked_pages, discarded_pages = recording_docs.doc_scanner(working_doc, job_begin)

        logger.info(f'JOB END: worked {worked_pages} pages in {doc} [{discarded_pages} discarded pages]')
        os.rename(working_doc, f"{PATH_RECORDED_DIR}/{doc.split('.')[0]}.recorded.pdf")

    if job_begin:
        # gaps: total number of document number gaps found
        gaps = recording_docs.check_gaps()
        logger.info(f'checking gaps... {gaps} gaps found!')

        # update overview of recorded documents
        logger.info('generate overview of recorded docs...')
        overview_docs.update_overviews()
        # update summary of recorded documents year
        logger.info('generate summary of recorded docs...')
        overview_docs.update_summaries()
