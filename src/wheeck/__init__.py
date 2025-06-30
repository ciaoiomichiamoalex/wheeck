from .constants import PATH_CFG, PATH_LOG, PATH_PRJ, PATH_RES, PATH_SCHEME
from .overview_docs import generate_current, generate_overview, generate_summary
from .recording_docs import check_duplicate, check_gaps, check_similarity, discard_doc, doc_scanner, run, save_warning

__version__ = '1.0.0'
