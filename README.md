# wheeck

Python package which extracts delivery information from the text of PDF DDTs using regular expressions.

- `common.py`: shared functions across the project
- `querier.py`: wrapper for pyodbc functions
- `geo.py`: wrapper for openrouteservice API calls
- `overview_docs.py`: generates Excel summaries of the extracted information
- `recording_docs.py`: manages the text extraction from PDFs with one database transaction per document, so
  all the pages of a document are committed together; any runtime error rolls the document back, deletes the
  discard PDFs it created and restores its original filename in `DDTs/`, so it is re-processed automatically
  on the next run
