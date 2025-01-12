# wheeck

wheeck is a simple project that through regular expressions extracts delivery information from the text of PDF DDTs and stores it in a database.

To backup database schema:

```bash
pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n wheeck -F c -f "c:/source/wheeck/scheme/reserve/wheeck_$(date -f 'yyyy_MM_dd').dump"
```
