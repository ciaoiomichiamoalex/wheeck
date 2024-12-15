# database schema backup
pg_dump -U postgres -h 127.0.0.1 -p 5432 -d postgres -n wheeck -F c -f "c:/source/wheeck/scheme/reserve/wheeck_$(date -f 'yyyy_MM_dd').dump"
