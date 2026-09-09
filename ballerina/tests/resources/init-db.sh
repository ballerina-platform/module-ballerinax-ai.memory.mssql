#!/bin/bash

/opt/mssql/bin/sqlservr &

sleep 15

/opt/mssql-tools18/bin/sqlcmd -S mssql -U sa -P Test-1234# -C -Q 'CREATE DATABASE message_db'

/opt/mssql-tools18/bin/sqlcmd -S mssql -U sa -P Test-1234# -C -b -Q "CREATE LOGIN dml_user WITH PASSWORD = 'Dml-P@ss123!'"

/opt/mssql-tools18/bin/sqlcmd -S mssql -U sa -P Test-1234# -C -b -d message_db -Q "
CREATE USER dml_user FOR LOGIN dml_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON DATABASE::message_db TO dml_user;"

wait
