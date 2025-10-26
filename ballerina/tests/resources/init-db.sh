#!/bin/bash

/opt/mssql/bin/sqlservr &

sleep 15

/opt/mssql-tools18/bin/sqlcmd -S mssql -U sa -P Test-1234# -C -Q 'CREATE DATABASE message_db'

wait
