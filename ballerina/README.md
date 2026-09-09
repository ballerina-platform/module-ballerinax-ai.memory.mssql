## Overview

This module provides an MS SQL-backed short-term memory store to use with AI messages (e.g., with AI agents, model providers, etc.).

### Key Features

- MS SQL-backed persistent storage for short-term AI message memory
- Configurable maximum messages per key with automatic eviction
- Built-in in-memory caching for improved read performance
- Support for both direct database configuration and existing MSSQL client reuse

## Prerequisites

- Configuration for an MS SQL database
- The database tables described below

### Database tables

This store uses two tables, one for the chat history and one for human-in-the-loop pause state. You name them with the `tableName` and `checkpointTableName` parameters when creating the store. Production deployments are expected to provision both up front, typically by a DBA, rather than relying on the application to create them.

The chat history table is created at initialization if it does not already exist, which is convenient for development, but in production create it beforehand. Use the name you pass as `tableName`, which defaults to `ChatMessages`:

```sql
CREATE TABLE ChatMessages (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    MessageKey NVARCHAR(100) NOT NULL,
    MessageRole NVARCHAR(20) NOT NULL CHECK (MessageRole IN ('user', 'system', 'assistant', 'function')),
    MessageJson NVARCHAR(MAX) NOT NULL,
    CreatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
```

The checkpoint table holds human-in-the-loop pause state. The store never creates it, so it must exist before an agent with approval-gated tools runs. A deployment that does not use human-in-the-loop does not need it at all. Use the name you pass as `checkpointTableName`, which defaults to `Checkpoints`:

```sql
CREATE TABLE Checkpoints (
    SessionId NVARCHAR(100) NOT NULL PRIMARY KEY,
    ApprovalJson NVARCHAR(MAX) NOT NULL,
    UpdatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
);
```

## Quickstart

Follow the steps below to use this store in your Ballerina application:

1. Import the `ballerinax/ai.memory.mssql` module.

```ballerina
import ballerinax/ai.memory.mssql;
```

Optionally, import the `ballerina/ai` and/or `ballerinax/mssql` module(s).

```ballerina
import ballerina/ai;
import ballerinax/mssql;
```

2. Create the short-term memory store, by passing either the configuration for the database or an `mssql:Client` client.

    i. Using the configuration 

    ```ballerina
    import ballerina/ai;
    import ballerinax/ai.memory.mssql;

    configurable string host = ?;
    configurable string user = ?;
    configurable string password = ?;
    configurable string database = ?;

    ai:ShortTermMemoryStore store = check new mssql:ShortTermMemoryStore({
        host, user, password, database
    });
    ```

    ii. Using an `mssql:Client` client

    ```ballerina
    import ballerina/ai;
    import ballerinax/mssql;
    import ballerinax/ai.memory.mssql as mssqlStore;

    configurable string host = ?;
    configurable string user = ?;
    configurable string password = ?;
    configurable string database = ?;

    mssql:Client mssqlClient = check new (host, user, password, database);   
    ai:ShortTermMemoryStore store = check new mssqlStore:ShortTermMemoryStore(mssqlClient);
    ```

    Optionally, specify the maximum number of messages to store per key (`maxMessagesPerKey` - defaults to `20`) and/or the configuration for the in-memory cache for messages (`cacheConfig` - defaults to a capacity of `20`).

    ```ballerina
    ai:ShortTermMemoryStore store = check new mssql:ShortTermMemoryStore({
        host, user, password, database
    }, 10, {capacity: 10});
    ```
