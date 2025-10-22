## Overview

This module provides an MS SQL-backed short-term memory store to use with AI messages (e.g., with AI agents, model providers, etc.).

## Prerequisites

- Configuration for an MS SQL database

## Quickstart

Follow the steps below to use this store in your Ballerina application:

1. Import the `ballerinax/ai.mssql` module.

```ballerina
import ballerinax/ai.mssql;
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
    import ballerinax/ai.mssql;

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
    import ballerinax/ai.mssql as mssqlStore;

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