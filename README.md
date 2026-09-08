# Ballerina MSSQL-backed short-term chat message store connector

[![Build](https://github.com/ballerina-platform/module-ballerinax-ai.memory.mssql/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-ai.memory.mssql/actions/workflows/ci.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-ai.memory.mssql.svg)](https://github.com/ballerina-platform/module-ballerinax-ai.memory.mssql/commits/master)
[![GitHub Issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-library/module/ai.memory.mssql.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-library/labels/module%ai.memory.mssql)

## Overview

This module provides an MS SQL-backed short-term memory store to use with AI messages (e.g., with AI agents, model providers, etc.).

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

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Download and install [Docker](https://www.docker.com/get-started).

   > **Note**: Ensure that the Docker daemon is running before executing any tests.

4. Export Github Personal access token with read package permissions as follows,

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

   ```bash
   ./gradlew clean build
   ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To run tests against different environments:

   ```bash
   ./gradlew clean test -Pgroups=<Comma separated groups/test cases>
   ```

5. To debug the package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

6. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

7. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

8. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For more information go to the [`ai.memory.mssql` package](https://central.ballerina.io/ballerinax/ai.memory.mssql/latest).
* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
