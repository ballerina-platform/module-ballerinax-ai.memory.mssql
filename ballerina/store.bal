// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/ai;
import ballerina/cache;
import ballerina/lang.regexp;
import ballerina/sql;
import ballerinax/mssql;
import ballerinax/mssql.driver as _;

# Represents a distinct error type for memory store errors.
public type Error distinct ai:MemoryError;

type ExceedsSizeError distinct Error;

# Database configuration for MS SQL client.
public type DatabaseConfiguration record {|
    # Database host
    string host = "localhost";
    # Database user
    string user = "sa";
    # Database password
    string password?;
    # Database name
    string database;
    # Database port
    int port = 1433;
    # Instance name
    string instance?;
    # Additional options for the MS SQL client
    mssql:Options options?;
    # Connection pool configuration
    sql:ConnectionPool connectionPool?;
|};

type CachedMessages record {|
    readonly & ai:ChatSystemMessage systemMessage?;
    (readonly & ai:ChatInteractiveMessage)[] interactiveMessages;
|};

# Represents an MS SQL-backed short-term memory store for messages.
public isolated class ShortTermMemoryStore {
    *ai:ShortTermMemoryStore;

    private final mssql:Client dbClient;
    private final cache:Cache? cache;
    private final int maxMessagesPerKey;
    private final string tableName;

    # Initializes the MS SQL-backed short-term memory store.
    #
    # + mssqlClient - The MS SQL client or database configuration to connect to the database
    # + maxMessagesPerKey - The maximum number of interactive messages to store per key
    # + cacheConfig - The cache configuration for in-memory caching of messages
    # + tableName - The name of the database table to store chat messages (default: "ChatMessages"). 
    # Must start with a letter or underscore and contain only letters, digits, and underscores.
    # + returns - An error if the initialization fails
    public isolated function init(mssql:Client|DatabaseConfiguration mssqlClient,
            int maxMessagesPerKey = 20,
            cache:CacheConfig? cacheConfig = (),
            string tableName = "ChatMessages") returns Error? {
        if !regexp:isFullMatch(re `^[A-Za-z_][A-Za-z0-9_]*$`, tableName) {
            return error(string `Invalid table name: '${tableName}'.`
                + " Table name must start with a letter or underscore, "
                + "and can only contain letters, digits, and underscores.");
        }
        self.tableName = tableName;
        if mssqlClient is mssql:Client {
            self.dbClient = mssqlClient;
        } else {
            mssql:Client|sql:Error initializedClient = new mssql:Client(...mssqlClient);
            if initializedClient is sql:Error {
                return error("Failed to create MSSQL client: " + initializedClient.message(), initializedClient);
            }
            self.dbClient = initializedClient;
        }
        self.maxMessagesPerKey = maxMessagesPerKey;
        self.cache = cacheConfig is () ? () : new (cacheConfig);
        return self.initializeDatabase();
    }

    # Retrieves the system message, if it was provided, for a given key.
    #
    # + key - The key associated with the memory
    # + return - A copy of the message if it was specified, nil if it was not, or an 
    # `Error` error if the operation fails
    public isolated function getChatSystemMessage(string key) returns ai:ChatSystemMessage|Error? {
        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is CachedMessages {
                return cacheEntry.systemMessage;
            }
        }

        DatabaseRecord|sql:Error systemMessage = self.dbClient->queryRow(
            replaceTableNamePlaceholder(`
                SELECT MessageJson 
                FROM $_tableName_$
                WHERE MessageKey = ${key} AND MessageRole = 'system'
                ORDER BY CreatedAt ASC`,
                self.tableName
            )
        );

        if systemMessage is sql:NoRowsError {
            return ();
        }

        if systemMessage is sql:Error {
            return error("Failed to retrieve system message: " + systemMessage.message(), systemMessage);
        }

        ChatSystemMessageDatabaseMessage|error dbMessage = systemMessage.MessageJson.fromJsonStringWithType();
        if dbMessage is error {
            return error("Failed to parse chat message from database: " + dbMessage.message(), dbMessage);
        }

        // We intentionally don't populate the cache when just the system message is fetched
        // to avoid having to load interactive messages, which are generally significantly more in number, as well.
        return transformFromSystemMessageDatabaseMessage(dbMessage);
    }

    # Retrieves all stored interactive chat messages (i.e., all chat messages except the system
    # message) for a given key.
    #
    # + key - The key associated with the memory
    # + return - A copy of the messages, or an `Error` error if the operation fails
    public isolated function getChatInteractiveMessages(string key) returns ai:ChatInteractiveMessage[]|Error {
        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is CachedMessages {
                return cacheEntry.interactiveMessages.clone();
            }
        }

        do {
            final var allMessages = check self.cacheFromDatabase(key);
            if allMessages is readonly & ai:ChatInteractiveMessage[] {
                return allMessages;
            }
            var [_, ...interactiveMessages] = allMessages;
            return interactiveMessages;
        } on fail Error err {
            return error("Failed to retrieve chat messages: " + err.message(), err);
        }
    }

    # Retrieves all stored chat messages for a given key.
    #
    # + key - The key associated with the memory
    # + return - A copy of the messages, or an `Error` error if the operation fails
    public isolated function getAll(string key)
            returns [ai:ChatSystemMessage, ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[]|Error {
        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is CachedMessages {
                final readonly & ai:ChatSystemMessage? systemMessage = cacheEntry.systemMessage;
                if systemMessage is ai:ChatSystemMessage {
                    return [systemMessage, ...cacheEntry.interactiveMessages].clone();
                }
                return cacheEntry.interactiveMessages.clone();
            }
        }

        do {
            final var allMessages = check self.cacheFromDatabase(key);
            return allMessages;
        } on fail Error err {
            return error("Failed to retrieve chat messages: " + err.message(), err);
        }
    }

    # Adds one or more chat messages to the memory store for a given key.
    #
    # + key - The key associated with the memory
    # + message - The `ChatMessage` message or messages to store
    # + return - nil on success, or an `Error` if the operation fails
    public isolated function put(string key, ai:ChatMessage|ai:ChatMessage[] message) returns Error? {
        if message is ai:ChatMessage[] {
            return self.putAll(key, message);
        }
        ChatMessageDatabaseMessage dbMessage = transformToDatabaseMessage(message);

        if dbMessage is ChatSystemMessageDatabaseMessage {
            // Upsert system message for the key
            sql:ExecutionResult|sql:Error upsertResult = self.dbClient->execute(
                replaceTableNamePlaceholder(`
                    IF EXISTS (SELECT 1 FROM $_tableName_$ WHERE MessageKey = ${key} AND MessageRole = 'system')
                        UPDATE $_tableName_$ SET MessageJson = ${dbMessage.toJsonString()}
                        WHERE MessageKey = ${key} AND MessageRole = 'system'
                    ELSE
                        INSERT INTO $_tableName_$ (MessageKey, MessageRole, MessageJson) 
                        VALUES (${key}, ${dbMessage.role}, ${dbMessage.toJsonString()})`,
                    self.tableName
                )
            );
            if upsertResult is sql:Error {
                return error("Failed to upsert system message: " + upsertResult.message(), upsertResult);
            }
        } else {
            do {
                _ = check self.dbClient->execute(
                    replaceTableNamePlaceholder(`
                        INSERT INTO $_tableName_$ (MessageKey, MessageRole, MessageJson) 
                        VALUES (${key}, ${dbMessage.role}, ${dbMessage.toJsonString()})`,
                        self.tableName
                    )
                );
            } on fail error err {
                return error("Failed to add chat message: " + err.message(), err);
            }
        }

        final readonly & ai:ChatMessage immutableMessage = mapToImmutableMessage(message);
        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is () {
                return;
            }
            if immutableMessage is ai:ChatSystemMessage {
                cacheEntry.systemMessage = immutableMessage;
            } else {
                cacheEntry.interactiveMessages.push(immutableMessage);
            }
        }
        return;
    }

    private isolated function putAll(string key, ai:ChatMessage[] messages) returns Error? {
        if messages.length() == 0 {
            return;
        }

        // Separate system and interactive messages
        final ai:ChatSystemMessage[] systemMsgs = [];
        final ai:ChatInteractiveMessage[] interactiveMsgs = [];

        foreach ai:ChatMessage msg in messages {
            if msg is ai:ChatSystemMessage {
                systemMsgs.push(msg);
            } else if msg is ai:ChatInteractiveMessage {
                interactiveMsgs.push(msg);
            }
        }

        if systemMsgs.length() > 0 {
            // Only the last system message is used
            ai:ChatSystemMessage lastSystem = systemMsgs[systemMsgs.length() - 1];
            ChatMessageDatabaseMessage dbMessage = transformToDatabaseMessage(lastSystem);

            sql:ExecutionResult|sql:Error upsertResult = self.dbClient->execute(
                replaceTableNamePlaceholder(`
                    IF EXISTS (SELECT 1 FROM $_tableName_$ WHERE MessageKey = ${key} AND MessageRole = 'system')
                        UPDATE $_tableName_$ SET MessageJson = ${dbMessage.toJsonString()}
                        WHERE MessageKey = ${key} AND MessageRole = 'system'
                    ELSE
                        INSERT INTO $_tableName_$ (MessageKey, MessageRole, MessageJson) 
                        VALUES (${key}, ${dbMessage.role}, ${dbMessage.toJsonString()})`,
                    self.tableName
                )
            );
            if upsertResult is sql:Error {
                return error("Failed to upsert system message: " + upsertResult.message(), upsertResult);
            }
        }

        // Insert interactive messages in batch
        if interactiveMsgs.length() > 0 {

            // Fetch current interactive count
            ai:ChatInteractiveMessage[] chatMsgs = check self.getChatInteractiveMessages(key);
            int currentCount = chatMsgs.length();
            int incoming = interactiveMsgs.length();

            if currentCount + incoming > self.maxMessagesPerKey {
                return error(string `Cannot add more messages.`
                    + string ` Maximum limit '${self.maxMessagesPerKey}' exceeded for key '${key}'`);
            }

            sql:ParameterizedQuery[] insertQueries = from ai:ChatInteractiveMessage msg in interactiveMsgs
                let ChatMessageDatabaseMessage dbMsg = transformToDatabaseMessage(msg)
                select replaceTableNamePlaceholder(`
                        INSERT INTO $_tableName_$ (MessageKey, MessageRole, MessageJson) 
                        VALUES (${key}, ${msg.role}, ${dbMsg.toJsonString()})`,
                        self.tableName
                    );

            sql:ExecutionResult[]|sql:Error batchResult = self.dbClient->batchExecute(insertQueries);
            if batchResult is sql:Error {
                return error("Failed batch insert of interactive messages: " + batchResult.message(), batchResult);
            }
        }

        final readonly & ai:ChatSystemMessage? chatSystemMessage;
        if systemMsgs.length() > 0 {
            ai:ChatSystemMessage lastSystem = systemMsgs[systemMsgs.length() - 1];
            final readonly & ai:ChatMessage immutableMessage = mapToImmutableMessage(lastSystem);
            if immutableMessage is ai:ChatSystemMessage {
                chatSystemMessage = immutableMessage;
            } else {
                chatSystemMessage = ();
            }
        } else {
            chatSystemMessage = ();
        }

        final ai:ChatMessage[] & readonly immutableMessages = interactiveMsgs.'map(msg => mapToImmutableMessage(msg))
            .cloneReadOnly();

        // Update cache
        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is () {
                return;
            }

            if chatSystemMessage is ai:ChatSystemMessage {
                cacheEntry.systemMessage = chatSystemMessage;
            }

            foreach ai:ChatMessage msg in immutableMessages {
                if msg is ai:ChatInteractiveMessage {
                    cacheEntry.interactiveMessages.push(msg);
                }
            }
        }
        return;
    }

    # Removes the system chat message, if specified, for a given key.
    #
    # + key - The key associated with the memory
    # + return - nil on success or if there is no system chat message against the key, 
    # or an `Error` error if the operation fails
    public isolated function removeChatSystemMessage(string key) returns Error? {
        sql:ExecutionResult|sql:Error deleteResult = self.dbClient->execute(
            replaceTableNamePlaceholder(`
                DELETE FROM $_tableName_$ 
                WHERE MessageKey = ${key} AND MessageRole = 'system'`,
                self.tableName
            )
        );
        if deleteResult is sql:Error {
            self.removeCacheEntry(key);
            return error("Failed to delete existing system message: " + deleteResult.message(), deleteResult);
        }

        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is CachedMessages {
                if cacheEntry.hasKey("systemMessage") {
                    cacheEntry.systemMessage = ();
                }
            }
        }
    }

    # Removes all stored interactive chat messages (i.e., all chat messages except the system
    # message) for a given key.
    #
    # + key - The key associated with the memory
    # + count - Optional number of messages to remove, starting from the first interactive message in; 
    # if not provided, removes all messages
    # + return - nil on success, or an `Error` error if the operation fails
    public isolated function removeChatInteractiveMessages(string key, int? count = ()) returns Error? {
        if count is () {
            sql:ExecutionResult|sql:Error result = self.dbClient->execute(
                replaceTableNamePlaceholder(`
                    DELETE FROM $_tableName_$ 
                    WHERE MessageKey = ${key} AND MessageRole != 'system'`,
                    self.tableName
                )
            );
            if result is sql:Error {
                self.removeCacheEntry(key);
                return error("Failed to delete chat messages: " + result.message(), result);
            }
        } else {
            sql:ExecutionResult|sql:Error result = self.dbClient->execute(
                replaceTableNamePlaceholder(`
                    DELETE FROM $_tableName_$ 
                    WHERE Id IN (
                        SELECT TOP(${count}) Id 
                        FROM $_tableName_$ 
                        WHERE MessageKey = ${key} AND MessageRole != 'system'
                        ORDER BY CreatedAt ASC
                    )`, self.tableName
                )
            );
            if result is sql:Error {
                self.removeCacheEntry(key);
                return error("Failed to delete chat messages: " + result.message(), result);
            }
        }

        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is CachedMessages {
                ai:ChatInteractiveMessage[] interactiveMessages = cacheEntry.interactiveMessages;
                if count is () || count >= interactiveMessages.length() {
                    interactiveMessages.removeAll();
                } else {
                    foreach int i in 0 ..< count {
                        _ = interactiveMessages.shift();
                    }
                }
            }
        }
    }

    # Removes all stored chat messages for a given key.
    #
    # + key - The key associated with the memory
    # + return - nil on success, or an `Error` error if the operation fails
    public isolated function removeAll(string key) returns Error? {
        sql:ExecutionResult|sql:Error result = self.dbClient->execute(
            replaceTableNamePlaceholder(`
                DELETE FROM $_tableName_$ 
                WHERE MessageKey = ${key}`,
                self.tableName
            )
        );
        if result is sql:Error {
            self.removeCacheEntry(key);
            return error("Failed to delete chat messages: " + result.message(), result);
        }
        self.removeCacheEntry(key);
    }

    # Checks if the memory store is full for a given key.
    #
    # + key - The key associated with the memory
    # + return - true if the memory store is full, false otherwise, or an `Error` error if the operation fails
    public isolated function isFull(string key) returns boolean|Error {
        ai:ChatInteractiveMessage[]|Error interactiveMessages = self.getChatInteractiveMessages(key);

        if interactiveMessages is Error {
            error? cause = interactiveMessages.cause();
            if cause is ExceedsSizeError {
                return true;
            }
            return interactiveMessages;
        }

        return interactiveMessages.length() >= self.maxMessagesPerKey;
    }

    private isolated function initializeDatabase() returns Error? {
        int|sql:Error tableExists = self.dbClient->queryRow(
            replaceTableNamePlaceholder(
                `SELECT IIF(OBJECT_ID('dbo.$_tableName_$', 'U') IS NOT NULL, 1, 0) AS TableExists;`,
                self.tableName
            )
        );

        if tableExists is sql:Error {
            return error(string `Failed to check existence of the ${self.tableName} table: ${tableExists.message()}`,
                            tableExists);
        }

        if tableExists == 1 {
            return;
        }

        sql:ExecutionResult|sql:Error result = self.dbClient->execute(
            replaceTableNamePlaceholder(
                `CREATE TABLE $_tableName_$ ( 
                    Id INT IDENTITY(1,1) PRIMARY KEY, 
                    MessageKey NVARCHAR(100) NOT NULL, 
                    MessageRole NVARCHAR(20) NOT NULL CHECK (MessageRole IN ('user', 'system', 'assistant', 'function')), 
                    MessageJson NVARCHAR(MAX) NOT NULL, 
                    CreatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
                );`,
                self.tableName
                )
            );
        if result is sql:Error {
            return error(string `Failed to create ${self.tableName} table: ${result.message()}`, result);
        }
    }

    private isolated function cacheFromDatabase(string key)
            returns readonly & ([ai:ChatSystemMessage, ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[])|Error {
        do {
            stream<DatabaseRecord, sql:Error?> messages = self.dbClient->query(
                replaceTableNamePlaceholder(`
                    SELECT MessageJson 
                    FROM $_tableName_$ 
                    WHERE MessageKey = ${key}
                    ORDER BY CreatedAt ASC`, self.tableName
                )
            );
            (ai:ChatSystemMessage & readonly)? systemMessage = ();
            (ai:ChatInteractiveMessage & readonly)[] interactiveMessages = [];

            check from DatabaseRecord {MessageJson} in messages
                do {
                    ChatMessageDatabaseMessage|error dbMessage = MessageJson.fromJsonStringWithType();
                    if dbMessage is error {
                        return error("Failed to parse chat message from database: " + dbMessage.message(), dbMessage);
                    }

                    if dbMessage is ChatSystemMessageDatabaseMessage {
                        systemMessage = transformFromSystemMessageDatabaseMessage(dbMessage);
                    } else {
                        interactiveMessages.push(transformFromInteractiveMessageDatabaseMessage(
                                <ChatInteractiveMessageDatabaseMessage>dbMessage));
                    }
                };

            final ai:ChatInteractiveMessage[] & readonly immutableInteractiveMessages = interactiveMessages.cloneReadOnly();
            lock {
                cache:Cache? cache = self.cache;
                if cache !is () && !cache.hasKey(key) {
                    check cache.put(
                        key, <CachedMessages>{systemMessage, interactiveMessages: [...immutableInteractiveMessages]});
                }
            }

            if systemMessage is () {
                return immutableInteractiveMessages;
            }
            return [systemMessage, ...interactiveMessages];
        } on fail error err {
            return error("Failed to retrieve chat messages: " + err.message(), err);
        }
    }

    private isolated function removeCacheEntry(string key) {
        lock {
            cache:Cache? cache = self.cache;
            if cache !is () && cache.hasKey(key) {
                cache:Error? err = cache.invalidate(key);
                if err is cache:Error {
                    // Ignore, as this is for non-existent key
                }
            }
        }
    }

    private isolated function getCacheEntry(string key) returns CachedMessages? {
        lock {
            cache:Cache? cache = self.cache;
            if cache is () || !cache.hasKey(key) {
                return ();
            }

            any|cache:Error cacheEntry = cache.get(key);
            if cacheEntry is cache:Error {
                return ();
            }

            // Since we have sole control over what is stored in the cache, this use of
            // `checkpanic` is safe.
            return checkpanic cacheEntry.ensureType();
        }
    }

    # Retrieves the maximum number of interactive messages that can be stored for each key.
    #
    # + return - The configured capacity of the message store per key
    public isolated function getCapacity() returns int {
        return self.maxMessagesPerKey;
    }
}

isolated function replaceTableNamePlaceholder(sql:ParameterizedQuery query, string tableName) returns sql:ParameterizedQuery {
    final (string[] & readonly) strings = query.strings
        .'map(value => re `\$_tableName_\$`.replaceAll(value, tableName)).cloneReadOnly();
    query.strings = strings;
    return query;
}
