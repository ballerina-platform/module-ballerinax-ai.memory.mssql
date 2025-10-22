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
import ballerina/sql;
import ballerinax/mssql;
import ballerinax/mssql.driver as _;

# Represents a distinct error type for memory store errors.
public type Error distinct ai:MemoryError;

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
    private final cache:Cache cache;
    private final int maxMessagesPerKey;

    # Initializes the MS SQL-backed short-term memory store.
    # 
    # + mssqlClient - The MS SQL client or database configuration to connect to the database
    # + maxMessagesPerKey - The maximum number of interactive messages to store per key
    # + cacheConfig - The cache configuration for in-memory caching of messages
    # + returns - An error if the initialization fails
    public isolated function init(mssql:Client|DatabaseConfiguration mssqlClient, 
                                  int maxMessagesPerKey = 20,
                                  cache:CacheConfig cacheConfig = {capacity: 20}) returns Error? {
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
        self.cache = new (cacheConfig);
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

        DatabaseRecord|sql:Error systemMessage = self.dbClient->queryRow(`
            SELECT MessageJson 
            FROM ChatMessages 
            WHERE MessageKey = ${key} AND MessageRole = 'system'
            ORDER BY CreatedAt ASC`);

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

    # Adds a chat message to the memory store for a given key.
    # 
    # + key - The key associated with the memory
    # + message - The `ChatMessage` message to store
    # + return - nil on success, or an `Error` if the operation fails
    public isolated function put(string key, ai:ChatMessage message) returns Error? {
        ChatMessageDatabaseMessage dbMessage = transformToDatabaseMessage(message);

        if dbMessage is ChatSystemMessageDatabaseMessage {
            // Upsert system message for the key
            sql:ExecutionResult|sql:Error upsertResult = self.dbClient->execute(`
                IF EXISTS (SELECT 1 FROM ChatMessages WHERE MessageKey = ${key} AND MessageRole = 'system')
                    UPDATE ChatMessages SET MessageJson = ${dbMessage.toJsonString()}
                    WHERE MessageKey = ${key} AND MessageRole = 'system'
                ELSE
                    INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) 
                    VALUES (${key}, ${dbMessage.role}, ${dbMessage.toJsonString()})`);
            if upsertResult is sql:Error {
                return error("Failed to upsert system message: " + upsertResult.message(), upsertResult);
            }
        } else {
            // Expected to be checked by the caller, but doing an additional check here, without locking.
            ai:ChatInteractiveMessage[] chatInteractiveMessages = check self.getChatInteractiveMessages(key);
            if chatInteractiveMessages.length() >= self.maxMessagesPerKey {
                return error(string `Cannot add more messages. Maximum limit of '${
                    self.maxMessagesPerKey}' reached for key: '${key}'`);
            }

            sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) 
                VALUES (${key}, ${dbMessage.role}, ${dbMessage.toJsonString()})`);
            if result is sql:Error {
                return error("Failed to insert chat message: " + result.message(), result);
            }
        }

        final readonly & ai:ChatMessage immutableMessage = mapToImmutableMessage(message);
        lock {
            CachedMessages? cacheEntry = self.getCacheEntry(key);
            if cacheEntry is CachedMessages {
                if immutableMessage is ai:ChatSystemMessage {
                    cacheEntry.systemMessage = immutableMessage;
                } else {
                    cacheEntry.interactiveMessages.push(immutableMessage);
                }
                return;
            }
            do {
                if immutableMessage is ai:ChatSystemMessage {
                    check self.cache.put(key, <CachedMessages> {systemMessage: immutableMessage, interactiveMessages: []});
                } else {
                    check self.cache.put(key, <CachedMessages> {interactiveMessages: [immutableMessage]});
                }
            } on fail {
                self.removeCacheEntry(key);
            }
        }
    }

    # Removes the system chat message, if specified, for a given key.
    # 
    # + key - The key associated with the memory
    # + return - nil on success or if there is no system chat message against the key, 
    #       or an `Error` error if the operation fails
    public isolated function removeChatSystemMessage(string key) returns Error? {
        sql:ExecutionResult|sql:Error deleteResult = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE MessageKey = ${key} AND MessageRole = 'system'`);
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
    #               if not provided, removes all messages
    # + return - nil on success, or an `Error` error if the operation fails
    public isolated function removeChatInteractiveMessages(string key, int? count = ()) returns Error? {
        if count is () {
            sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE MessageKey = ${key} AND MessageRole != 'system'`);
            if result is sql:Error {
                self.removeCacheEntry(key);
                return error("Failed to delete chat messages: " + result.message(), result);
            }
        } else {
            sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE Id IN (
                    SELECT TOP(${count}) Id 
                    FROM ChatMessages 
                    WHERE MessageKey = ${key} AND MessageRole != 'system'
                    ORDER BY CreatedAt ASC
                )`);
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
        sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE MessageKey = ${key}`);
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
        ai:ChatInteractiveMessage[] interactiveMessages = check self.getChatInteractiveMessages(key);
        return interactiveMessages.length() == self.maxMessagesPerKey;
    }

    private isolated function initializeDatabase() returns Error? {
        int|sql:Error tableExists = self.dbClient->queryRow(
            `SELECT IIF(OBJECT_ID('dbo.ChatMessages', 'U') IS NOT NULL, 1, 0) AS TableExists;`);

        if tableExists is sql:Error {
            return error("Failed to check existence of the ChatMessages table: " + tableExists.message(), 
                            tableExists);
        }

        if tableExists == 1 {
            return;
        }

        sql:ExecutionResult|sql:Error result = self.dbClient->execute(
            `CREATE TABLE ChatMessages ( 
                Id INT IDENTITY(1,1) PRIMARY KEY, 
                MessageKey NVARCHAR(100) NOT NULL, 
                MessageRole NVARCHAR(20) NOT NULL CHECK (MessageRole IN ('user', 'system', 'assistant', 'function')), 
                MessageJson NVARCHAR(MAX) NOT NULL, 
                CreatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
            );`);
        if result is sql:Error {
            return error("Failed to create ChatMessages table: " + result.message(), result);
        }
    }

    private isolated function cacheFromDatabase(string key) 
            returns readonly & ([ai:ChatSystemMessage,  ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[])|Error {
        stream<DatabaseRecord, sql:Error?> messages = self.dbClient->query(`
            SELECT MessageJson 
            FROM ChatMessages 
            WHERE MessageKey = ${key}
            ORDER BY CreatedAt ASC`);
        do {
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
                        <ChatInteractiveMessageDatabaseMessage> dbMessage));
                }
            };

            final ai:ChatInteractiveMessage[] & readonly immutableInteractiveMessages = interactiveMessages.cloneReadOnly();
            lock {
                if !self.cache.hasKey(key) {
                    check self.cache.put(
                        key, <CachedMessages> {systemMessage, interactiveMessages: [...immutableInteractiveMessages]});
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
            if self.cache.hasKey(key) {
                cache:Error? err = self.cache.invalidate(key);
                if err is cache:Error {
                    // Ignore, as this is for non-existent key
                }
            }
        }
    }

    private isolated function getCacheEntry(string key) returns CachedMessages? {
        lock {
            if !self.cache.hasKey(key) {
                return ();
            }

            any|cache:Error cacheEntry = self.cache.get(key);
            if cacheEntry is cache:Error {
                return ();
            }

            // Since we have sole control over what is stored in the cache, this cast is safe.
            return checkpanic cacheEntry.ensureType();
        }
    }
}
