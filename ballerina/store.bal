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

public type Error distinct ai:MemoryError;

public type Configuration record {|
    string host = "localhost";
    string user = "sa";
    string password?;
    string database;
    int port = 1433;
    string instance?;
    mssql:Options options?;
    sql:ConnectionPool connectionPool?;
|};

type CachedMessages record {|
    readonly & ai:ChatSystemMessage systemMessage?;
    (readonly & ai:ChatInteractiveMessage)[] interactiveMessages;
|};

public isolated class ShortTermMemoryStore {
    *ai:ShortTermMemoryStore;

    private final mssql:Client dbClient;
    private final cache:Cache cache;
    private final int maxMessagesPerKey;

    public isolated function init(mssql:Client|Configuration dbClient, 
                                  int maxMessagesPerKey = 20,
                                  cache:CacheConfig cacheConfig = {capacity: 20}) returns Error? {
        if dbClient is mssql:Client {
            self.dbClient = dbClient;
        } else {
            mssql:Client|sql:Error initializedClient = new mssql:Client(...dbClient);
            if initializedClient is sql:Error {
                return error("Failed to create MSSQL client: " + initializedClient.message(), initializedClient);
            }
            self.dbClient = initializedClient;
        }
        self.maxMessagesPerKey = maxMessagesPerKey;
        self.cache = new (cacheConfig);
        return self.initializeDatabase();
    }

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

    public isolated function isFull(string key) returns boolean|Error {
        ai:ChatInteractiveMessage[] interactiveMessages = check self.getChatInteractiveMessages(key);
        return interactiveMessages.length() == self.maxMessagesPerKey;
    }

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
