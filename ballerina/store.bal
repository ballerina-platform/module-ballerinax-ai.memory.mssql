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
import ballerina/lang.value;
import ballerina/sql;
import ballerinax/mssql;

const OBJECT_EXISTS_ERROR_CODE = 2714;

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
    private final cache:Cache cache = new ({capacity: 20});
    // TODO
    private final int maximumRecordCount;

    public isolated function init(mssql:Client|Configuration dbClient, int maximumRecordCount = 20) returns Error? {
        if dbClient is mssql:Client {
            self.dbClient = dbClient;
        } else {
            mssql:Client|sql:Error initializedClient = new mssql:Client(...dbClient);
            if initializedClient is sql:Error {
                return error("Failed to create MSSQL client: " + initializedClient.message(), initializedClient);
            }
            self.dbClient = initializedClient;
        }
        self.maximumRecordCount = maximumRecordCount;
        return self.initializeDatabase();
    }

    public isolated function getChatSystemMessage(string key) returns ai:ChatSystemMessage|ai:MemoryError? {
        lock {
            if self.cache.hasKey(key) {
                return (check self.getCacheEntry(key)).systemMessage;
            }
        }

        ChatSystemMessageDatabaseMessage|sql:Error systemMessage = self.dbClient->queryRow(`
            SELECT MessageJson 
            FROM ChatMessages 
            WHERE MessageKey = ${key} AND MessageRole = 'system'
            ORDER BY CreatedAt ASC`);

        if systemMessage is sql:NoRowsError {
            return ();
        }

        if systemMessage is ChatSystemMessageDatabaseMessage {
            // We intentionally don't populate the cache when just the system message is fetched
            // to avoid having to load interactive messages, which are generally significantly more in number, as well.
            return transformFromSystemMessageDatabaseMessage(systemMessage);
        }
    
        return error("Failed to retrieve system message: " + systemMessage.message(), systemMessage);
    }

    public isolated function getChatInteractiveMessages(string key) returns ai:ChatInteractiveMessage[]|ai:MemoryError {
        lock {
            if self.cache.hasKey(key) {
                CachedMessages cachedMessages = check self.getCacheEntry(key);
                return cachedMessages.interactiveMessages.clone();
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
            returns [ai:ChatSystemMessage, ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[]|ai:MemoryError {
        lock {
            if self.cache.hasKey(key) {
                CachedMessages cachedMessages = check self.getCacheEntry(key);
                final readonly & ai:ChatSystemMessage? systemMessage = cachedMessages.systemMessage;
                if systemMessage is ai:ChatSystemMessage {
                    return [systemMessage, ...cachedMessages.interactiveMessages].clone();
                }
                return cachedMessages.interactiveMessages.clone();
            }
        }

        do {
            final var allMessages = check self.cacheFromDatabase(key);
            return allMessages;
        } on fail Error err {
            return error("Failed to retrieve chat messages: " + err.message(), err);
        }
    }

    public isolated function isFull(string key) returns boolean {
        // TODO
        return false;
    }

    public isolated function put(string key, ai:ChatMessage message) returns ai:MemoryError? {
        ChatMessageDatabaseMessage dbMessage = transformToDatabaseMessage(message);

        if dbMessage is ChatSystemMessageDatabaseMessage {
            // Remove existing system message for the key
            sql:ExecutionResult|sql:Error deleteResult = self.dbClient->execute(`
                UPDATE ChatMessages 
                SET MessageJson = ${dbMessage.toJsonString()}
                WHERE MessageKey = ${key} AND MessageRole = 'system'`);
            if deleteResult is sql:Error {
                return error("Failed to delete existing system message: " + deleteResult.message(), deleteResult);
            }
        } else {
            sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) 
                VALUES (${key}, ${dbMessage.role}, ${dbMessage.toJsonString()})`);
            if result is sql:Error {
                return error("Failed to insert chat message: " + result.message(), result);
            }
        }

        final readonly & ai:ChatMessage immutableMessage = mapToImmutableMessage(message);
        lock {
            if self.cache.hasKey(key) {
                CachedMessages cachedMessages = check self.getCacheEntry(key);
                if immutableMessage is ai:ChatSystemMessage {
                    cachedMessages.systemMessage = immutableMessage;
                } else {
                    cachedMessages.interactiveMessages.push(immutableMessage);
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

    public isolated function removeChatSystemMessage(string key) returns ai:MemoryError? {
        lock {
            if self.cache.hasKey(key) {
                CachedMessages cachedMessages = check self.getCacheEntry(key);
                if cachedMessages.hasKey("systemMessage") {
                    cachedMessages.systemMessage = ();
                }
            }
        }

        sql:ExecutionResult|sql:Error deleteResult = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE MessageKey = ${key} AND MessageRole = 'system'`);
        if deleteResult is sql:Error {
            self.removeCacheEntry(key);
            return error("Failed to delete existing system message: " + deleteResult.message(), deleteResult);
        }
    }

    public isolated function removeChatInteractiveMessages(string key, int? count = ()) returns ai:MemoryError? {
        lock {
            if self.cache.hasKey(key) {
                ai:ChatInteractiveMessage[] interactiveMessages = (check self.getCacheEntry(key)).interactiveMessages;
                if count is () || count >= interactiveMessages.length() {
                    interactiveMessages.removeAll();
                } else {
                    foreach int i in 0...count {
                        _ = interactiveMessages.shift();
                    }
                }
            }
        }

        if count is () {
            sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE MessageKey = ${key} AND MessageRole != 'system'`);
            if result is sql:Error {
                self.removeCacheEntry(key);
                return error("Failed to delete chat messages: " + result.message(), result);
            }
            return;
        }

        sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
            DELETE TOP(${count}) FROM ChatMessages 
            WHERE MessageKey = ${key} AND MessageRole != 'system'
            ORDER BY CreatedAt ASC`);
        if result is sql:Error {
            self.removeCacheEntry(key);
            return error("Failed to delete chat messages: " + result.message(), result);
        }
    }

    public isolated function removeAll(string key) returns ai:MemoryError? {
        self.removeCacheEntry(key);
        sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
                DELETE FROM ChatMessages 
                WHERE MessageKey = ${key}`);
        if result is sql:Error {
            return error("Failed to delete chat messages: " + result.message(), result);
        }
    }

    private isolated function initializeDatabase() returns Error? {
        // sql:ExecutionResult|sql:Error result = self.dbClient->execute(`
        //         IF OBJECT_ID('ChatMessages', 'U') IS NULL
        //         BEGIN
        //             CREATE TABLE ChatMessages ( 
        //                 Id INT IDENTITY(1,1) PRIMARY KEY, 
        //                 MessageKey NVARCHAR(100) NOT NULL, 
        //                 MessageRole NVARCHAR(20) NOT NULL CHECK (MessageRole IN ('user', 'system', 'assistant', 'function')), 
        //                 MessageJson NVARCHAR(MAX) NOT NULL, 
        //                 CreatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        //             );
        //         END
        //     `);

        sql:ExecutionResult|sql:Error result = self.dbClient->execute(
            `CREATE TABLE ChatMessages ( 
                Id INT IDENTITY(1,1) PRIMARY KEY, 
                MessageKey NVARCHAR(100) NOT NULL, 
                MessageRole NVARCHAR(20) NOT NULL CHECK (MessageRole IN ('user', 'system', 'assistant', 'function')), 
                MessageJson NVARCHAR(MAX) NOT NULL, 
                CreatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
            );`);
        if result is sql:Error {
            // Workaround for what seems like a connector issue.
            map<value:Cloneable> & readonly detail = result.detail();
            if detail is sql:DatabaseErrorDetail && detail.errorCode == OBJECT_EXISTS_ERROR_CODE {
                // Table already exists
                return;
            }
            return error("Failed to create ChatMessages table: " + result.message(), result);
        }
    }

    private isolated function cacheFromDatabase(string key) 
            returns readonly & ([ai:ChatSystemMessage,  ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[])|Error {
        stream<ChatMessageDatabaseMessage , sql:Error?> messages = self.dbClient->query(`
            SELECT MessageJson 
            FROM ChatMessages 
            WHERE MessageKey = ${key}
            ORDER BY CreatedAt ASC`);
        do {
            (ai:ChatSystemMessage & readonly)? systemMessage = ();
            (ai:ChatInteractiveMessage & readonly)[] interactiveMessages = [];

            check from ChatMessageDatabaseMessage dbMessage in messages
            do {
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
                        key, {systemMessage, interactiveMessages: [...immutableInteractiveMessages]});
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

    private isolated function getCacheEntry(string key) returns CachedMessages|Error {
        lock {
            do {
                any cacheEntry = check self.cache.get(key);
                return check cacheEntry.ensureType();
            } on fail error err {
                return error("Failed to retrieve cache entry: " + err.message(), err);
            }
        }
    }
}
