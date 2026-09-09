// Copyright (c) 2025 WSO2 LLC (http://www.wso2.com).
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
import ballerina/test;
import ballerinax/mssql;

const string K1 = "key1";
const string K2 = "key2";
const string K3 = "key3";

const ai:ChatSystemMessage K1SM1 = {role: ai:SYSTEM, content: "You are a helpful assistant that is aware of the weather."};

const ai:ChatUserMessage K1M1 = {role: ai:USER, content: "Hello, my name is Alice. I'm from Seattle."};
final readonly & ai:ChatAssistantMessage k1m2 = {role: ai:ASSISTANT, content: "Hello Alice, what can I do for you?"};
const ai:ChatUserMessage K1M3 = {role: ai:USER, content: "I would like to know the weather today."};
final readonly & ai:ChatAssistantMessage K1M4 = {
    role: ai:ASSISTANT,
    content: "The weather in Seattle today is mostly cloudy with occasional showers and a high around 58°F."
};

const ai:ChatUserMessage K2M1 = {role: ai:USER, content: "Hello, my name is Bob."};

isolated mssql:Client? modCl = ();

@test:BeforeSuite
function initClient() returns error? {
    lock {
        modCl = check new (database = "message_db", password = "Test-1234#");
    }
}

function getClient() returns mssql:Client {
    lock {
        return <mssql:Client>modCl;
    }
}

function dropTable() returns error? {
    mssql:Client cl = getClient();
    int tableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.ChatMessages', 'U') IS NOT NULL, 1, 0) AS TableExists;`);

    if tableExists == 1 {
        _ = check cl->execute(`DROP TABLE dbo.ChatMessages;`);
    }

    int checkpointTableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.Checkpoints', 'U') IS NOT NULL, 1, 0) AS TableExists;`);

    if checkpointTableExists == 1 {
        _ = check cl->execute(`DROP TABLE dbo.Checkpoints;`);
    }
}

// The checkpoint table schema. The store never creates this table; a deployment provisions it, so
// tests that exercise checkpoint operations must stand in for that deployment.
final sql:ParameterizedQuery createCheckpointTableQuery = `
    CREATE TABLE Checkpoints (
        SessionId NVARCHAR(100) NOT NULL PRIMARY KEY,
        ApprovalJson NVARCHAR(MAX) NOT NULL,
        UpdatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
    );`;

// `before` hook for tests that exercise checkpoint operations: clean slate, plus the
// deployment-provisioned checkpoint table those operations require.
function dropTableAndProvisionCheckpoints() returns error? {
    check dropTable();
    mssql:Client cl = getClient();
    _ = check cl->execute(createCheckpointTableQuery);
}

@test:Config {
    before: dropTable
}
function testBasicStore() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1SM1, K1M1, k1m2]);

    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDatabase(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeAll(K1);

    check assertFromDatabase(cl, K1, [], SYSTEM);
    check assertFromDatabase(cl, K1, [], INTERACTIVE);
    check assertFromDatabase(cl, K1, []);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDatabase(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    // Add more messages to K1 after deletion.
    check store.put(K1, K1M3);

    check assertFromDatabase(cl, K1, [], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M3], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1M3]);

    check assertAllMessages(store, K1, [K1M3]);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [K1M3]);
}

@test:Config {
    before: dropTable
}
function testRemoveSystemMessage() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check store.removeChatSystemMessage(K1);

    check assertFromDatabase(cl, K1, [], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K1, [K1M1, k1m2]);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDatabase(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeChatSystemMessage(K2);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDatabase(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);
}

@test:Config {
    before: dropTable
}
function testRemoveInteractiveMessages() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check store.removeChatInteractiveMessages(K1);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1SM1]);

    check assertAllMessages(store, K1, [K1SM1]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, []);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDatabase(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeChatInteractiveMessages(K2);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1SM1]);

    check assertAllMessages(store, K1, [K1SM1]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, []);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [], INTERACTIVE);
    check assertFromDatabase(cl, K2, []);

    check assertAllMessages(store, K2, []);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, []);
}

@test:Config {
    before: dropTable
}
function testRemoveAllMessages() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    check store.removeAll(K1);

    check assertFromDatabase(cl, K1, [], SYSTEM);
    check assertFromDatabase(cl, K1, [], INTERACTIVE);
    check assertFromDatabase(cl, K1, []);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [K2M1], INTERACTIVE);
    check assertFromDatabase(cl, K2, [K2M1]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [K2M1]);

    check store.removeAll(K2);

    check assertFromDatabase(cl, K1, [], SYSTEM);
    check assertFromDatabase(cl, K1, [], INTERACTIVE);
    check assertFromDatabase(cl, K1, []);

    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertFromDatabase(cl, K2, [], SYSTEM);
    check assertFromDatabase(cl, K2, [], INTERACTIVE);
    check assertFromDatabase(cl, K2, []);

    check assertAllMessages(store, K2, []);
    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, []);
}

@test:Config {
    before: dropTable
}
function testRemovingSubsetOfInteractiveMessages() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);
    check store.put(K1, K1M4);

    check store.removeChatInteractiveMessages(K1, 2);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M3, K1M4], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1SM1, K1M3, K1M4]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M3, K1M4]);
    check assertAllMessages(store, K1, [K1SM1, K1M3, K1M4]);
}

@test:Config {
    before: dropTable
}
function testSystemMessageOverwrite() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1SM1, K1M1, k1m2]);

    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, k1sm2);

    check assertSystemMessage(store, K1, k1sm2);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertAllMessages(store, K1, [k1sm2, K1M1, k1m2]);

    check assertFromDatabase(cl, K1, [k1sm2], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDatabase(cl, K1, [k1sm2, K1M1, k1m2]);

    stream<DatabaseRecord, error?> fromDb = cl->query(
        `SELECT MessageJson FROM ChatMessages WHERE MessageKey = ${K1} AND MessageRole = 'SYSTEM'`);
    DatabaseRecord[] records = check from DatabaseRecord dbRecord in fromDb
        select dbRecord;
    test:assertEquals(records.length(), 1);
    ChatSystemMessageDatabaseMessage dbSystemMessage = check records[0].MessageJson.fromJsonStringWithType();
    assertChatMessageEquals(transformFromSystemMessageDatabaseMessage(dbSystemMessage), k1sm2);
}

@test:Config {
    before: dropTable
}
function testSystemMessageOverwriteWithPutAll() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, [K1SM1, K1M1, k1m2, k1sm2]);
    check assertSystemMessage(store, K1, k1sm2);
    check assertFromDatabase(cl, K1, [k1sm2, K1M1, k1m2]);

    stream<DatabaseRecord, error?> fromDb = cl->query(
        `SELECT MessageJson FROM ChatMessages WHERE MessageKey = ${K1} AND MessageRole = 'SYSTEM'`);
    DatabaseRecord[] records = check from DatabaseRecord dbRecord in fromDb
        select dbRecord;
    test:assertEquals(records.length(), 1);
    ChatSystemMessageDatabaseMessage dbSystemMessage = check records[0].MessageJson.fromJsonStringWithType();
    assertChatMessageEquals(transformFromSystemMessageDatabaseMessage(dbSystemMessage), k1sm2);
}

@test:Config {
    before: dropTable
}
function testPutWithDifferentMessageKinds() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    final readonly & ai:ChatFunctionMessage funcMessage = {
        role: "function",
        name: "getWeather",
        id: "func1"
    };

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, funcMessage);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M1, k1m2, funcMessage], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1SM1, K1M1, k1m2, funcMessage]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2, funcMessage]);
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, funcMessage]);
}

@test:Config {
    before: dropTable
}
function testUpdateWithSystemMessageWhenInteractiveMessagesPresentInDbOnStart() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 5);

    _ = check cl->batchExecute([
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M1.role}, ${K1M1.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${k1m2.role}, ${k1m2.toJsonString()})`
    ]);

    check store.put(K1, K1SM1);

    check assertFromDatabase(cl, K1, [K1SM1], SYSTEM);
    check assertFromDatabase(cl, K1, [K1M1, k1m2], INTERACTIVE);
    check assertFromDatabase(cl, K1, [K1M1, k1m2, K1SM1]);

    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
}

function assertAllMessages(ShortTermMemoryStore store, string key, ai:ChatMessage[] expected) returns error? {
    ai:ChatMessage[] actual = check store.getAll(key);
    int actualLength = actual.length();
    test:assertEquals(actualLength, expected.length());
    foreach var index in 0 ..< actualLength {
        assertChatMessageEquals(actual[index], expected[index]);
    }
}

function assertSystemMessage(ShortTermMemoryStore store, string key, ai:ChatSystemMessage? expected) returns error? {
    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(key);
    if expected is () && actual is () {
        return;
    }

    if expected is () || actual is () {
        test:assertFail("Actual and expected ChatSystemMessage do not match");
    }

    assertChatMessageEquals(actual, expected);
}

function assertInteractiveMessages(ShortTermMemoryStore store, string key, ai:ChatInteractiveMessage[] expected) returns error? {
    ai:ChatInteractiveMessage[] actual = check store.getChatInteractiveMessages(key);
    int actualLength = actual.length();
    test:assertEquals(actualLength, expected.length());
    foreach var index in 0 ..< actualLength {
        assertChatMessageEquals(actual[index], expected[index]);
    }
}

enum MessageType {
    SYSTEM,
    INTERACTIVE,
    ALL
}

function assertFromDatabase(mssql:Client cl, string key, ai:ChatMessage[] expected, MessageType messageType = ALL) returns error? {
    sql:ParameterizedQuery[] selectQuery = [`SELECT MessageJson FROM ChatMessages WHERE MessageKey = ${key}`];
    if messageType == SYSTEM {
        selectQuery.push(` AND MessageRole = 'system'`);
    } else if messageType == INTERACTIVE {
        selectQuery.push(` AND MessageRole != 'system'`);
    }
    selectQuery.push(` ORDER BY CreatedAt ASC`);
    stream<DatabaseRecord, error?> databaseRecords = cl->query(sql:queryConcat(...selectQuery));
    ai:ChatMessage[] actualMessages = check toChatMessages(databaseRecords);
    int actualLength = actualMessages.length();
    test:assertEquals(actualLength, expected.length());
    foreach var index in 0 ..< actualLength {
        assertChatMessageEquals(actualMessages[index], expected[index]);
    }
}

function toChatMessages(stream<DatabaseRecord, error?> databaseRecords) returns ai:ChatMessage[]|error =>
    from DatabaseRecord databaseRecord in databaseRecords
select transformFromDatabaseMessage(check toChatMessage(databaseRecord));

function toChatMessage(DatabaseRecord databaseRecord) returns ChatMessageDatabaseMessage|error =>
    databaseRecord.MessageJson.fromJsonStringWithType();

isolated function assertChatMessageEquals(ai:ChatMessage actual, ai:ChatMessage expected) {
    if (actual is ai:ChatUserMessage && expected is ai:ChatUserMessage) ||
            (actual is ai:ChatSystemMessage && expected is ai:ChatSystemMessage) {
        test:assertEquals(actual.role, expected.role);
        assertContentEquals(actual.content, expected.content);
        test:assertEquals(actual.name, expected.name);
        return;
    }

    if actual is ai:ChatFunctionMessage && expected is ai:ChatFunctionMessage {
        test:assertEquals(actual.role, expected.role);
        test:assertEquals(actual.name, expected.name);
        test:assertEquals(actual.id, expected.id);
        return;
    }

    if actual is ai:ChatAssistantMessage && expected is ai:ChatAssistantMessage {
        test:assertEquals(actual.role, expected.role);
        test:assertEquals(actual.name, expected.name);
        test:assertEquals(actual.toolCalls, expected.toolCalls);
        return;
    }

    test:assertFail("Actual and expected ChatMessage types do not match");
}

isolated function assertContentEquals(ai:Prompt|string actual, ai:Prompt|string expected) {
    if actual is string && expected is string {
        test:assertEquals(actual, expected);
        return;
    }

    if actual is ai:Prompt && expected is ai:Prompt {
        test:assertEquals(actual.strings, expected.strings);
        test:assertEquals(actual.insertions, expected.insertions);
        return;
    }

    test:assertFail("Actual and expected content do not match");
}

@test:Config {
    before: dropTable
}
function testBasicStoreWithCache() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K2, K2M1);

    // First retrieval - should load from database and cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    // Second retrieval - should use cache (verify by checking results still match)
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertInteractiveMessages(store, K2, [K2M1]);
}

@test:Config {
    before: dropTable
}
function testBasicStoreWithCacheWithPutAll() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, [K1SM1, K1M1, k1m2]);
    check store.put(K2, K2M1);

    // First retrieval - should load from database and cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    // Second retrieval - should use cache (verify by checking results still match)
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);

    check assertAllMessages(store, K2, [K2M1]);
    check assertInteractiveMessages(store, K2, [K2M1]);
}

@test:Config {
    before: dropTable
}
function testCacheUpdateOnPut() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);

    // Load into cache
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    // Add more messages - cache should be updated
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);

    // Verify cache reflects the updates
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2, K1M3]);
}

@test:Config {
    before: dropTable
}
function testCacheUpdateWithPutAll() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, [K1SM1, K1M1]);
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    // Add more messages - cache should be updated
    check store.put(K1, [k1m2, K1M3]);

    // Verify cache reflects the updates
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);
    check assertInteractiveMessages(store, K1, [K1M1, k1m2, K1M3]);
}

@test:Config {
    before: dropTable
}
function testCacheSystemMessageUpdate() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);

    // Load into cache
    check assertSystemMessage(store, K1, K1SM1);
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    // Update system message
    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, k1sm2);

    // Verify cache reflects the system message update
    check assertSystemMessage(store, K1, k1sm2);
    check assertAllMessages(store, K1, [k1sm2, K1M1]);
}

@test:Config {
    before: dropTable
}
function testCacheSystemMessageUpdateOnPutAll() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, [K1SM1, K1M1]);

    // Load into cache
    check assertSystemMessage(store, K1, K1SM1);
    check assertAllMessages(store, K1, [K1SM1, K1M1]);

    // Update system message
    final readonly & ai:ChatSystemMessage k1sm2 = {
        role: ai:SYSTEM,
        content: "You are a helpful assistant that is aware of sports."
    };
    check store.put(K1, [k1sm2, k1m2]);

    // Verify cache reflects the system message update
    check assertSystemMessage(store, K1, k1sm2);
    check assertAllMessages(store, K1, [k1sm2, K1M1, k1m2]);
}

@test:Config {
    before: dropTable
}
function testCacheInvalidationOnRemoveAll() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // Load into cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);

    // Remove all messages
    check store.removeAll(K1);

    // Verify cache is invalidated and returns empty
    check assertAllMessages(store, K1, []);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {
    before: dropTable
}
function testCacheInvalidationOnRemoveInteractiveMessages() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);

    // Load into cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);

    // Remove all interactive messages
    check store.removeChatInteractiveMessages(K1);

    // Verify cache reflects the removal
    check assertAllMessages(store, K1, [K1SM1]);
    check assertSystemMessage(store, K1, K1SM1);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {
    before: dropTable
}
function testCacheInvalidationOnRemoveSubsetOfInteractiveMessages() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);
    check store.put(K1, K1M3);
    check store.put(K1, K1M4);

    // Load into cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3, K1M4]);

    // Remove first 2 interactive messages
    check store.removeChatInteractiveMessages(K1, 2);

    // Verify cache reflects the partial removal
    check assertAllMessages(store, K1, [K1SM1, K1M3, K1M4]);
    check assertInteractiveMessages(store, K1, [K1M3, K1M4]);
}

@test:Config {
    before: dropTable
}
function testCacheUpdateOnRemoveSystemMessage() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // Load into cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertSystemMessage(store, K1, K1SM1);

    // Remove system message
    check store.removeChatSystemMessage(K1);

    // Verify cache reflects the system message removal
    check assertAllMessages(store, K1, [K1M1, k1m2]);
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [K1M1, k1m2]);
}

@test:Config {
    before: dropTable
}
function testCacheWithMultipleKeys() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    // Add messages for K1
    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // Add messages for K2
    check store.put(K2, K2M1);

    // Load both into cache
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2]);
    check assertAllMessages(store, K2, [K2M1]);

    // Remove K1
    check store.removeAll(K1);

    // Verify K1 is cleared but K2 is still in cache
    check assertAllMessages(store, K1, []);
    check assertAllMessages(store, K2, [K2M1]);
}

@test:Config {
    before: dropTable
}
function testCacheWithSmallCapacity() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 2,
        evictionFactor: 0.5
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1M1);
    check store.put(K2, K2M1);
    check store.put(K3, K1M3);

    // Load K1 and K2 into cache
    check assertAllMessages(store, K1, [K1M1]);
    check assertAllMessages(store, K2, [K2M1]);

    // Load K3 - may evict older entries due to capacity
    check assertAllMessages(store, K3, [K1M3]);

    // All keys should still be retrievable (from cache or database)
    check assertAllMessages(store, K1, [K1M1]);
    check assertAllMessages(store, K2, [K2M1]);
    check assertAllMessages(store, K3, [K1M3]);
}

@test:Config {
    before: dropTable
}
function testSystemMessageRetrievalDoesNotPopulateCache() returns error? {
    mssql:Client cl = getClient();
    cache:CacheConfig cacheConfig = {
        capacity: 10,
        evictionFactor: 0.2
    };
    ShortTermMemoryStore store = check new (cl, cacheConfig = cacheConfig);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    // Retrieve only system message - should NOT populate cache
    check assertSystemMessage(store, K1, K1SM1);

    // Add more messages
    check store.put(K1, K1M3);

    // Retrieve all messages - should load from database and include K1M3
    check assertAllMessages(store, K1, [K1SM1, K1M1, k1m2, K1M3]);
}

final readonly & ai:ChatFunctionMessage K1FN = {role: "function", name: "lookupOrder", content: "{\"id\":\"ORD-1\"}"};

// `PendingApproval` is not statically `anydata` (its `history`/`iterations` admit `Prompt` and
// `Error`), so compare via the database-storable form, which captures every persisted field.
function assertCheckpointEquals(ai:PendingApproval? actual, ai:PendingApproval expected) {
    if actual !is ai:PendingApproval {
        test:assertFail("expected a persisted checkpoint but found none");
    }
    test:assertEquals(toApprovalDatabaseMessage(actual), toApprovalDatabaseMessage(expected));
}

function assertNoCheckpoint(ai:PendingApproval? actual) {
    test:assertTrue(actual is (), "expected no persisted checkpoint");
}

// Builds a `PendingApproval` whose fields exercise every persisted concern: multi-kind history,
// an iteration (with its own history and non-error outputs), tool calls, an approval request, an
// undecided slot, and a response schema. Kept free of `Prompt` content and `Error` outputs so the
// round trip is exactly value-equal (both are intentionally lossy and covered separately below).
function buildPendingApproval(string sessionId) returns ai:PendingApproval {
    ai:FunctionCall toolCall = {name: "issueRefund", arguments: {orderId: "ORD-1", amount: 20}, id: "call-1"};
    ai:ApprovalRequest request = {
        id: "req-1",
        sessionId,
        toolName: "issueRefund",
        toolDescription: "Issues a refund for an order",
        arguments: {orderId: "ORD-1", amount: 20},
        toolCallId: "call-1",
        batchIndex: 0
    };
    ai:Iteration iteration = {
        history: [K1SM1, K1M1, k1m2],
        output: [k1m2, K1FN],
        startTime: [1700000000, 0.5d],
        endTime: [1700000001, 0.25d]
    };
    return {
        sessionId,
        executionId: "exec-1",
        iterationsUsed: 1,
        history: [K1SM1, K1M1, k1m2, K1FN],
        historyPrefixLength: 2,
        iterations: [iteration],
        toolCalls: [toolCall],
        startTime: [1700000000, 0d],
        originalBatch: [toolCall],
        pendingRequests: [request],
        decisions: [()]
    };
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testCheckpointPersistAndRetrieve() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    // No checkpoint yet for an unknown session.
    assertNoCheckpoint(check store.getCheckpoint(K1));

    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);

    // getCheckpoint returns an equal value and leaves it in place.
    assertCheckpointEquals(check store.getCheckpoint(K1), approval);
    assertCheckpointEquals(check store.getCheckpoint(K1), approval);

    // A checkpoint is scoped to its session.
    assertNoCheckpoint(check store.getCheckpoint(K2));
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testCheckpointReplace() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.putCheckpoint(buildPendingApproval(K1));

    ai:PendingApproval updated = buildPendingApproval(K1);
    updated.iterationsUsed = 5;
    updated.decisions = [{decision: ai:APPROVE, reason: "looks good"}];
    check store.putCheckpoint(updated);

    // The second put replaces the first rather than adding a duplicate row.
    assertCheckpointEquals(check store.getCheckpoint(K1), updated);
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testTakeCheckpointClaimsAtomically() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);

    // The first take returns the checkpoint and removes it.
    assertCheckpointEquals(check store.takeCheckpoint(K1), approval);
    // A second take (e.g. a duplicate resume) finds nothing to claim.
    assertNoCheckpoint(check store.takeCheckpoint(K1));
    assertNoCheckpoint(check store.getCheckpoint(K1));
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testRemoveCheckpoint() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.putCheckpoint(buildPendingApproval(K1));
    check store.removeCheckpoint(K1);
    assertNoCheckpoint(check store.getCheckpoint(K1));

    // Removing a non-existent checkpoint is a no-op, not an error.
    check store.removeCheckpoint(K2);
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testCheckpointClearedOnRemoveAll() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    check store.put(K1, K1M1);
    check store.putCheckpoint(buildPendingApproval(K1));

    check store.removeAll(K1);

    // removeAll drops both the messages and the pending checkpoint for the session.
    check assertAllMessages(store, K1, []);
    assertNoCheckpoint(check store.getCheckpoint(K1));
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testCheckpointErrorOutputStringified() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    ai:PendingApproval approval = buildPendingApproval(K1);
    approval.iterations = [
        {
            history: [K1M1],
            output: [error ai:Error("tool failed", error("connection reset"))],
            startTime: [1700000000, 0d],
            endTime: [1700000001, 0d]
        }
    ];
    check store.putCheckpoint(approval);

    ai:PendingApproval? retrieved = check store.getCheckpoint(K1);
    if retrieved !is ai:PendingApproval {
        test:assertFail("expected a persisted checkpoint");
    }
    var output = retrieved.iterations[0].output[0];
    if output !is ai:Error {
        test:assertFail("expected the error output to round-trip as an ai:Error");
    }
    test:assertEquals(output.message(), "tool failed (cause: connection reset)");
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testCheckpointWithPromptContent() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    string city = "Seattle";
    ai:Prompt prompt = `What is the weather in ${city}?`;
    ai:ChatUserMessage userMessage = {role: ai:USER, content: prompt};

    ai:PendingApproval approval = buildPendingApproval(K1);
    approval.history = [userMessage];
    check store.putCheckpoint(approval);

    ai:PendingApproval? retrieved = check store.getCheckpoint(K1);
    if retrieved !is ai:PendingApproval {
        test:assertFail("expected a persisted checkpoint");
    }
    var restored = retrieved.history[0];
    if restored !is ai:ChatUserMessage {
        test:assertFail("expected a user message");
    }
    ai:Prompt|string content = restored.content;
    if content !is ai:Prompt {
        test:assertFail("expected the prompt content to round-trip as a Prompt");
    }
    test:assertEquals(content.strings, prompt.strings);
    test:assertEquals(content.insertions, prompt.insertions);
}

@test:Config {
    before: dropTable
}
function testCheckpointTableNotCreatedOnInit() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore _ = check new (cl);

    int tableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.Checkpoints', 'U') IS NOT NULL, 1, 0) AS TableExists;`);
    test:assertEquals(tableExists, 0,
            "The checkpoint table is the deployment's to provision, never the store's to create");
}

@test:Config {
    before: dropTable
}
function testStoreNeverCreatesCheckpointTable() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    // Nothing the store does creates the checkpoint table: not initialization, not the message
    // operations, and not `removeAll`, which touches the checkpoint table when it exists.
    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    _ = check store.getAll(K1);
    check store.removeAll(K1);

    int tableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.Checkpoints', 'U') IS NOT NULL, 1, 0) AS TableExists;`);
    test:assertEquals(tableExists, 0, "The store must never create the checkpoint table");
}

@test:Config {
    before: dropTable
}
function testCheckpointOperationsFailWithoutTable() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    // The checkpoint table is the deployment's to provision. If it is missing, every checkpoint
    // operation surfaces the database's own error naming the table, rather than silently creating
    // it or pretending nothing is pending.
    ai:PendingApproval?|Error read = store.getCheckpoint(K1);
    if read !is Error {
        test:assertFail("Expected an error when reading a checkpoint without a checkpoint table");
    }
    test:assertTrue(read.message().includes("Checkpoints"), read.message());

    Error? written = store.putCheckpoint(buildPendingApproval(K1));
    if written !is Error {
        test:assertFail("Expected an error when persisting a checkpoint without a checkpoint table");
    }
    test:assertTrue(written.message().includes("Checkpoints"), written.message());

    ai:PendingApproval?|Error claimed = store.takeCheckpoint(K1);
    if claimed !is Error {
        test:assertFail("Expected an error when claiming a checkpoint without a checkpoint table");
    }
    test:assertTrue(claimed.message().includes("Checkpoints"), claimed.message());

    Error? removed = store.removeCheckpoint(K1);
    if removed !is Error {
        test:assertFail("Expected an error when removing a checkpoint without a checkpoint table");
    }
    test:assertTrue(removed.message().includes("Checkpoints"), removed.message());

    int tableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.Checkpoints', 'U') IS NOT NULL, 1, 0) AS TableExists;`);
    test:assertEquals(tableExists, 0, "A failed checkpoint operation must not create the table");
}

@test:Config {
    before: dropTableAndProvisionCheckpoints
}
function testCheckpointsSharedAcrossStoreInstances() returns error? {
    mssql:Client cl = getClient();
    // Two store instances over one provisioned table. Neither holds per-instance state about the
    // table, so a checkpoint written by one is immediately visible to the other, the way two
    // replicas or a restarted process would see it.
    ShortTermMemoryStore storeA = check new (cl);
    check storeA.putCheckpoint(buildPendingApproval(K1));

    ShortTermMemoryStore storeB = check new (cl);
    ai:PendingApproval second = buildPendingApproval(K2);
    check storeB.putCheckpoint(second);

    assertCheckpointEquals(check storeB.getCheckpoint(K2), second);
    assertCheckpointEquals(check storeB.getCheckpoint(K1), buildPendingApproval(K1));
    assertCheckpointEquals(check storeA.takeCheckpoint(K2), second);
}

@test:Config {
    before: dropTable
}
function testCheckpointTableWithExtraColumn() returns error? {
    mssql:Client cl = getClient();
    // A provisioned table carrying an extra nullable column works as-is: the store never validates
    // or migrates the schema, it just reads and writes the columns it knows about.
    _ = check cl->execute(`
        CREATE TABLE Checkpoints (
            SessionId NVARCHAR(100) NOT NULL PRIMARY KEY,
            ApprovalJson NVARCHAR(MAX) NOT NULL,
            UpdatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
            OperatorNote NVARCHAR(200) NULL
        );`);

    ShortTermMemoryStore store = check new (cl);
    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);

    assertCheckpointEquals(check store.takeCheckpoint(K1), approval);
}

@test:Config {
    before: dropTable
}
function testCustomCheckpointTableName() returns error? {
    mssql:Client cl = getClient();
    _ = check cl->execute(`
        CREATE TABLE CustomCheckpoints (
            SessionId NVARCHAR(100) NOT NULL PRIMARY KEY,
            ApprovalJson NVARCHAR(MAX) NOT NULL,
            UpdatedAt DATETIME2 NOT NULL DEFAULT SYSDATETIME()
        );`);

    ShortTermMemoryStore store = check new (cl, checkpointTableName = "CustomCheckpoints");
    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);
    assertCheckpointEquals(check store.getCheckpoint(K1), approval);

    // The default-named table should not have been touched.
    int defaultTableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.Checkpoints', 'U') IS NOT NULL, 1, 0) AS TableExists;`);
    test:assertEquals(defaultTableExists, 0, "Only the configured checkpoint table should be used");

    // Clean up the custom table so it doesn't leak into other test runs.
    _ = check cl->execute(`DROP TABLE dbo.CustomCheckpoints;`);
}

@test:Config {}
function testInvalidCheckpointTableName() {
    mssql:Client cl = getClient();
    ShortTermMemoryStore|Error store = new (cl, checkpointTableName = "invalid-checkpoint-table-name");
    if store !is Error {
        test:assertFail("Expected an error for an invalid checkpoint table name");
    }
    test:assertTrue(store.message().includes("Invalid checkpoint table name"));
}

@test:Config {}
function testCheckpointTableNameCollidingWithMessagesTableRejected() {
    mssql:Client cl = getClient();
    // Without this check, `initializeDatabase()` creates the messages schema under this name
    // eagerly at init, and the checkpoint operations then run against it - so every one of them
    // would fail with a confusing SQL error (missing SessionId/ApprovalJson columns) instead of a
    // clear error here.
    ShortTermMemoryStore|Error store = new (cl, checkpointTableName = "ChatMessages");
    if store !is Error {
        test:assertFail("Expected an error when checkpointTableName collides with tableName");
    }
    test:assertTrue(store.message().includes("must be different from the chat messages table name"));
}

@test:Config {
    before: dropTable
}
function testRemoveAllDoesNotCreateCheckpointTable() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl);

    // No checkpoint was ever put for this key, so the checkpoint table should not exist yet.
    check store.put(K1, K1SM1);
    check store.removeAll(K1);

    int tableExists = check cl->queryRow(
        `SELECT IIF(OBJECT_ID('dbo.Checkpoints', 'U') IS NOT NULL, 1, 0) AS TableExists;`);
    test:assertEquals(tableExists, 0,
            "removeAll should not create the checkpoint table when it does not already exist");
}
