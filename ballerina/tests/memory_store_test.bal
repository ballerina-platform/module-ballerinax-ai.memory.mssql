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
import ballerina/sql;
import ballerina/test;
import ballerinax/mssql;

const string K1 = "key1";
const string K2 = "key2";

const ai:ChatSystemMessage K1SM1 = {role: ai:SYSTEM, content: "You are a helpful assistant that is aware of the weather."};

const ai:ChatUserMessage K1M1 = {role: ai:USER, content: "Hello, my name is Alice. I'm from Seattle."};
final readonly & ai:ChatAssistantMessage k1m2 = {role: ai:ASSISTANT, content: "Hello Alice, what can I do for you?"};
const ai:ChatUserMessage K1M3 = {role: ai:USER, content: "I would like to know the weather today."};
final readonly & ai:ChatAssistantMessage K1M4 = {role: ai:ASSISTANT, 
        content: "The weather in Seattle today is mostly cloudy with occasional showers and a high around 58°F."};

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
    DatabaseRecord[] records = check from DatabaseRecord dbRecord in fromDb select dbRecord;
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
function testFillingUp() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 3);

    check store.put(K1, K1SM1);
    check store.put(K1, K1M1);
    check store.put(K1, k1m2);

    test:assertFalse(check store.isFull(K1));

    check store.put(K1, K1M3);
    test:assertTrue(check store.isFull(K1));

    Error? res = store.put(K1, K1M4);
    if res is () {
        test:assertFail("Expected an error when adding message to a full store");
    }
    test:assertEquals(res.message(), "Cannot add more messages. Maximum limit of '3' reached for key: 'key1'");
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

@test:Config {
    before: dropTable
}
function testMaxMessageCountLessThanInteractiveMessagesPresentInDbOnStart() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 3);

    _ = check cl->batchExecute([
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M1.role}, ${K1M1.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${k1m2.role}, ${k1m2.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M3.role}, ${K1M3.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M4.role}, ${K1M4.toJsonString()})`
    ]);

    test:assertTrue(check store.isFull(K1));

    Error? res = store.put(K1, <ai:ChatUserMessage>{role: ai:USER, content: "Thank you!"});
    if res is () {
        test:assertFail("Expected an error when adding message to a full store");
    }
    test:assertEquals(res.message(), "Failed to add chat message: " +
        "Cannot load messages from the database: Message count '4' exceeds maximum limit of '3' for key: 'key1'"); 
}

@test:Config {
    before: dropTable
}
function testRetrievalWithMaxMessageCountLessThanInteractiveMessagesPresentInDbOnStart() returns error? {
    mssql:Client cl = getClient();
    ShortTermMemoryStore store = check new (cl, 3);

    _ = check cl->batchExecute([
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M1.role}, ${K1M1.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${k1m2.role}, ${k1m2.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M3.role}, ${K1M3.toJsonString()})`,
        `INSERT INTO ChatMessages (MessageKey, MessageRole, MessageJson) VALUES 
        (${K1}, ${K1M4.role}, ${K1M4.toJsonString()})`
    ]);

    ai:ChatInteractiveMessage[]|Error interactiveMessages = store.getChatInteractiveMessages(K1);
    if interactiveMessages is ai:ChatInteractiveMessage[] {
        test:assertFail("Expected an error when retrieving messages when database entries exceed max limit");
    }
    test:assertEquals(interactiveMessages.message(), "Failed to retrieve chat messages: " +
        "Cannot load messages from the database: Message count '4' exceeds maximum limit of '3' for key: 'key1'"); 
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
        selectQuery.push(` AND MessageRole = 'system'` );
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
    if (actual is ai:ChatUserMessage && expected is ai:ChatUserMessage)  || 
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
