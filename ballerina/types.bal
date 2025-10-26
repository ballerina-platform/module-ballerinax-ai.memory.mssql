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

type Prompt record {|
    string[] strings;
    anydata[] insertions;
|};

type ChatUserMessageDatabaseMessage record {|
    ai:USER role;
    string|Prompt content;
    string name?;
|};

type ChatSystemMessageDatabaseMessage record {|
    ai:SYSTEM role;
    string|Prompt content;
    string name?;
|};

type ChatMessageDatabaseMessage 
    ChatUserMessageDatabaseMessage|ChatSystemMessageDatabaseMessage|ai:ChatAssistantMessage|ai:ChatFunctionMessage;

type ChatInteractiveMessageDatabaseMessage 
    ChatUserMessageDatabaseMessage|ai:ChatAssistantMessage|ai:ChatFunctionMessage;

isolated function transformToDatabaseMessage(ai:ChatMessage message) returns ChatMessageDatabaseMessage {
    if message is ai:ChatAssistantMessage|ai:ChatFunctionMessage {
        return message;
    }

    string|ai:Prompt content = message.content;
    string|Prompt transformedContent = content is string ? content : {
        strings: content.strings,
        insertions: content.insertions
    };

    if message is ai:ChatUserMessage {
        return {
            role: ai:USER,
            content: transformedContent,
            name: message.name
        };
    } 
    
    return {
        role: ai:SYSTEM,
        content: transformedContent,
        name: message.name
    };
}

isolated function transformFromDatabaseMessage(ChatMessageDatabaseMessage dbMessage) returns ai:ChatMessage {
    if dbMessage is ChatSystemMessageDatabaseMessage {
        return transformFromSystemMessageDatabaseMessage(dbMessage);
    }

    return transformFromInteractiveMessageDatabaseMessage(<ChatInteractiveMessageDatabaseMessage> dbMessage);
}

isolated function transformFromSystemMessageDatabaseMessage(ChatSystemMessageDatabaseMessage dbMessage) 
        returns ai:ChatSystemMessage & readonly {
    string|Prompt content = dbMessage.content;
    string|(ai:Prompt & readonly) transformedContent = content is string ? 
            content : createAIPrompt(content.strings.cloneReadOnly(), content.insertions.cloneReadOnly());
    
    return {
        role: ai:SYSTEM,
        content: transformedContent,
        name: dbMessage.name
    };
}

isolated function transformFromInteractiveMessageDatabaseMessage(ChatInteractiveMessageDatabaseMessage dbMessage) 
        returns ai:ChatInteractiveMessage & readonly {
    if dbMessage is ai:ChatAssistantMessage|ai:ChatFunctionMessage {
        return dbMessage.cloneReadOnly();
    }

    string|Prompt content = dbMessage.content;
    string|(ai:Prompt & readonly) transformedContent = content is string ? 
            content : createAIPrompt(content.strings.cloneReadOnly(), content.insertions.cloneReadOnly());

    return {
        role: ai:USER,
        content: transformedContent,
        name: dbMessage.name
    };
}

isolated function createAIPrompt(string[] & readonly strings, anydata[] & readonly insertions)
        returns readonly & ai:Prompt => isolated object ai:Prompt {
    public final string[] & readonly strings = strings;
    public final anydata[] & readonly insertions = insertions;
};

isolated function mapToImmutableMessage(ai:ChatMessage message) returns readonly & ai:ChatMessage {
    if message is ai:ChatSystemMessage {
        final ai:Prompt|string content = message.content;
        readonly & ai:Prompt|string memoryContent = 
            getPromptContent(content is string ? content : [content.strings, content.insertions.cloneReadOnly()]);
        return {role: message.role, content: memoryContent, name: message.name};
    }
    return mapToMemoryChatInteractiveMessage(<ai:ChatInteractiveMessage> message);
}

isolated function mapToMemoryChatInteractiveMessage(ai:ChatInteractiveMessage message) returns 
        readonly & ai:ChatInteractiveMessage {
    if message is ai:ChatAssistantMessage|ai:ChatFunctionMessage {
        return message.cloneReadOnly();
    }
    final ai:Prompt|string content = message.content;
    readonly & ai:Prompt|string memoryContent = 
        getPromptContent(content is string ? content : [content.strings, content.insertions.cloneReadOnly()]);

    return {role: message.role, content: memoryContent, name: message.name};
}

isolated function getPromptContent(string|([string[], anydata[]] & readonly) content) returns string|(ai:Prompt & readonly) => 
    content is string ? content : createAIPrompt(content[0], content[1]);

type DatabaseRecord record {|
    string MessageJson;
    json...;
|};
