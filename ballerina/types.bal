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
import ballerina/time;

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

type CheckpointRecord record {|
    string ApprovalJson;
|};

// A single reasoning-action cycle of a paused agent run, in its database-storable form.
// `history` uses `ChatMessageDatabaseMessage` for the same reason `PendingApproval.history` does
// (see `ApprovalDatabaseMessage`); `output` narrows `ai:Error` to a `string` summary, since
// `ai:Error` values are not JSON-serializable.
type IterationDatabaseMessage record {|
    ChatMessageDatabaseMessage[] history;
    (ai:ChatAssistantMessage|ai:ChatFunctionMessage|string)[] output;
    time:Utc startTime;
    time:Utc endTime;
|};

// The database-storable form of `ai:PendingApproval`. Identical to `ai:PendingApproval` except
// `history`/`iterations[*].history` are converted to `ChatMessageDatabaseMessage[]`, since
// `ai:ChatMessage`'s `Prompt`-typed content is not directly JSON-serializable (same problem
// `ChatMessageDatabaseMessage` already solves for stored chat messages).
type ApprovalDatabaseMessage record {|
    string sessionId;
    string executionId;
    int iterationsUsed;
    ChatMessageDatabaseMessage[] history;
    int historyPrefixLength;
    IterationDatabaseMessage[] iterations;
    ai:FunctionCall[] toolCalls;
    time:Utc startTime;
    ai:FunctionCall[] originalBatch;
    ai:ApprovalRequest[] pendingRequests;
    ai:HumanDecision?[] decisions;
|};

isolated function toStoredIterationOutput(ai:ChatAssistantMessage|ai:ChatFunctionMessage|ai:Error output)
        returns ai:ChatAssistantMessage|ai:ChatFunctionMessage|string {
    if output is ai:Error {
        error? cause = output.cause();
        return cause is error ? string `${output.message()} (cause: ${cause.message()})` : output.message();
    }
    return output;
}

isolated function fromStoredIterationOutput(ai:ChatAssistantMessage|ai:ChatFunctionMessage|string stored)
        returns ai:ChatAssistantMessage|ai:ChatFunctionMessage|ai:Error =>
    stored is string ? error ai:Error(stored) : stored;

isolated function toIterationDatabaseMessage(ai:Iteration iteration) returns IterationDatabaseMessage => {
    history: from ai:ChatMessage message in iteration.history select transformToDatabaseMessage(message),
    output: from var output in iteration.output select toStoredIterationOutput(output),
    startTime: iteration.startTime,
    endTime: iteration.endTime
};

isolated function fromIterationDatabaseMessage(IterationDatabaseMessage dbMessage) returns ai:Iteration => {
    history: from ChatMessageDatabaseMessage message in dbMessage.history select transformFromDatabaseMessage(message),
    output: from var output in dbMessage.output select fromStoredIterationOutput(output),
    startTime: dbMessage.startTime,
    endTime: dbMessage.endTime
};

isolated function toApprovalDatabaseMessage(ai:PendingApproval approval) returns ApprovalDatabaseMessage => {
    sessionId: approval.sessionId,
    executionId: approval.executionId,
    iterationsUsed: approval.iterationsUsed,
    history: from ai:ChatMessage message in approval.history select transformToDatabaseMessage(message),
    historyPrefixLength: approval.historyPrefixLength,
    iterations: from ai:Iteration iteration in approval.iterations select toIterationDatabaseMessage(iteration),
    toolCalls: approval.toolCalls,
    startTime: approval.startTime,
    originalBatch: approval.originalBatch,
    pendingRequests: approval.pendingRequests,
    decisions: approval.decisions
};

isolated function fromApprovalDatabaseMessage(ApprovalDatabaseMessage dbMessage) returns ai:PendingApproval => {
    sessionId: dbMessage.sessionId,
    executionId: dbMessage.executionId,
    iterationsUsed: dbMessage.iterationsUsed,
    history: from ChatMessageDatabaseMessage message in dbMessage.history select transformFromDatabaseMessage(message),
    historyPrefixLength: dbMessage.historyPrefixLength,
    iterations: from IterationDatabaseMessage iteration in dbMessage.iterations select fromIterationDatabaseMessage(iteration),
    toolCalls: dbMessage.toolCalls,
    startTime: dbMessage.startTime,
    originalBatch: dbMessage.originalBatch,
    pendingRequests: dbMessage.pendingRequests,
    decisions: dbMessage.decisions
};
