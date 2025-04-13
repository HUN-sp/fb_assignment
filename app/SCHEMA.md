# FB Messenger MVP - Cassandra Schema Design

## Key Design Considerations
- Cassandra is optimized for writes and specific read patterns
- Queries should be driven by primary key design
- Denormalization is necessary for efficient querying
- Data is often duplicated across tables to support different access patterns

## Schema Design

### 1. User Table
Store user information:

```cql
CREATE TABLE users (
    user_id INT,
    username TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (user_id)
);
```

### 2. Conversations Table
For storing conversation metadata:

```cql
CREATE TABLE conversations (
    conversation_id INT,
    user1_id INT,
    user2_id INT,
    last_message_at TIMESTAMP,
    last_message_content TEXT,
    PRIMARY KEY (conversation_id)
);
```

### 3. Conversations By User
For retrieving conversations for a specific user ordered by most recent activity:

```cql
CREATE TABLE conversations_by_user (
    user_id INT,
    conversation_id INT,
    last_message_at TIMESTAMP,
    user1_id INT,
    user2_id INT,
    last_message_content TEXT,
    PRIMARY KEY (user_id, last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
```

### 4. Messages By Conversation
For fetching all messages in a conversation and supporting pagination by timestamp:

```cql
CREATE TABLE messages_by_conversation (
    conversation_id INT,
    created_at TIMESTAMP,
    message_id INT,
    sender_id INT,
    receiver_id INT,
    content TEXT,
    PRIMARY KEY (conversation_id, created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id ASC);
```

### 5. Message Counter
For generating unique message IDs:

```cql
CREATE TABLE counters (
    name TEXT,
    value COUNTER,
    PRIMARY KEY (name)
);
```

## Query Patterns

### Sending messages between users
1. Find or create a conversation between users
2. Get next message_id
3. Insert message into `messages_by_conversation`
4. Update `conversations` and `conversations_by_user` with last message info

### Fetching user conversations ordered by recent activity
```cql
SELECT * FROM conversations_by_user 
WHERE user_id = ? 
ORDER BY last_message_at DESC 
LIMIT ? OFFSET ?;
```

### Fetching all messages in a conversation
```cql
SELECT * FROM messages_by_conversation 
WHERE conversation_id = ? 
ORDER BY created_at DESC 
LIMIT ? OFFSET ?;
```

### Fetching messages before a given timestamp (for pagination)
```cql
SELECT * FROM messages_by_conversation 
WHERE conversation_id = ? AND created_at < ? 
ORDER BY created_at DESC 
LIMIT ?;
```