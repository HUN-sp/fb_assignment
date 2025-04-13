import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client
from cassandra.query import SimpleStatement

class MessageModel:
    """
    Message model for interacting with the messages table.
    """

    @staticmethod
    async def create_message(conversation_id: int, sender_id: int, receiver_id: int, content: str) -> None:
        """
        Create a new message in a conversation.
        
        :param conversation_id: The conversation ID
        :param sender_id: The ID of the user sending the message
        :param receiver_id: The ID of the user receiving the message
        :param content: The message content
        """
        message_id = uuid.uuid4()  # Generate a unique message ID
        created_at = datetime.now()

        query = """
            INSERT INTO messages_by_conversation (conversation_id, created_at, message_id, sender_id, receiver_id, content)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        await cassandra_client.execute(query, (conversation_id, created_at, message_id, sender_id, receiver_id, content))
        
        # Optionally update the conversation with the latest message info
        update_query = """
            UPDATE conversations 
            SET last_message_at = %s, last_message_content = %s
            WHERE conversation_id = %s
        """
        await cassandra_client.execute(update_query, (created_at, content, conversation_id))

    @staticmethod
    async def get_conversation_messages(conversation_id: int, page: int = 1, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get messages for a conversation with pagination.
        
        :param conversation_id: The conversation ID
        :param page: The page number for pagination (default is 1)
        :param limit: The number of messages to return per page
        :return: A list of messages
        """
        offset = (page - 1) * limit
        query = SimpleStatement("""
            SELECT message_id, sender_id, receiver_id, content, created_at
            FROM messages_by_conversation
            WHERE conversation_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """)
        
        result = await cassandra_client.execute(query, (conversation_id, limit))
        messages = [{"message_id": row.message_id, "sender_id": row.sender_id, "receiver_id": row.receiver_id,
                     "content": row.content, "created_at": row.created_at} for row in result]
        
        return messages

    @staticmethod
    async def get_messages_before_timestamp(conversation_id: int, timestamp: datetime, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get messages before a timestamp with pagination.
        
        :param conversation_id: The conversation ID
        :param timestamp: The timestamp to filter messages before
        :param limit: The number of messages to return
        :return: A list of messages
        """
        query = SimpleStatement("""
            SELECT message_id, sender_id, receiver_id, content, created_at
            FROM messages_by_conversation
            WHERE conversation_id = %s AND created_at < %s
            ORDER BY created_at DESC
            LIMIT %s
        """)
        
        result = await cassandra_client.execute(query, (conversation_id, timestamp, limit))
        messages = [{"message_id": row.message_id, "sender_id": row.sender_id, "receiver_id": row.receiver_id,
                     "content": row.content, "created_at": row.created_at} for row in result]
        
        return messages


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    """

    @staticmethod
    async def get_user_conversations(user_id: int, page: int = 1, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get conversations for a user with pagination.
        
        :param user_id: The user ID to fetch conversations for
        :param page: The page number for pagination (default is 1)
        :param limit: The number of conversations to return per page
        :return: A list of conversations
        """
        offset = (page - 1) * limit
        query = SimpleStatement("""
            SELECT conversation_id, user1_id, user2_id, last_message_at, last_message_content
            FROM conversations_by_user
            WHERE user_id = %s
            LIMIT %s
        """)
        
        result = await cassandra_client.execute(query, (user_id, limit))
        conversations = [{"conversation_id": row.conversation_id, "user1_id": row.user1_id, "user2_id": row.user2_id,
                          "last_message_at": row.last_message_at, "last_message_content": row.last_message_content} for row in result]
        
        return conversations

    @staticmethod
    async def get_conversation(conversation_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a conversation by ID.
        
        :param conversation_id: The conversation ID
        :return: A conversation dict or None if not found
        """
        query = SimpleStatement("""
            SELECT conversation_id, user1_id, user2_id, last_message_at, last_message_content
            FROM conversations
            WHERE conversation_id = %s
        """)
        
        result = await cassandra_client.execute(query, (conversation_id,))
        if result:
            row = result[0]
            return {"conversation_id": row.conversation_id, "user1_id": row.user1_id, "user2_id": row.user2_id,
                    "last_message_at": row.last_message_at, "last_message_content": row.last_message_content}
        return None

    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.
        
        :param user1_id: The first user ID
        :param user2_id: The second user ID
        :return: A conversation dict
        """
        # Ensure the smaller user ID comes first to avoid duplicate conversations
        if user1_id > user2_id:
            user1_id, user2_id = user2_id, user1_id
        
        # Try to get the existing conversation
        query = SimpleStatement("""
            SELECT conversation_id, user1_id, user2_id, last_message_at, last_message_content
            FROM conversations
            WHERE user1_id = %s AND user2_id = %s
        """)
        
        result = await cassandra_client.execute(query, (user1_id, user2_id))
        if result:
            row = result[0]
            return {"conversation_id": row.conversation_id, "user1_id": row.user1_id, "user2_id": row.user2_id,
                    "last_message_at": row.last_message_at, "last_message_content": row.last_message_content}
        
        # If no conversation exists, create a new one
        conversation_id = uuid.uuid4()
        created_at = datetime.now()
        
        insert_query = """
            INSERT INTO conversations (conversation_id, user1_id, user2_id, last_message_at, last_message_content)
            VALUES (%s, %s, %s, %s, %s)
        """
        await cassandra_client.execute(insert_query, (conversation_id, user1_id, user2_id, created_at, ""))
        
        # Return the newly created conversation
        return {"conversation_id": conversation_id, "user1_id": user1_id, "user2_id": user2_id,
                "last_message_at": created_at, "last_message_content": ""}
