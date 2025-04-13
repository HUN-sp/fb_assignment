from typing import Optional
from datetime import datetime
from fastapi import HTTPException, status

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse
from app.db.cassandra_client import cassandra_client

class MessageController:
    """Controller for handling message operations"""

    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another.
        """
        try:
            sender_id = message_data.sender_id
            receiver_id = message_data.receiver_id
            content = message_data.content

            print(f"Sender ID: {sender_id}, Receiver ID: {receiver_id}, Content: {content}")

            # Ensure users exist
            sender = cassandra_client.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (sender_id,)
            )
            if not sender:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Sender with ID {sender_id} not found")

            receiver = cassandra_client.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (receiver_id,)
            )
            if not receiver:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Receiver with ID {receiver_id} not found")

            # For consistent lookup, always store the lower user_id as user1_id
            user1_id = min(sender_id, receiver_id)
            user2_id = max(sender_id, receiver_id)

            # Check if conversation exists or create new one
            result = cassandra_client.execute(
                "SELECT conversation_id FROM conversations WHERE user1_id = %s AND user2_id = %s ALLOW FILTERING",
                (user1_id, user2_id)
            )

            if result:
                conversation_id = result[0]['conversation_id']
            else:
                cassandra_client.execute("UPDATE counters SET value = value + 1 WHERE name = 'conversation_id'")
                result = cassandra_client.execute("SELECT value FROM counters WHERE name = 'conversation_id'")
                conversation_id = result[0]['value']

                now = datetime.now()
                cassandra_client.execute(
                    """
                    INSERT INTO conversations 
                    (conversation_id, user1_id, user2_id, last_message_at) 
                    VALUES (%s, %s, %s, %s)
                    """,
                    (conversation_id, user1_id, user2_id, now)
                )

            cassandra_client.execute("UPDATE counters SET value = value + 1 WHERE name = 'message_id'")
            result = cassandra_client.execute("SELECT value FROM counters WHERE name = 'message_id'")
            message_id = result[0]['value']

            created_at = datetime.now()

            cassandra_client.execute(
                """
                INSERT INTO messages_by_conversation 
                (conversation_id, created_at, message_id, sender_id, receiver_id, content) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conversation_id, created_at, message_id, sender_id, receiver_id, content)
            )

            cassandra_client.execute(
                """
                UPDATE conversations 
                SET last_message_at = %s, last_message_content = %s 
                WHERE conversation_id = %s
                """,
                (created_at, content, conversation_id)
            )

            cassandra_client.execute(
                """
                INSERT INTO conversations_by_user 
                (user_id, conversation_id, last_message_at, user1_id, user2_id, last_message_content) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (user1_id, conversation_id, created_at, user1_id, user2_id, content)
            )

            cassandra_client.execute(
                """
                INSERT INTO conversations_by_user 
                (user_id, conversation_id, last_message_at, user1_id, user2_id, last_message_content) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (user2_id, conversation_id, created_at, user1_id, user2_id, content)
            )

            return MessageResponse(
                id=message_id,
                conversation_id=conversation_id,
                sender_id=sender_id,
                receiver_id=receiver_id,
                content=content,
                created_at=created_at
            )

        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to send message: {str(e)}"
            )

    async def get_conversation_messages(self,conversation_id: int,page: int = 1,limit: int = 20) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination.
        """
        try:
            # Validate conversation exists
            result = cassandra_client.execute(
                "SELECT conversation_id FROM conversations WHERE conversation_id = %s",
                (conversation_id,)
            )

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Conversation with ID {conversation_id} not found"
                )

            offset = (page - 1) * limit

            # Count total messages in conversation
            count_result = cassandra_client.execute(
                "SELECT COUNT(*) FROM messages_by_conversation WHERE conversation_id = %s",
                (conversation_id,)
            )
            total = count_result[0]['count']

            # Fetch messages for this conversation
            messages = cassandra_client.execute(
                """
                SELECT message_id, conversation_id, sender_id, receiver_id, content, created_at
                FROM messages_by_conversation
                WHERE conversation_id = %s
                ORDER BY created_at DESC, message_id ASC
                LIMIT %s
                """,
                (conversation_id, offset + limit)
            )

            # Apply offset manually
            messages = messages[offset:offset + limit] if offset < len(messages) else []

            formatted_messages = [
                MessageResponse(
                    id=msg['message_id'],
                    conversation_id=msg['conversation_id'],
                    sender_id=msg['sender_id'],
                    receiver_id=msg['receiver_id'],
                    content=msg['content'],
                    created_at=msg['created_at']
                )
                for msg in messages
            ]

            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=formatted_messages
            )

        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get conversation messages: {str(e)}"
            )


    async def get_messages_before_timestamp(self,conversation_id: int,before_timestamp: datetime,page: int = 1,limit: int = 20) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination.
        """
        try:
            result = cassandra_client.execute(
                "SELECT conversation_id FROM conversations WHERE conversation_id = %s",
                (conversation_id,)
            )

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Conversation with ID {conversation_id} not found"
                )

            offset = (page - 1) * limit

            count_result = cassandra_client.execute(
                """
                SELECT COUNT(*) FROM messages_by_conversation
                WHERE conversation_id = %s AND created_at < %s
                """,
                (conversation_id, before_timestamp)
            )
            total = count_result[0]['count']

            messages = cassandra_client.execute(
                """
                SELECT message_id, conversation_id, sender_id, receiver_id, content, created_at
                FROM messages_by_conversation
                WHERE conversation_id = %s AND created_at < %s
                ORDER BY created_at DESC, message_id ASC
                LIMIT %s
                """,
                (conversation_id, before_timestamp, offset + limit)
            )

            messages = messages[offset:offset + limit] if offset < len(messages) else []

            formatted_messages = [
                MessageResponse(
                    id=msg['message_id'],
                    conversation_id=msg['conversation_id'],
                    sender_id=msg['sender_id'],
                    receiver_id=msg['receiver_id'],
                    content=msg['content'],
                    created_at=msg['created_at']
                )
                for msg in messages
            ]

            return PaginatedMessageResponse(
                total=total,
                page=page,
                limit=limit,
                data=formatted_messages
            )

        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get messages before timestamp: {str(e)}"
            )
