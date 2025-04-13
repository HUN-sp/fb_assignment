from fastapi import HTTPException, status
from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse
from app.db.cassandra_client import cassandra_client

class ConversationController:
    """Controller for handling conversation operations"""

    async def get_user_conversations(self, user_id: int, page: int = 1, limit: int = 20) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination.
        """
        try:
            # Check if user exists
            user = cassandra_client.execute(
                "SELECT user_id FROM users WHERE user_id = %s",
                (user_id,)
            )

            if not user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"User with ID {user_id} not found"
                )

            # Count total conversations
            count_result = cassandra_client.execute(
                "SELECT COUNT(*) FROM conversations_by_user WHERE user_id = %s",
                (user_id,)
            )
            total = count_result[0]['count']

            offset = (page - 1) * limit

            conversations = cassandra_client.execute(
                """
                SELECT conversation_id, user1_id, user2_id, last_message_at, last_message_content
                FROM conversations_by_user
                WHERE user_id = %s
                ORDER BY last_message_at DESC, conversation_id ASC
                LIMIT %s
                """,
                (user_id, offset + limit)
            )

            conversations = conversations[offset:offset + limit] if offset < len(conversations) else []

            formatted_conversations = []
            for conv in conversations:
                other_user_id = conv['user2_id'] if conv['user1_id'] == user_id else conv['user1_id']

                formatted_conversations.append(
                    ConversationResponse(
                        id=conv['conversation_id'],
                        user1_id=conv['user1_id'],
                        user2_id=conv['user2_id'],
                        last_message_at=conv['last_message_at'],
                        last_message_content=conv['last_message_content']
                    )
                )

            return PaginatedConversationResponse(
                total=total,
                page=page,
                limit=limit,
                data=formatted_conversations
            )

        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get user conversations: {str(e)}"
            )

    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID.
        """
        try:
            result = cassandra_client.execute(
                """
                SELECT conversation_id, user1_id, user2_id, last_message_at, last_message_content
                FROM conversations
                WHERE conversation_id = %s
                """,
                (conversation_id,)
            )

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Conversation with ID {conversation_id} not found"
                )

            conv = result[0]

            return ConversationResponse(
                id=conv['conversation_id'],
                user1_id=conv['user1_id'],
                user2_id=conv['user2_id'],
                last_message_at=conv['last_message_at'],
                last_message_content=conv['last_message_content']
            )

        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch conversation: {str(e)}"
            )
