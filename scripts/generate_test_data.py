"""
Script to generate test data for the Messenger application.
"""
import os
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def get_next_id(session, counter_name):
    """Get next ID from counter."""
    session.execute(f"UPDATE counters SET value = value + 1 WHERE name = '{counter_name}'")
    result = session.execute(f"SELECT value FROM counters WHERE name = '{counter_name}'")
    return result.one()[0]


def generate_test_data(session):
    """Generate test data in Cassandra."""
    logger.info("Generating test data...")
    
    # Create users
    logger.info(f"Creating {NUM_USERS} users...")
    usernames = [f"user{i}" for i in range(1, NUM_USERS + 1)]
    for i, username in enumerate(usernames, 1):
        session.execute(
            "INSERT INTO users (user_id, username, created_at) VALUES (%s, %s, %s) IF NOT EXISTS",
            (i, username, datetime.now() - timedelta(days=random.randint(1, 30)))
        )
    
    # Create conversations between users
    logger.info(f"Creating {NUM_CONVERSATIONS} conversations...")
    conversations = []
    
    # Ensure each user has at least one conversation
    for user_id in range(1, NUM_USERS + 1):
        other_user_id = random.choice([i for i in range(1, NUM_USERS + 1) if i != user_id])
        
        # Always store the lower user_id as user1_id to avoid duplicates
        user1_id = min(user_id, other_user_id)
        user2_id = max(user_id, other_user_id)
        
        # Check if this pair already has a conversation
        result = session.execute(
            "SELECT conversation_id FROM conversations WHERE user1_id = %s AND user2_id = %s ALLOW FILTERING",
            (user1_id, user2_id)
        )
        
        if not result.one():
            conversation_id = get_next_id(session, 'conversation_id')
            created_at = datetime.now() - timedelta(days=random.randint(1, 30))
            
            # Insert into conversations
            session.execute(
                """
                INSERT INTO conversations 
                (conversation_id, user1_id, user2_id, last_message_at) 
                VALUES (%s, %s, %s, %s)
                """,
                (conversation_id, user1_id, user2_id, created_at)
            )
            conversations.append((conversation_id, user1_id, user2_id, created_at))
    
    # Fill in remaining conversations to meet NUM_CONVERSATIONS
    while len(conversations) < NUM_CONVERSATIONS:
        user1_id = random.randint(1, NUM_USERS)
        user2_id = random.choice([i for i in range(1, NUM_USERS + 1) if i != user1_id])
        
        # Always store the lower user_id as user1_id to avoid duplicates
        if user1_id > user2_id:
            user1_id, user2_id = user2_id, user1_id
        
        # Check if this pair already has a conversation
        result = session.execute(
            "SELECT conversation_id FROM conversations WHERE user1_id = %s AND user2_id = %s ALLOW FILTERING",
            (user1_id, user2_id)
        )

        if not result.one():
            conversation_id = get_next_id(session, 'conversation_id')
            created_at = datetime.now() - timedelta(days=random.randint(1, 30))
            
            # Insert into conversations
            session.execute(
                """
                INSERT INTO conversations 
                (conversation_id, user1_id, user2_id, last_message_at) 
                VALUES (%s, %s, %s, %s)
                """,
                (conversation_id, user1_id, user2_id, created_at)
            )
            conversations.append((conversation_id, user1_id, user2_id, created_at))
    
    # Generate messages for each conversation
    logger.info("Generating messages for each conversation...")
    for conv_id, user1_id, user2_id, created_at in conversations:
        num_messages = random.randint(5, MAX_MESSAGES_PER_CONVERSATION)
        
        last_message_at = None
        last_message_content = None
        
        for _ in range(num_messages):
            sender_id = random.choice([user1_id, user2_id])
            receiver_id = user2_id if sender_id == user1_id else user1_id
            
            # Random time after conversation creation, but before now
            created_at_msg = created_at + timedelta(
                minutes=random.randint(1, int((datetime.now() - created_at).total_seconds() / 60))
            )
            
            message_id = get_next_id(session, 'message_id')
            content = f"Test message {message_id} from {sender_id} to {receiver_id} at {created_at_msg}"
            
            # Insert message into messages_by_conversation
            session.execute(
                """
                INSERT INTO messages_by_conversation 
                (conversation_id, created_at, message_id, sender_id, receiver_id, content) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conv_id, created_at_msg, message_id, sender_id, receiver_id, content)
            )
            
            # Track the last message
            if last_message_at is None or created_at_msg > last_message_at:
                last_message_at = created_at_msg
                last_message_content = content
        
        # Update conversations table with the last message info
        session.execute(
            """
            UPDATE conversations 
            SET last_message_at = %s, last_message_content = %s 
            WHERE conversation_id = %s
            """,
            (last_message_at, last_message_content, conv_id)
        )
        
        # Update conversations_by_user for both users
        for user_id in [user1_id, user2_id]:
            session.execute(
                """
                INSERT INTO conversations_by_user 
                (user_id, conversation_id, last_message_at, user1_id, user2_id, last_message_content) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (user_id, conv_id, last_message_at, user1_id, user2_id, last_message_content)
            )
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()