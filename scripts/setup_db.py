"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None
    
    for _ in range(10):  # Try 10 times
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5)  # Wait 5 seconds before trying again
    
    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    """
    Create the keyspace if it doesn't exist.
    """
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")
    
    # Create keyspace with SimpleStrategy and replication factor 1
    # In a production environment, you would use NetworkTopologyStrategy
    # and configure the replication factor for each datacenter
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    """
    Create the tables for the application.
    """
    logger.info("Creating tables...")
    
    # Create users table
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INT,
            username TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (user_id)
        )
    """)
    
    # Create conversations table
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            conversation_id INT,
            user1_id INT,
            user2_id INT,
            last_message_at TIMESTAMP,
            last_message_content TEXT,
            PRIMARY KEY (conversation_id)
        )
    """)
    
    # Create conversations_by_user table
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id INT,
            conversation_id INT,
            last_message_at TIMESTAMP,
            user1_id INT,
            user2_id INT,
            last_message_content TEXT,
            PRIMARY KEY (user_id, last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)
    """)
    
    # Create messages_by_conversation table
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_conversation (
            conversation_id INT,
            created_at TIMESTAMP,
            message_id INT,
            sender_id INT,
            receiver_id INT,
            content TEXT,
            PRIMARY KEY (conversation_id, created_at, message_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, message_id ASC)
    """)
    
    # Create counters table for ID generation
    session.execute("""
        CREATE TABLE IF NOT EXISTS counters (
            name TEXT,
            value COUNTER,
            PRIMARY KEY (name)
        )
    """)
    
    # Initialize counters if they don't exist
    try:
        session.execute("UPDATE counters SET value = value + 0 WHERE name = 'message_id'")
        session.execute("UPDATE counters SET value = value + 0 WHERE name = 'conversation_id'")
    except Exception as e:
        logger.warning(f"Counter initialization warning: {str(e)}")
    
    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")
    
    # Wait for Cassandra to be ready
    cluster = wait_for_cassandra()
    
    try:
        # Connect to the server
        session = cluster.connect()
        
        # Create keyspace and tables
        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)
        
        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()