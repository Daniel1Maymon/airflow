import logging
from sqlalchemy import create_engine
from sqlmodel import Session
from sqlalchemy.dialects.postgresql import insert
from ETL.models.Post import Post

def connect_to_postgres(postgres_url: str) -> create_engine:
    """Connect to PostgreSQL using SQLAlchemy and return the engine."""
    engine = create_engine(postgres_url)
    return engine

def create_table(engine):
    """Create tables based on SQLModel definitions."""
    Post.metadata.create_all(engine)
    logging.info("Table created successfully!")

def insert_data(engine, data: list):
    """Insert processed SQLModel objects into PostgreSQL using ON CONFLICT DO NOTHING."""
    logging.info(f"Attempting to insert {len(data)} records into PostgreSQL.")

    try:
        create_table(engine)

        with Session(engine) as session:
            stmt = insert(Post).values([obj.model_dump() for obj in data])
            stmt = stmt.on_conflict_do_nothing(index_elements=["mongo_id"])

            result = session.execute(stmt)
            session.commit()

            inserted_count = result.rowcount if result and result.rowcount is not None else 0

            if inserted_count > 0:
                logging.info(f"{inserted_count} records actually inserted into PostgreSQL.")
            else:
                logging.info("No new records were inserted due to duplicates.")
    except Exception as e:
        logging.error(f"PostgreSQL insertion failed: {e}")
        raise
