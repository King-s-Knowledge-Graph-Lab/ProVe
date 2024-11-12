import sqlite3
import logging
import pandas as pd
from typing import Tuple, Any, List

class DatabaseManager:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.setup_database()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def setup_database(self):
        self.cursor.executescript('''
            CREATE TABLE IF NOT EXISTS claims(
                entity_id TEXT,
                claim_id TEXT,
                rank TEXT,
                property_id TEXT,
                datatype TEXT,
                datavalue TEXT,
                PRIMARY KEY (claim_id)
            );
            
            CREATE TABLE IF NOT EXISTS claims_refs(
                claim_id TEXT,
                reference_id TEXT,
                PRIMARY KEY (claim_id, reference_id)
            );
            
            CREATE TABLE IF NOT EXISTS refs(
                reference_id TEXT,
                reference_property_id TEXT,
                reference_index TEXT,
                reference_datatype TEXT,
                reference_value TEXT,
                PRIMARY KEY (reference_id, reference_property_id, reference_index)
            );
                                  
            CREATE TABLE IF NOT EXISTS filtered_claims(
                entity_id TEXT,
                claim_id TEXT,
                rank TEXT,
                property_id TEXT,
                datatype TEXT,
                datavalue TEXT,
                PRIMARY KEY (claim_id)
            );
                                  
            CREATE TABLE IF NOT EXISTS url_references(
                entity_id TEXT,
                reference_id TEXT,
                reference_property_id TEXT,
                reference_datatype TEXT,
                url TEXT,
                PRIMARY KEY (entity_id, reference_id, reference_property_id)
            );
        ''')
        self.conn.commit()

    def reset_database(self):
        """Drops all tables and recreates them."""
        logging.info("Resetting database...")
        self.cursor.executescript('''
            DROP TABLE IF EXISTS claims;
            DROP TABLE IF EXISTS claims_refs;
            DROP TABLE IF EXISTS refs;
            DROP TABLE IF EXISTS filtered_claims;
            DROP TABLE IF EXISTS url_references;
        ''')
        self.setup_database()
        logging.info("Database reset completed.")

    def insert_claim(self, claim_data: Tuple[Any, ...]):
        try:
            self.cursor.execute('''
                INSERT INTO claims(entity_id, claim_id, rank, property_id, datatype, datavalue)
                VALUES(?, ?, ?, ?, ?, ?)
            ''', claim_data)
        except sqlite3.IntegrityError:
            self._handle_claim_integrity_error(claim_data)
        except sqlite3.Error as e:
            logging.error(f"SQLite error: {e}")
            raise

    def _handle_claim_integrity_error(self, claim_data: Tuple[Any, ...]):
        self.cursor.execute(
            'SELECT * FROM claims WHERE claim_id = ?', 
            (claim_data[1],)
        )
        existing_claim = self.cursor.fetchone()
        
        if existing_claim == claim_data:
            logging.info(f"Duplicate claim ignored: {claim_data[1]}")
        else:
            logging.error(f"Integrity error for claim: {claim_data[1]}")
            logging.error(f"Existing: {existing_claim}")
            logging.error(f"New: {claim_data}")
            raise sqlite3.IntegrityError(f"Conflicting data for claim: {claim_data[1]}")

    def insert_claim_reference(self, claim_id: str, reference_hash: str):
        try:
            self.cursor.execute('''
                INSERT OR IGNORE INTO claims_refs(claim_id, reference_id)
                VALUES(?, ?)
            ''', (claim_id, reference_hash))
        except sqlite3.Error as err:
            logging.error(f"Error inserting claim_reference: {err}")
            logging.error(f"Claim ID: {claim_id}, Reference Hash: {reference_hash}")

    def insert_reference(self, ref_data: Tuple[Any, ...]):
        try:
            self.cursor.execute('''
                INSERT INTO refs(reference_id, reference_property_id, reference_index,
                reference_datatype, reference_value)
                VALUES(?, ?, ?, ?, ?)
            ''', ref_data)
        except sqlite3.IntegrityError:
            self._handle_reference_integrity_error(ref_data)
        except sqlite3.Error as err:
            logging.error(f"Error inserting reference: {err}")
            logging.error(f"Reference data: {ref_data}")

    def _handle_reference_integrity_error(self, ref_data: Tuple[Any, ...]):
        self.cursor.execute('''
            SELECT reference_id, reference_property_id, reference_datatype, reference_value
            FROM refs
            WHERE reference_id = ? AND reference_property_id = ?
        ''', (ref_data[0], ref_data[1]))
        
        existing_refs = self.cursor.fetchall()
        if (ref_data[0], ref_data[1], ref_data[3], ref_data[4]) in existing_refs:
            logging.info(f"Duplicate reference ignored: {ref_data[0]}, {ref_data[1]}")
        else:
            logging.error(f"Integrity error for reference: {ref_data[0]}, {ref_data[1]}")
            logging.error(f"Existing: {existing_refs}")
            logging.error(f"New: {ref_data}")
            raise sqlite3.IntegrityError(f"Conflicting data for reference: {ref_data[0]}, {ref_data[1]}")

    def commit(self):
        if self.conn:
            self.conn.commit()
        else:
            logging.warning("No database connection to commit")

    def get_claims_by_entity(self, entity_id: str) -> pd.DataFrame:
        query = "SELECT * FROM claims WHERE entity_id = ?"
        return pd.read_sql_query(query, self.conn, params=(entity_id,))

    def delete_filtered_claims(self, entity_id: str):
        self.cursor.execute("DELETE FROM filtered_claims WHERE entity_id = ?", (entity_id,))

    def insert_filtered_claims(self, df: pd.DataFrame):
        df.to_sql('filtered_claims', self.conn, if_exists='append', index=False)

    def get_filtered_claims(self, entity_id: str) -> List[Tuple]:
        """Get filtered claims for a specific entity."""
        self.cursor.execute(
            "SELECT * FROM filtered_claims WHERE entity_id = ?", 
            (entity_id,)
        )
        return self.cursor.fetchall()

    def get_references_for_claims(self, claim_ids: List[str]) -> pd.DataFrame:
        """Get references for given claim IDs."""
        if not claim_ids:
            return pd.DataFrame()

        # Get reference IDs from claims_refs
        query = """
            SELECT r.reference_id, r.property_id as reference_property_id, 
                   r.index as reference_index, r.datatype as reference_datatype, 
                   r.value as reference_value
            FROM refs r
            JOIN claims_refs cr ON r.reference_id = cr.reference_id
            WHERE cr.claim_id IN ({})
        """.format(','.join(['?'] * len(claim_ids)))
        
        return pd.read_sql_query(
            query,
            self.conn,
            params=claim_ids
        )

    def insert_url_references(self, url_data: pd.DataFrame):
        """Insert URL references into database."""
        if url_data.empty:
            return
            
        for _, row in url_data.iterrows():
            self.cursor.execute('''
                INSERT OR IGNORE INTO url_references 
                (entity_id, reference_id, reference_property_id, reference_datatype, url)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['entity_id'], row['reference_id'], row['reference_property_id'], 
                  row['reference_datatype'], row['url']))
