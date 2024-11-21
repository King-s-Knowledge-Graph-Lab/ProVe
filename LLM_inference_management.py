from pymongo import MongoClient
from datetime import datetime
from typing import Optional, Dict, Any
import uuid

class LLMInference:
    def __init__(self):
        # MongoDB 연결
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['llm_inference']
        
        # 컬렉션 생성
        self.status_collection = self.db['llm_status']
        self.inference_collection = self.db['llm_inference']

    def create_status(self, qid: str, task_id: str, llm_task_id: str, 
                     infer_type: str) -> Dict[str, Any]:
        status_doc = {
            'qid': qid,
            'task_id': task_id,
            'llm_task_id': llm_task_id,
            'llm_status': 'in queue',
            'infer_type': infer_type,
            'requested_time': datetime.utcnow(),
            'processing_start_time': None,
            'completed_time': None
        }
        self.status_collection.insert_one(status_doc)
        return status_doc

    def create_inference(self, llm_task_id: str, input_data: Dict[str, Any], 
                        prompt: str, generated_text: Optional[str] = None) -> Dict[str, Any]:
        inference_doc = {
            'llm_task_id': llm_task_id,
            'source_data_from_html': input_data,
            'prompt': prompt,
            'generated_text': generated_text
        }
        self.inference_collection.insert_one(inference_doc)
        return inference_doc

    def update_status(self, llm_task_id: str, status: str, 
                     processing_start_time: Optional[datetime] = None,
                     completed_time: Optional[datetime] = None) -> None:
        update_dict = {'llm_status': status}
        if processing_start_time:
            update_dict['processing_start_time'] = processing_start_time
        if completed_time:
            update_dict['completed_time'] = completed_time
            
        self.status_collection.update_one(
            {'llm_task_id': llm_task_id},
            {'$set': update_dict}
        )
        
    def get_latest_verification_status(self, qid: str) -> Optional[Dict[str, Any]]:
        # Access status collection from wikidata_verification DB
        verification_db = self.client['wikidata_verification']
        verification_status = verification_db['status']
        
        # Query the latest completed status for the given qid
        latest_status = verification_status.find_one(
            {
                'qid': qid,
                'status': 'completed'
            },
            sort=[('completed_timestamp', -1)]  # Sort by most recent first
        )
        
        return latest_status

    def create_status_from_verification(self, qid: str, llm_task_id: str, 
                                    infer_type: str) -> Optional[Dict[str, Any]]:
        # Fetch verification status
        verification_status = self.get_latest_verification_status(qid)
        
        if not verification_status:
            return None
        
        # Create llm_status document using verification data
        status_doc = {
            'qid': verification_status['qid'],
            'task_id': verification_status['task_id'],
            'llm_task_id': llm_task_id,
            'llm_status': 'in queue',
            'infer_type': infer_type,
            'requested_time': datetime.utcnow(),
            'processing_start_time': None,
            'completed_time': None
        }
        
        self.status_collection.insert_one(status_doc)
        return status_doc
    
    def create_inference_from_verification(self, llm_task_id: str, prompt: str) -> Optional[list]:
        # Get status document to retrieve task_id
        status_doc = self.status_collection.find_one({'llm_task_id': llm_task_id})
        if not status_doc:
            return None
        
        # Get HTML contents using task_id
        html_contents = self.get_html_content(status_doc['task_id'])
        if not html_contents:
            return None
        
        # Create separate inference documents for each HTML content
        inference_docs = []
        for html_content in html_contents:
            inference_doc = {
                'llm_task_id': llm_task_id,
                'source_data_from_html': html_content['html'],
                'entity_label': html_content.get('entity_label'),
                'property_label': html_content.get('property_label'),
                'object_label': html_content.get('object_label'),
                'prompt': prompt,
                'generated_text': None
            }
            
            # Insert each document
            self.inference_collection.insert_one(inference_doc)
            inference_docs.append(inference_doc)
        
        return inference_docs
    
    def get_html_content(self, task_id: str) -> Optional[list]:
        """
        Get HTML contents and labels from wikidata_verification DB's html_content collection
        Only returns content where status is 200
        """
        try:
            # Access html_content collection from wikidata_verification DB
            verification_db = self.client['wikidata_verification']
            html_collection = verification_db['html_content']
            
            # Query documents with task_id and status 200
            html_contents = list(html_collection.find(
                {
                    'task_id': task_id,
                    'status': 200  # Only get successfully fetched content
                },
                {
                    'html': 1,            # HTML content
                    'url': 1,             # URL
                    'entity_label': 1,    # Entity label
                    'property_label': 1,  # Property label
                    'object_label': 1,    # Object label
                    '_id': 0              # Exclude MongoDB _id
                }
            ))
            
            return html_contents if html_contents else None
            
        except Exception as e:
            print(f"Error fetching HTML content: {e}")
            return None
    
    def reset_database(self) -> None:
        """
        Reset all collections in llm_inference database.
        For testing purposes only.
        """
        try:
            # Drop existing collections
            self.status_collection.drop()
            self.inference_collection.drop()
            
            # Recreate collections
            self.db.create_collection('llm_status')
            self.db.create_collection('llm_inference')
            
            print("Successfully reset llm_inference database")
            
        except Exception as e:
            print(f"Error resetting database: {e}")
    
if __name__ == "__main__":
    # Example usage
    llm = LLMInference()
    
    # Reset database for testing
    llm.reset_database()

    # Create status using qid
    status = llm.create_status_from_verification(
        qid="Q44",
        llm_task_id="llm_task_" + str(uuid.uuid4()),
        infer_type="relevant_sentences_generation_from_HTML"
    )

    if status:
        # Then create inference documents
        inference_docs = llm.create_inference_from_verification(
            llm_task_id=status['llm_task_id'],
            prompt="Extract relevant sentences from the HTML content"
        )
        
        if inference_docs:
            print(f"Created {len(inference_docs)} inference documents successfully")
        else:
            print("Failed to create inference documents")
    else:
        print("No verification data found for the given qid")