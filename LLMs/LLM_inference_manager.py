from pymongo import MongoClient
from datetime import datetime
from typing import Optional, Dict, Any
import uuid
import torch
from transformers import pipeline, AutoTokenizer
import signal
import sys
import time

# Initialize LLM model
def initialize_llm_model():
    model_id = "meta-llama/Llama-3.2-3B-Instruct"
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    pipe = pipeline(
        "text-generation",
        model=model_id,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    )
    return tokenizer, pipe

class LLMInference:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['llm_inference']
 
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
                        generated_text: Optional[str] = None) -> Dict[str, Any]:
        inference_doc = {
            'llm_task_id': llm_task_id,
            'source_data_from_html': input_data,
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
    
    def generate_and_update_text(self, llm_task_id: str) -> Optional[str]:
        """
        Generates text using LLM model and updates the inference document
        """
        # Find inference document by llm_task_id
        inference_doc = self.inference_collection.find_one({'llm_task_id': llm_task_id})
        
        if not inference_doc:
            return None
            
        # Create claim-based prompt

        entity = inference_doc.get('entity_label', '')
        property = inference_doc.get('property_label', '')
        object_val = inference_doc.get('object_label', '')

        html_content = inference_doc.get('source_data_from_html', '')
    
        # Generate text using LLM model
        messages = [
            {"role": "system", "content": "You extract plain text from HTML and prioritise information directly related to the claim. Minimise unrelated processing."},
            {"role": "user", "content": f"""
            Claim: {entity} {property} {object_val}.
            Extract only text containing keywords or concepts related to the claim from the following HTML:
            {html_content}
            Focus only on sections that are relevant."""},
        ]

        outputs = pipe(
            messages,
            max_new_tokens=4096,
        )
        generated_text = outputs[0]["generated_text"][-1]['content']
        
        # Update the inference document with generated text
        self.inference_collection.update_one(
            {'_id': inference_doc['_id']},
            {'$set': {'generated_text': generated_text}}
        )
        
        return generated_text
    
    def process_queued_inferences(self):
        """
        Process all tasks that are in 'in queue' status and perform inference.
        """
        try:
            # Convert cursor to list to avoid reprocessing
            queued_tasks = list(self.status_collection.find({'llm_status': 'in queue'}))
            
            for task in queued_tasks:
                llm_task_id = task['llm_task_id']
                
                try:
                    # Update status to 'processing'
                    self.update_status(
                        llm_task_id=llm_task_id,
                        status='processing',
                        processing_start_time=datetime.utcnow()
                    )
                    
                    # Get the inference document
                    inference_doc = self.inference_collection.find_one(
                        {'llm_task_id': llm_task_id}
                    )
                    
                    if inference_doc:
                        # Generate text
                        generated_text = self.generate_and_update_text(llm_task_id)
                        
                        if generated_text:
                            # Update status to 'completed' if successful
                            self.update_status(
                                llm_task_id=llm_task_id,
                                status='completed',
                                completed_time=datetime.utcnow()
                            )
                        else:
                            # Update status to 'failed' if no text was generated
                            self.update_status(
                                llm_task_id=llm_task_id,
                                status='failed',
                                completed_time=datetime.utcnow()
                            )
                            
                except Exception as e:
                    print(f"Error processing task {llm_task_id}: {e}")
                    # Update status to 'error' if exception occurs
                    self.update_status(
                        llm_task_id=llm_task_id,
                        status='error',
                        completed_time=datetime.utcnow()
                    )
                    
        except Exception as e:
            print(f"Error in process_queued_inferences: {e}")
    
class LLMQueueMonitor:
    def __init__(self, check_interval=1):  # 1초마다 체크
        """
        Initialize the queue monitor
        Args:
            check_interval (int): Time interval between checks in seconds
        """
        self.llm_manager = LLMInference()
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['llm_inference']
        self.status_collection = self.db['llm_status']
        self.check_interval = check_interval
        self.is_running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutting down monitor...")
        self.is_running = False

    def check_queue(self):
        """
        Check for new tasks in queue
        """
        try:
            # Find documents with 'in queue' status
            queued_tasks = self.status_collection.find({'llm_status': 'in queue'})
            
            for task in queued_tasks:
                llm_task_id = task['llm_task_id']
                print(f"Processing task: {llm_task_id}")
                self.llm_manager.process_queued_inferences()
                
        except Exception as e:
            print(f"Error checking queue: {e}")

    def monitor_queue(self):
        """
        Continuously monitor the queue for new inference tasks
        """
        print(f"Starting LLM queue monitor. Checking every {self.check_interval} seconds...")
        
        while self.is_running:
            try:
                self.check_queue()
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"Error in monitor_queue: {e}")
                time.sleep(self.check_interval)

    def reset_databases(self):
        """
        Reset all collections in both llm_inference and wikidata_verification databases.
        For development and testing purposes only.
        """
        try:
            # Reset llm_inference database
            print("\nResetting llm_inference database...")
            self.db['llm_status'].drop()
            self.db['llm_inference'].drop()
            self.db.create_collection('llm_status')
            self.db.create_collection('llm_inference')
            print("Successfully reset llm_inference collections")

        except Exception as e:
            print(f"Error resetting databases: {e}")


if __name__ == "__main__":
    tokenizer, pipe = initialize_llm_model()

    monitor = LLMQueueMonitor(check_interval=1)
    #monitor.reset_databases()
    monitor.monitor_queue()
