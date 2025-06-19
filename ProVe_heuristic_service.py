import random
import sys
from typing import List

from background_processing import process_system_qid
from ProVe_main_service import ProVeService
from utils.logger import logger


class HeuristicBasedService(ProVeService):
    """HeuristicBasedService is a subclass of ProVeService that implements a heuristic-based
    approach for selecting QIDs to process. It uses a random selection strategy by default,
    but can be extended to include other heuristics as needed.

    This service initializes resources, selects a QID using the specified heuristic, and processes
    it. It also verifies the QID to ensure it does not already exist in any of the secondary queues.
    It runs indefinitely until a shutdown signal is received.

    Args:
        config_path (str): Path to the configuration file.
        priority_queue (str): Name of the priority queue in MongoDB.
        secondary_queue (List[str]): List of names of secondary queues in MongoDB.

    Attributes:
        heuristics (dict): A dictionary mapping heuristic names to their corresponding methods.
        heuristic (callable): The heuristic function to use for selecting QIDs.
        running (bool): A flag indicating whether the service is running.
        task_lock (Lock): A threading lock to ensure thread-safe operations.
        mongo_handler (MongoDBHandler): An instance of MongoDBHandler for database operations.
        priority_queue (collection): The priority queue collection in MongoDB.
        secondary_queue (List[collection]): A list of secondary queue collections in MongoDB.
    """

    def __init__(self, config_path: str, priority_queue: str, secondary_queue: List[str] = []):
        super().__init__(config_path, priority_queue, secondary_queue)

        self.heuristics = {
            "random": self.random_selection,
        }
        strategy = self.config.get("queue", {"heuristic": "random"}).get("heuristic", 'random')

        if strategy not in self.heuristics:
            logger.warning(f"Unknown heuristic strategy '{strategy}', defaulting to 'random'.")

        self.heuristic = self.heuristics.get(strategy, self.random_selection)

    def random_selection(self) -> int:
        """Generate a random QID for processing.
        This method generates a random QID in the format 'Q<random_number>'.

        Returns:
            int:  A random QID in the format 'Q<random_number>'.
        """
        return f"Q{random.randint(0, 129999999)}"

    def initialize_resources(self) -> bool:
        """Initialize resources for the heuristic-based service. It skips model initialization."""
        return super().initialize_resources(model=False)

    def run(self) -> None:
        """
        Start the heuristic-based service. It selects a QID using the heuristic set in the
        configuration and processes it. This method runs indefinitely until a shutdown signal.

        Raises:
            SystemExit: If the service fails to initialize resources or encounters a fatal error.
        """
        try:
            if not self.initialize_resources():
                logger.fatal("Failed to initialize resources. Exiting...")
                sys.exit(1)

            logger.info("HeuristicBasedService started successfully")

            while self.running:
                with self.task_lock:
                    qid = self.heuristic()
                    if self.verify_qid(qid):
                        try:
                            process_system_qid(qid)
                            logger.info(f"Queued random QID {qid} for processing.")
                        except ValueError as e:
                            logger.error(f"Invalid QID generated: {str(e)}")

        except Exception as e:
            logger.fatal(f"Error in HeuristicBasedService: {str(e)}")
            sys.exit(1)

    def verify_qid(self, qid: str) -> bool:
        """
        Verify if the QID is valid.

        Args:
            qid (str): The QID to verify.

        Returns:
            bool: True if the QID is valid and does not already exist in any of the secondary
            queues, and priority queue. False otherwise.
        """
        if self.priority_queue.find_one({'qid': qid}):
            logger.warning(f"{qid} already exists in priority queue {self.priority_queue.name}.")
            return False

        for queue in self.secondary_queue:
            if queue.find_one({'qid': qid}):
                logger.warning(f"QID {qid} already exists in secondary queue {queue.name}.")
                return False
        return True


if __name__ == "__main__":
    # Main entry point for the HeuristicBasedService. It initializes the service with the
    # specified configuration and queues, and starts the service. `status_collection` should
    # always be the last queue in the list to ensure efficient duplicate checking (it is the
    # largest collection and contains all processed QIDs).
    service = HeuristicBasedService(
        config_path='config.yaml',
        priority_queue='random_collection',
        secondary_queue=['user_collection', 'status_collection']
    )
    service.run()
