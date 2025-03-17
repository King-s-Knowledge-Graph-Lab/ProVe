# ProVe (Provenance Verification for Wikidata claims)

## Overview
ProVe is a system designed to automatically verify claims and references in Wikidata. It extracts claims from Wikidata entities, fetches the referenced URLs, processes the HTML content, and uses NLP models to determine whether the claims are supported by the referenced content.

## System Architecture

The RQV system consists of several key components:

1. **Data Collection and Processing**:
   - `WikidataParser`: Extracts claims and URLs from Wikidata based on QID (item identifier)
   - `HTMLFetcher`: Collects HTML content from reference URLs
   - `HTMLSentenceProcessor`: Converts HTML to sentences for analysis

2. **Evidence Selection and Verification**:
   - `EvidenceSelector`: Selects relevant sentences as evidence
   - `ClaimEntailmentChecker`: Verifies entailment relationship between claims and evidence

3. **NLP Models**:
   - `TextualEntailmentModule`: Checks textual entailment relationships
   - `SentenceRetrievalModule`: Retrieves relevant sentences
   - `VerbModule`: Handles verbalization processing

4. **Data Storage**:
   - MongoDB: Stores HTML content, entailment results, parser statistics, and status information
   - SQLite: Stores verification results for API access

5. **Service Structure**:
   - `ProVe_main_service.py`: Main service logic
   - `ProVe_main_process.py`: Entity processing logic
   - `background_processing.py`: Background processing tasks

## Setup Instructions

### 1. Install Dependencies
```
pip install -r requirements.txt
```

### 2. Download NLP Models
The 'base' folder contains essential NLP models for the RQV tool, including pre-trained & fine-tuned BERT, T5, and related parsers and NLP models.

Download from:
```
https://emckclac-my.sharepoint.com/:f:/r/personal/k2369089_kcl_ac_uk/Documents/base?csf=1&web=1&e=TBo3nE
```

Place the downloaded 'base' folder in the project root directory.

### 3. Configure the System
Review and modify the `config.yaml` file to adjust database settings, HTML fetching parameters, and evidence selection thresholds.

## Usage

### Processing a Single Entity
```python
from ProVe_main_process import initialize_models, process_entity

# Initialize models
models = initialize_models()

# Process entity by QID
qid = 'Q44'  # Example: Barack Obama
html_df, entailment_results, parser_stats = process_entity(qid, models)
```

### Running the Service
The main service can be started by running:
```
python ProVe_main_service.py
```

This will start the MongoDB handler and schedule background processing tasks.

### Background Processing
The system can automatically process:
- Top viewed Wikidata items
- Items from a pagepile list
- Random QIDs

## Configuration

The `config.yaml` file contains important settings:
- Database configurations
- Algorithm version
- HTML fetching parameters (batch size, delay, timeout)
- Text processing settings
- Evidence selection parameters

## Data Flow

1. A Wikidata QID is provided to the system
2. The system extracts claims and reference URLs from the entity
3. HTML content is fetched from the reference URLs
4. The HTML is processed into sentences
5. Relevant sentences are selected as evidence
6. NLP models verify if the evidence supports the claims
7. Results are stored in the database


