import requests
import json
from urllib.parse import quote

def query_wikimedia(target_id, languages=None):
    if languages is None:
        languages = ["en", "de", "fr", "es", "zh"]
    
    languages_str = '", "'.join(languages)
    
    sparql_query = f"""
    SELECT ?label ?labelLang ?alias ?aliasLang ?description ?descLang
    WHERE {{
    OPTIONAL {{
    {target_id} rdfs:label ?label.
    FILTER(LANG(?label) IN ("{languages_str}"))
    BIND(LANG(?label) AS ?labelLang)
    }}
    OPTIONAL {{
    {target_id} skos:altLabel ?alias.
    FILTER(LANG(?alias) IN ("{languages_str}"))
    BIND(LANG(?alias) AS ?aliasLang)
    }}
    OPTIONAL {{
    {target_id} schema:description ?description.
    FILTER(LANG(?description) IN ("{languages_str}"))
    BIND(LANG(?description) AS ?descLang)
    }}
    }}
    ORDER BY
    (IF(?labelLang = "en", 0, 1))
    ?labelLang
    (IF(?aliasLang = "en", 0, 1))
    ?aliasLang
    (IF(?descLang = "en", 0, 1))
    ?descLang
    LIMIT 30
    """

    url = 'https://query.wikidata.org/sparql'
    params = {
        'query': sparql_query,
        'format': 'json'
    }

    headers = {
        'User-Agent': 'Python-SPARQL-Query/1.0 (https://example.com/; username@example.com)'
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        
        results = data.get('results', {}).get('bindings', [])
        
        if results:
            first_result = results[0]
            label = first_result.get('label', {}).get('value', 'No label')
            alias = first_result.get('alias', {}).get('value', 'No alias')
            description = first_result.get('description', {}).get('value', 'No description')
        else:
            label, alias, description = 'No label', 'No alias', 'No description'
        
        return {
            'target_id': target_id,
            'label': label,
            'alias': alias,
            'description': description,
            'full_results': results
        }
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# 사용 예시
target_id = 'wd:Q816695'  # 인간을 나타내는 Wikidata ID
result = query_wikimedia(target_id)
print(json.dumps(result, indent=2, ensure_ascii=False))