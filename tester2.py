import requests

def fetch_qid_by_label(label):
    """
    Fetch QID for a given label using SPARQL.
    """
    url = "https://query.wikidata.org/sparql"
    query = f"""
    SELECT ?item WHERE {{
      ?item rdfs:label "{label}"@en.
    }}
    """
    headers = {
        "User-Agent": "ProVe/1.1.0 (jongmo.kim@kcl.ac.uk)",
        "Accept": "application/sparql-results+json"
    }
    response = requests.get(url, params={"query": query}, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        results = data.get("results", {}).get("bindings", [])
        if results:
            return results[0]["item"]["value"].split("/")[-1]  # Extract QID from URI
        return None  # No QID found
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None


def fetch_top_pageviews_and_qid(project, access, year, month, day, limit=10):
    """
    Fetches the top viewed pages and their QIDs.
    
    Args:
        project: The Wikipedia project (e.g., "en.wikipedia").
        access: The access method (e.g., "all-access").
        year: The year of the data.
        month: The month of the data.
        day: The day of the data.
        limit: The maximum number of articles to return.
        
    Returns:
        List of tuples containing (title, views, QID).
    """
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/{access}/{year}/{month}/{day}"
    headers = {
        "User-Agent": "ProVe/1.1.0 (jongmo.kim@kcl.ac.uk)"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        articles = data.get('items', [])[0].get('articles', [])
        top_articles = []
        
        for article in articles:  # Iterate through all articles
            title = article['article'].replace("_", " ")  # Replace underscores with spaces
            views = article['views']
            qid = fetch_qid_by_label(title)  # Use the correct function to fetch QID
            
            # Exclude specific titles
            if title in ["Main Page", "Special:Search"]:
                print(f"Excluding title: {title}")
                continue
            
            # Debugging output
            if qid is None:
                print(f"QID not found for title: {title}")
            
            top_articles.append((title, views, qid))
            
            # Stop if we have reached the desired limit
            if len(top_articles) >= limit:
                break
        
        return top_articles
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Example usage
project = "en.wikipedia"
access = "all-access"
year = "2023"
month = "11"
day = "20"
limit = 5  # Change this to the desired number of articles

top_articles_with_qid = fetch_top_pageviews_and_qid(project, access, year, month, day, limit)

if top_articles_with_qid:
    print(f"\nTop {limit} viewed articles with QIDs (excluding Main Page and Special:Search):")
    for rank, (title, views, qid) in enumerate(top_articles_with_qid, start=1):
        print(f"{rank}. Title: {title}, Views: {views}, QID: {qid}")
else:
    print("No articles found.")
