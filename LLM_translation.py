import torch
from transformers import pipeline, AutoTokenizer

model_id = "meta-llama/Llama-3.2-3B-Instruct"
tokenizer = AutoTokenizer.from_pretrained(model_id)
pipe = pipeline(
    "text-generation",
    model=model_id,
    torch_dtype=torch.bfloat16,
    device_map="auto",
)

def translate_text(text, chunk_size=1000):  # chunk_size in characters
    translated_parts = []
    
    # First split text into character-based chunks
    text_chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    
    for i, chunk in enumerate(text_chunks):
        # Now tokenize each chunk
        tokens = tokenizer.encode(chunk)
        
        # Convert tokens back to text (this ensures we have valid token boundaries)
        chunk_text = tokenizer.decode(tokens)
        
        messages = [
            {"role": "system", "content": "If the user's input is in a non-English language, translate it fully to English. Ensure the response is entirely in English, providing only the translated text without any additional comments or formatting."},
            {"role": "user", "content": chunk_text},
        ]

        outputs = pipe(
            messages,
            max_new_tokens=1024,
        )
        translated_chunk = outputs[0]["generated_text"][-1]['content']
        translated_parts.append(translated_chunk)
        
        # Optional: Print progress
        print(f"Processed chunk {i+1}/{len(text_chunks)}")

    # Combine all translated parts
    return ' '.join(translated_parts)

if __name__ == "__main__":
    #example dataset
    text = """Marion Cotillard
französische Schauspielerin
Geburtstag:	30. September 1975 Paris
Nation:	Frankreich

Internationales Biographisches Archiv 35/2021 vom 31. August 2021 (mf)
Ergänzt um Nachrichten durch MA-Journal bis KW 35/2024


Herkunft
Marion Cotillard wurde am 30. Sept. 1975 in Paris als Tochter eines Schauspielerpaares geboren. Ihr Vater Jean-Claude ist außerdem Schauspiellehrer an der École Supérieure d'Art dramatique in Paris und betreibt eine Theatergruppe mit dem Namen "Company Cotillard". C. hat noch zwei jüngere Brüder, die Zwillinge Guillaume und Quentin. Guillaume wurde Drehbuchautor und Regisseur, Quentin Bildhauer.

Ausbildung
C. schauspielerte seit ihrem 5. Lebensjahr und machte sich zunächst mit dem Theatermilieu vertraut. Mit 17 stand sie zum ersten Mal für eine Folge der TV-Serie "Highlander" vor der Kamera. C. besuchte eine Schauspielschule in Orléans, an der sie 1994 mit einem Preis ausgezeichnet wurde.

Wirken
Künstlerische Einordnung und KarrierebeginnMit 90 Filmen (Stand: 2021) avancierte C. zu einer der bekanntesten französischen Filmschauspielerinnen. "Es ist dieser Blick, diese Gabe, mit ganz wenig Mimik ganz große Geschichten zu erzählen", beschrieb der stern (29.12.2016) ihr Erfolgsgeheimnis. C. lasse "die verführerische Aura der alten Filmdiven im modernen Kino wieder aufleben", befand die Süddeutsche ...


Die Biographie von Marion Cotillard ist nur eine von über 40.000, die in unseren biographischen Datenbanken Personen, Sport und Pop verfügbar sind. Wöchentlich bringen wir neue Porträts, publizieren redaktionell überarbeitete Texte und aktualisieren darüberhinaus Hunderte von Biographien.
Unsere Datenbanken sind unverzichtbare Recherchequelle für Journalisten und Publizisten, wertvolle Informationsquelle für Entscheidungsträger in Politik und Wirtschaft, Grundausstattung für jede Bibliothek und unerschöpfliche Fundgrube für jeden, der mit den Zeitläuften und ihren Protagonisten Schritt halten will.

"""
    print(translate_text(text))


