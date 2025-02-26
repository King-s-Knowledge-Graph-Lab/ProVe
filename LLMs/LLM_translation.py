import torch
from transformers import pipeline, AutoTokenizer
import logging

logger = logging.getLogger(__name__)

model_id = "meta-llama/Llama-3.2-3B-Instruct"

tokenizer = AutoTokenizer.from_pretrained(model_id)
pipe = pipeline(
    "text-generation",
    model=model_id,
    torch_dtype=torch.bfloat16,
    device_map="auto",
)

def translate_text(text, chunk_size=4000):  # chunk_size in characters
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
            {"role": "user", "content": "Translate the following text to English: " + chunk_text},
        ]

        outputs = pipe(
            messages,
            max_new_tokens=8192,
        )
        translated_chunk = outputs[0]["generated_text"][-1]['content']
        translated_parts.append(translated_chunk)
        
        logger.info(f"Translation: Processed chunk {i+1}/{len(text_chunks)}")

    # Combine all translated parts
    return ' '.join(translated_parts)

if __name__ == "__main__":
    #example dataset
    text = """    Généalogie de Marion COTILLARD

Acteurs & comédiens

FrançaisNé(e) Marion COTILLARD

Actrice française

Né(e) le 30 septembre 1975 à Paris, France , France (49 ans)

Origine du nom
C'est surtout en Bretagne (22) que l'on rencontre ce nom. On trouve aussi en Bretagne le patronyme Cotillec, Le Cotillec, qui semble avoir la même signification. On peut hésiter pour le sens entre un dérivé de cotte (sorte de tunique, puis cotte de mailles) et un dérivé de coutil, variété de toile. Je pencherais plutôt pour la seconde solution, et il pourrait s'agir d'un fabricant de coutil. Le nom de famille Coutillard existe d'ailleurs lui aussi, on le rencontre dans le Maine-et-Loire.
"""
    print(translate_text(text))


