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
    text = """Cesar 2023 : le palmares complet\n\nAvec six trophées, La Nuit du 12 a triomphe aux Cesar 2023. Pacifiction, L'Innocent, Simone - Le Voyage du siècle et A plein temps sont chacun récompensés avec deux César. \n\nCésar du meilleur film : Gagnant : La Nuit du 12 de Dominik Moll. Les Amandiers de Valeria Bruni-Tedeschi, En Corps de Cedric Klapisch et L'Innocent de Louis Garrel ont chacun reçu un César.\n\nCésar de la meilleure réalisation : Gagnant : Dominik Moll pour La Nuit du 12. Cedric Klapisch pour En Corps et Louis Garrel pour L'Innocent ont reçu un César pour leur réalisation.\n\nCésar du meilleur acteur : Gagnant : Benoit Magimel dans Pacifiction - Tourment sur les îles. Jean Dujardin dans Novembre, Louis Garrel dans L'Innocent et Vincent Macaigne dans Chronique d'une liaison passagère ont tous reçu un César.\n\nCésar de la meilleure actrice : Gagnante : Virginie Efira dans Revoir Paris. Fanny Ardant dans Les Jeunes Amants, Juliette Binoche dans Ouistreham et Laure Calamy dans A plein temps ont tous reçu un César.\n\nCésar du meilleur acteur dans un second rôle : Gagnant : Bouli Lanners dans La Nuit du 12. Francois Civil dans En Corps, Micha Lescot dans Les Amandiers et Pio Marmai dans En Corps ont tous reçu un César.\n\nCésar de la meilleure actrice dans un second rôle : Gagnante : Noemie Merlant dans L'Innocent. Judith Chemla dans Le Sixieme Enfant, Anais Demoustier dans Novembre et Anouk Grinberg dans L'Innocent ont tous reçu un César.\n\nCésar du meilleur espoir masculin : Gagnant : Bastien Bouillon dans La Nuit du 12. Stefan Crepon dans Peter Von Kant, Dimitri Dore dans Bruno Reidal, Confession d'un meurtrier et Paul Kircher dans Le Lyceen ont tous reçu un César.\n\nCésar de la meilleure espoir féminin : Gagnante : Nadia Tereszkiewicz dans Les Amandiers. Marion Barbeau dans En Corps, Guslagie Malanda dans Saint Omer et Rebecca Marder dans Une Jeune fille qui va bien ont tous reçu un César.\n\nCésar du meilleur scénario original : Gagnants : Louis Garrel, Tanguy Viel et Naila Guiguet pour L'Innocent. Eric Gravel pour A plein temps, Valeria Bruni-Tedeschi, Noemie Lvovsky et Agnes de Sacy pour Les Amandiers et Cedric Klapisch et Santiago Amigorena pour En Corps ont tous reçu un César.\n\nCésar de la meilleure adaptation : Gagnants : Gilles Marchand et Dominik Moll pour La Nuit du 12. Michel Hazanavicius pour Coupez! Thierry de Peretti et Jeanne Aptekman pour Enquete sur un scandale d'Etat ont tous reçu un César.\n\nCésar du meilleur premier film : Gagnante : Saint Omer d'Alice Diop. Bruno Reidal, Confession d'un meurtrier de Vincent Le Port, Falcon Lake de Charlotte Le Bon et Les Pires de Lise Akoka et Romane Gueret ont tous reçu un César.\n\nCésar des meilleurs costumes : Gagnante : Gigi Lepage pour Simone - Le voyage du siècle. Caroline de Vivaise pour Les Amandiers, Pierre-Jean Larroque pour Couleurs de l'incendie et Emmanuelle Youchnovski pour En attendant Bojangles ont tous reçu un César.\n\nCésar de la meilleure photographie : Gagnant : Artur Tort pour Pacifiction. Julien Poupard pour Les Amandiers, Alexis Kavyrchine pour En Corps et Patrick Ghiringhelli pour La Nuit du 12 ont tous reçu un César.\n\nCésar des meilleurs décors : Gagnant : Christian Marti pour Simone - Le voyage du siècle. Emmanuelle Duplay pour Les Amandiers, Sebastien Birchler pour Couleurs de l'incendie et Michel Barthelemy pour La Nuit du 12 ont tous reçu un César.\n\nCésar du meilleur montage : Gagnante : Mathilde Van de Moortel pour A plein temps. Anne-Sophie Bion pour En Corps, Pierre Deschamps pour L'Innocent et Laure Gardette pour Novembre ont tous reçu un César.\n\nCésar du meilleur son : Gagnants : Francois Maurel, Olivier Mortier et Luc Thomas pour La Nuit du 12. Cyril Moisson, Nicolas Moreau et Cyril Holtz pour En Corps, Laurent Benaim, Alexis Meynet et Olivier Guillaume pour L'Innocent et Cedric Deloche, Alexis Place, Gwennole Le Borgne et Marc Doisne pour Novembre ont tous reçu un César.\n\nCésar des meilleurs effets visuels : Gagnant : Laurens Ehrmann pour Notre-Dame Brule. Guillaume Marien pour Les Cinq Diables, Sebastien Rame pour Fumer fait tousser et Mikael Tanguy pour Novembre ont tous reçu un César.\n\nCésar de la meilleure musique originale : Gagnante : Irene Dresel pour A plein temps. Alexandre Desplat pour Coupez!, Gregoire Hetzel pour L'Innocent et Olivier Marguerit pour La Nuit du 12 ont tous reçu un César.\n\nCésar du meilleur film d'animation : Gagnante : Ma Famille Afghane de Michaela Pavlatova. Ernest et Celestine : le Voyage en Charabia de Jean-Christophe Roger et Julien Chheng, Le Petit Nicolas - Qu'est-ce qu'on attend pour être heureux? d'Amandine Fredon et Benjamin Massoubre ont tous reçu un César.\n\nCésar du meilleur film étranger : Gagnant : As Bestas de Rodrigo Sorogoyen. Close de Lukas Dhont, La Conspiration du Caire de Tarik Saleh et EO de Jerzy Skolimowski ont tous reçu un César.\n\nCésar du meilleur documentaire : Gagnant : Retour à Reims (Fragments) de Jean-Gabriel Periot. Allons enfants de Thierry Demaiziere et Alban Teurlai, Les Années Super 8 d'Annie Ernaux et Davie Ernaux-Briot, Le Chene de Laurent Charbonnier et Michel Seydoux et Jane par Charlotte de Charlotte Gainsbourg ont tous reçu un César.\n\nCésar du meilleur court-métrage de fiction : Gagnante : Partir un jour d'Amelie Bonnin. Haut les coeurs d'Adrian Moyse, Dullin Le Roi David de Lila Pinell et Les Vertueuses de Stephanie Halfon ont tous reçu un César.\n\nCésar du meilleur court-métrage documentaire : Gagnante : Marie Schneider, 1983 d'Elisabeth Subrin. Churchill, Polar Bear Town d'Anabelle Amoros, Ecoutez le battement de nos images d'Audrey Jean-Baptiste et Maxime Jean-Baptiste ont tous reçu un César.\n\nCésar du meilleur court-métrage d'animation : Gagnant : La Vie Sexuelle de Mamie de Urska Djukic et Emilie Pigeard. Caline de Margot Reumont et Noir-Soleil de Marie Larrive ont tous reçu un César.\n\nVanity Fair est votre choix est important. Votre consentement aide et soutient notre mission journalistique.
"""
    print(translate_text(text))


