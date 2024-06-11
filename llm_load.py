from huggingface_hub import login
from unsloth import FastLanguageModel
import torch
from transformers import TextStreamer

def llmLoad(max_seq_length):
    with open('API_key.txt', 'r') as file:
        token = file.read().strip()  
    login(token=token)

    dtype = None # None for auto detection. Float16 for Tesla T4, V100, Bfloat16 for Ampere+
    load_in_4bit = True # Use 4bit quantization to reduce memory usage. Can be False.

    # 4bit pre quantized models we support for 4x faster downloading + no OOMs.
    fourbit_models = [
        "unsloth/mistral-7b-bnb-4bit",
        "unsloth/mistral-7b-instruct-v0.2-bnb-4bit",
        "unsloth/llama-2-7b-bnb-4bit",
        "unsloth/gemma-7b-bnb-4bit",
        "unsloth/gemma-7b-it-bnb-4bit", # Instruct version of Gemma 7b
        "unsloth/gemma-2b-bnb-4bit",
        "unsloth/gemma-2b-it-bnb-4bit", # Instruct version of Gemma 2b
        "unsloth/llama-3-8b-bnb-4bit", # [NEW] 15 Trillion token Llama-3
    ] # More models at https://huggingface.co/unsloth

    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name = "unsloth/llama-3-8b-Instruct-bnb-4bit",
        max_seq_length = max_seq_length,
        dtype = dtype,
        load_in_4bit = load_in_4bit,
    )
    return tokenizer, model

def llmQuestion(tokenizer, model, instruct, question, output_size):
    FastLanguageModel.for_inference(model) # Enable native 2x faster inference
    alpaca_prompt = """Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.

    ### Instruction:
    {}

    ### Input:
    {}

    ### Response:
    {}"""

    # alpaca_prompt = Copied from above
    FastLanguageModel.for_inference(model) # Enable native 2x faster inference
    inputs = tokenizer(
    [
        alpaca_prompt.format(
            instruct, # instruction
            question, # input
            "", # output - leave this blank for generation!
        )
    ], return_tensors = "pt").to("cuda")


    outputs = model.generate(**inputs, max_new_tokens=output_size, use_cache=True)
    output_text = tokenizer.batch_decode(outputs)[0].split('### Response:')[1]

    return output_text

if __name__ == "__main__":
    tokenizer, model = llmLoad(8192)
    sentences = """['\n  \n   \n\t\t\t\n\t\t\t\n\t\t \n     \n     \n    \n   \n    \n     \n      \n       \n        \n         UK News Website of the Year\n        \n         \n          The Telegraph logo\n         \n       \n      \n      \n       ',
 '\n        \n         \n          \n           \n            Search Icon\n           \n         \n         \n             News   \n             Sport   \n             Money   \n             Travel   \n             ',
 'Business   \n             Health   \n             Opinion   \n             General election   \n             Ukraine   \n             Royals   \n             Life & Style   \n             Culture   \n        ',
 "     Puzzles   \n         \n         \n\t\t(function () {\n\t\t\tdocument.querySelectorAll('.site-header__navigation .e-site-header-button__link').forEach(link => {\n\t\t\t\tlink.addEventListener('click', (e) => {\n",
 '\t\t\t\t\teVar94 = "header-search-icon-mobile";\n\t\t\t\t\teVar95 = link.textContent.trim();\n\t\t\t\t\teVar96 = "img";\n\t\t\t\t\teVar97 = document.title;\n\t\t\t\t\ttmgComponentString = eVar94+";"+eVar95+"_"+eVar96+";"+eVar97;\n',
 '\t\t\t\t\tlocalStorage.setItem("tmgComponentTracking", tmgComponentString);\n\t\t\t\t});\n\t\t\t});\n\t\t})();\n\t\n        \n       \n      \n      \n       \n        \n              UK Edition  \n         \n          \n        ',
 ' \n        \n        \n            US Edition  \n        \n       \n      \n      \n       \n        \n         \n          Search Icon\n         \n       \n         \n             Subscribe now   Free for one month',
 '  \n           \n            \n           \n         \n        \n       \n        \n\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\tLog in\n\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\n       \n       \n          \n       \n      \n     \n    \n   \n   \n    \n  ',
 '   \n      \n        \n            \n                \n                \n                \n            \n\n        \n                \n            \n    \n      \n       \n        \n         Sections\n         \n      ',
 '    \n           \n                 UK Edition  \n            \n             \n            \n           \n           \n               US Edition  \n           \n          \n         \n        \n        \n         \n',
 '           News\n            \n             \n            \n           \n             News home \n             UK news \n             Politics \n             World \n             Health news \n             Defe',
 'nce \n             Science \n             Education \n             Environment \n             Investigations \n             Global Health Security \n           \n           Sport\n            \n             \n ',
 "           \n           \n             Sport home \n             Football \n             Rugby Union \n             Cricket \n             F1 \n             Golf \n             Tennis \n             Women's Sp",
 'ort \n             Racing \n             Cycling \n             Boxing \n             More... \n           \n           Money\n            \n             \n            \n           \n             Money home \n   ',
 '          Property \n             Tax \n             Pensions \n             Banking \n             Investing \n             Net Zero \n             Calculators \n             Guides \n           \n           ',
 'Travel\n            \n             \n            \n           \n             Travel home \n             Europe \n             UK \n             Worldwide \n             City breaks \n             Hotels \n      ',
 '       Cruise \n             Ski \n             Advice \n           \n           Business\n            \n             \n            \n           \n             Business home \n             Alex \n             Ec',
 'onomy \n             Companies \n             Markets \n             Tech \n           \n           Health\n            \n             \n            \n           \n             Health home \n             Diet \n ',
 '            Fitness \n             Conditions \n             Wellbeing \n             Parenting \n             Guides \n             Tools \n           \n           Opinion\n            \n             \n       ',
 '     \n           \n             Opinion home \n             Obituaries \n             Letters to the Editor \n             Telegraph View \n             Our columnists \n             Cartoons \n           \n ',
 '          General election \n           Ukraine\n            \n             \n            \n           \n             Ukraine home \n             Daily podcast \n             Daily newsletter \n           \n   ',
 '        Royals\n            \n             \n            \n           \n             Royals home \n             King Charles III \n             Queen Camilla \n             Prince William \n             Prince',
 'ss of Wales \n             Prince Harry \n             Duchess of Sussex \n           \n           Life & Style\n            \n             \n            \n           \n             Life & Style home \n        ',
 '     Family \n             Columnists \n             Cookbook \n             Food & Drink \n             Fashion \n             Beauty \n             Luxury \n             Cars \n             Gardening \n     ',
 '        Interiors \n             Puzzle News \n             Recommended \n             Tel Mag \n           \n           Culture\n            \n             \n            \n           \n             Culture hom',
 'e \n             TV \n             Film \n             Music \n             Books \n             Theatre \n             Comedy \n             Dance \n             Opera \n             Art \n             \n      ',
 '       Telegraph Tickets \n           \n           Puzzles \n         \n        \n       \n       \n        \n         \n          \n                 UK Edition  \n             \n              \n             \n    ',
 '        \n                US Edition  \n            \n          \n         \n        \n       \n       \n        \n          \n             Subscribe now   Free for one month \n            \n             \n       ',
 '     \n          \n         \n        \n         \n          \n           \n            \n              Log in\n              \n               Login icon\n              \n            \n           \n         \n      ',
 '   \n          Follow us on:\n          \n           \n             \n              Facebook icon\n             \n           \n             \n              Instagram icon\n             \n           \n            ',
 ' \n              X icon\n             \n           \n             \n              Snapchat icon\n             \n           \n             \n              LinkedIn icon\n             \n           \n             \n ',
 '             YouTube icon \n             \n          \n         \n        \n       \n       \n        More from The Telegraph\n        \n         \n           Download our app \n           Newsletters \n         ',
 '  Telegraph Extra \n           Recommended \n           Financial Solutions \n           Events \n           Betting \n           Dating \n           Offers \n           Travel offers \n           Shop \n     ',
 '      Garden shop \n           Bookshop \n           Tickets \n           Puzzles \n           Fantasy Football \n           Work at The Telegraph \n           Telegraph Corporate \n           Help and suppo',
 'rt \n           The Chelsea Magazine Company \n           Broadband and Mobile Deals \n           Voucher codes \n           \n             See top shops\n             \n              \n             \n        ',
 '    \n              Samsung \n              Nike \n              ASOS \n              eBay \n              Currys \n              Wayfair \n              TUI \n              JD Sports \n              Travelodg',
 'e \n              Adidas \n              Broadband deals \n              Cheap broadband \n              Broadband in my area \n              Broadband and TV deals \n              Mobile deals \n           ',
 "   SIM-only deals \n            \n           \n         \n        \n       \n      \n     \n    \n   \n   \n\t\t(function () {\n\t\t\tdocument.querySelectorAll('.site-header__buttons .e-site-header-button__link').forE",
 'ach(link => {\n\t\t\t\tlink.addEventListener(\'click\', (e) => {\n\t\t\t\t\teVar94 = "header-search-icon-desktop";\n\t\t\t\t\teVar95 = link.textContent.trim();\n\t\t\t\t\teVar96 = "img";\n\t\t\t\t\teVar97 = document.title;\n\t\t\t\t\ttmg',
 'ComponentString = eVar94+";"+eVar95+"_"+eVar96+";"+eVar97;\n\t\t\t\t\tlocalStorage.setItem("tmgComponentTracking", tmgComponentString);\n\t\t\t\t});\n\t\t\t});\n\t\t})();\n\t\n  \n\t\n\t\t\n\t\t\t\n\t\t Jump to navigation\n  \n   \n   \n',
 "    \n     \n     \n      \n   \n    \n     Hitch Hiker's Guide author Douglas Adams dies aged 49\n     \n    \n    \n     \n        By Andrew Alderson and Daniel Foggo    13 May 2001 • 12:00am \n     \n     \n    ",
 "\n     \n      \n       \n        DOUGLAS ADAMS, the thought-provoking author who inspired a generation with his cult science-fiction novel, The Hitch Hiker's Guide to the Galaxy, has died at the age of 4",
 '9 from a heart attack while working out at the gym.\n       \n      \n      \n       \n        \n         \n          \n           \n          \n          \n           Douglas Adams: inspired a generation with t',
 'he cult novel, A Hitch Hiker\'s Guide to the Galaxy\n          \n         \n        \n       \n      \n      \n       \n        Adams\'s age was seven more than his cryptic answer of "42" to the intriguing ques',
 'tion the comic novel had posed: what is the answer to life, the universe and everything? His book has sold more than 14 million copies worldwide, but Adams became a household name in Britain after it ',
 'was turned into a BBC television series in the early 1980s.\n       \n      \n      \n       \n        Adams, 6ft 5in tall and well built, did not have a history of heart problems. However, say friends, he',
 ' had visited the doctor just days ago complaining of a numbness in his arm. He collapsed on Friday while exercising at a gym in Santa Barbara on the west coast of America and never regained consciousn',
 'ess. He leaves a widow and a six-year-old daughter.\n       \n      \n      \n       \n        Adams was British but moved with his family to California in 1999, to be involved in a Disney film version of ',
 'his book: he had previously lived in Islington, north London, for 22 years. A complex man, he was transported from obscurity to fame in 1979 by the instant success of his novel, which became hugely po',
 'pular with students.\n       \n      \n      \n       \n        Soon after the book was published, he was invited to sign copies at a small Soho bookshop. On his way there, Adams became convinced he was be',
 'en caught up in a demonstration, only to discover the crowds were waiting for him.\n       \n      \n      \n       \n        The book shot to the number one spot in the best-seller list the next day. He s',
 'aid: "It was like being helicoptered to the top of Mount Everest, or having an orgasm without the foreplay." Adams, however, later suffered from writer\'s block and was so notoriously bad at meeting de',
 "adlines that Sue Freestone, his former publisher, was even known to move into his house to bully him into writing.\n       \n      \n      \n       \n        Ed Victor, Adams's literary agent for 20 years ",
 'and a close friend, was devastated by the news yesterday. He said: "I feel as if someone has torn a limb off me. Tragic is an overused word, but this really is a tragic loss.\n       \n      \n      \n   ',
 '    \n        Mr Victor said: "He was one of the truly original writers and thinkers of our generation who should have had many years ahead of him. He was not only entertaining, but also stimulating an',
 'd provoking: he was a unique thinker with a huge audience."\n       \n      \n      \n       \n        Mr Victor said that writer\'s block had been a terrible problem for Adams, who hated spending time alon',
 'e. He said: "He was once locked in a hotel suite at the Berkeley for two weeks by Sonny Mehta [his former publisher]. When I asked Douglas how it had worked, he said: \'It was simple. I sat at the desk',
 ' and typed and Sonny sat in an armchair and glowered.\' "\n       \n      \n      \n       \n        Adams was said to have used The Hitch Hiker\'s Guide, which started off as a radio show in the 1970s, to p',
 'oke fun at those who seek solutions to unanswerable questions. It was intended to highlight the absurdity of attempting to do so.\n       \n      \n      \n       \n        The novel has since been turned ',
 'into a play and a computer game, and has spawned four sequels. Adams also set up a website called h2g2, an entertainment guide now run by the BBC, as a spin-off from his book.\n       \n      \n      \n  ',
 '     \n        In his novel, which deals with the voyages of a suburban earthling, Arthur Dent, Adams describes a race of hyper-intelligent beings, who had reached a point where they were determined to',
 ' understand the purpose of the universe and their own existence.\n       \n      \n      \n       \n        They built a supercomputer, Deep Thought, and asked it for the answer to the ultimate question of',
 ' life, the universe and everything. The computer worked for several millennia on the answer. Finally, the beings were shocked and disappointed with the computer\'s ridiculous response: "42".\n       \n  ',
 '    \n      \n       \n        In the book, the Earth is referred to as "mostly harmless", which became a buzz phrase of the 1980s. Adams was born in Cambridge in 1952 and educated at Brentwood School, E',
 "ssex, before returning to Cambridge to study at St John's College.\n       \n      \n      \n       \n        His early career included work as a radio and television writer and producer. Some of his early",
 " writing was with his friend Graham Chapman, a member of the Monty Python's Flying Circus comedy team.\n       \n      \n      \n       \n        He later collaborated with Terry Jones, another Python team",
 ' member. Jones was in tears after learning of his friend\'s death yesterday. He told the Telegraph: "Douglas was a total original: he had a beautiful way of thinking and an incisive mind that went stra',
 'ight to the heart of matters. He had a genius for putting those concepts into words. His books were great works of literature. He was a lovely man, and I loved him."\n       \n      \n      \n       \n    ',
 '    Senior staff at the BBC, who worked with Adams, were equally sad. Alan Yentob, the corporation\'s director of drama and entertainment, said: "Douglas was a big character who will be hugely missed b',
 'y a host of friends and millions of fans around the world."\n       \n      \n      \n       \n        Geoffrey Perkins, the BBC\'s head of comedy and who produced the original radio series of the novel, sa',
 'id: "I\'ve known Douglas for 25 years. He was absolutely one of the most creative geniuses to ever work in radio comedy."\n       \n      \n      \n       \n        Adams\'s life was transformed by the publi',
 "cation of The Hitch Hiker's Guide providing him with a wealth he had never imagined. He married Jane Belson, a barrister, in 1991 and they had a daughter, Polly, in 1994.\n       \n      \n      \n       ",
 "\n        Adams's other bestselling titles include The Restaurant at the End of the Universe; Life, the Universe and Everything and So Long, and Thanks for All the Fish. He was in discussion to turn an",
 "other of his books, Dirk Gently's Holistic Detective Agency, into a film and was working on another novel, which was 12 years late.\n       \n      \n     \n     \n      \n      \n      \n       \n        \n   ",
 '       \n           Twitter Icon\n          \n        \n          \n           Facebook Icon\n          \n        \n          \n           WhatsApp Icon\n          \n        \n          \n           Email Icon\n   ',
 '       \n       \n       \n        \n         \n          Comment speech bubble\n          \n       \n      \n     \n    \n    \n     \n      \n       \n        Advertisement\n       \n       \n      \n\n\tMore stories\n\n\n',
 '\n\n\n\n\n\n\n\t\n\n\n\n\n\n\t\n\t\n\n\n\n\n\n     \n    \n    \n     \n     \n     \n      \n       \n         \n          Twitter Icon\n         \n       \n         \n          Facebook Icon\n         \n       \n         \n          Whats',
 'App Icon\n         \n       \n         \n          Email Icon\n         \n      \n      \n       \n        \n         Comment speech bubble\n         \n      \n     \n    \n   \n  \n  \n   \n    \n     \n\n\tMore from The T',
 'elegraph\n\n\n\n\n\n\n\n\n\n\t\n\n\n\n\n\n\t\n\t\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\t\n\t\n\n\t\n\n\t\n\n\t\t\n\t\t\n\n\t\n\t\n\n\t\n\n\t\n\n\t\t\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\n\n\t\t\t\t\t\n\t\t\t\t\t\tMore stories\n\t\t\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\n\t\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t',
 '\n\n\t\t\n\n\t\n\n\n\n\t\t\n\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t\n\t\t\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\n\t\n\t\n\t\n\n\t\n\t\n\n\t\n\t\n\n\t\n\n\t\n\n',
 '\t\n\n\t\n\t\n\n\t\n\t\n\t\n\n\t\n\n\t\n\n\t\n\t\t\n\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\tProtesters charged after blocking coach bound ',
 'for Bibby Stockholm \n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\n\t\t\n\t\n\n\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n',
 '\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\n\t\n\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\tTelegraph Reporters\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t4 May 2024, 1:53am\n\t\t\t\t\t\t\t\n',
 '\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\n\n\t\n\t\t\n\n\t\t\t\n\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t',
 '\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\n\t\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\t\n\n\n\n\t\t\n\t\n\n\n\t\t\n\t\n\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t',
 '\n\t\t\t\t\t\t\n\t\t\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\n\t\n\t\n\t\n\n\t\n\t\n\n\t\n\t\n\n\t\n\n\t\n\n\t\n\n\t\n\t\n\n\t\n\t\n\t\n\n\t\n\n\t\n\n\t\n\t\t\n\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t',
 '\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\tCanada police lay charges over murder of Sikh leader and probe Indian ties\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\n\t\t\n\t\n\n\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t',
 '\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\n\t\n\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\tOur F',
 'oreign Staff\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t4 May 2024, 1:12am\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\n\n\t\n\t\t\n\n\t\t\t\n\t\t\t\n\t',
 '\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\n\t',
 '\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\t\n\n\n\n\t\t\n\t\n\n\n\t\t\n\t\n\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t\n\t\t\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n',
 '\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\n\t\n\t\n\t\n\n\t\n\t\n\n\t\n\t\n\n\t\n\n\t\n\n\t\n\n\t\n\t\n\n\t\n\t\n\t\n\n\t\n\n\t\n\n\t\n\t\t\n\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n',
 '\t\t\t\t\n\n\t\t\t\tKing takes on hundreds of new patronages\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\n\t\t\n\t\n\n\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t',
 '\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\n\t\n\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\tVictoria Ward\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t4 M',
 'ay 2024, 12:01am\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\n\n\t\n\t\t\n\n\t\t\t\n\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n',
 '\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\n\t\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\t\n\n\n\n\t\t\n\t\n\n\n\t\t\n\t\n\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t',
 '\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t\n\t\t\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\n\t\n\t\n\t\n\n\t\n\t\n\n\t\n\t\n\n\t\n\n\t\n\n\t\n\n\t\n\t\n\n\t\n\t\n\t\n\n\t\n\n\t\n\n\t\n\t\t\n\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t',
 '\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\tLabour’s strategy ‘won’t last’ into a general election, says Cabinet minister\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\n\t\t\n',
 '\t\n\n\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\n\t\n\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t',
 '\n\t\t\n\n\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\tJack Maidment\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t3 May 2024, 11:01pm\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\n\t\t\t\n\n\t\t',
 '\t\n\t\t\t\n\n\t\n\n\t\n\t\t\n\n\t\t\t\n\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n',
 '\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\n\t\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\t\n\n\n\n\t\t\n\t\n\n\n\t\t\n\t\n\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t\n\t\t\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\n\t\n\t\n\t',
 '\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\n\t\n\t\n\t\n\n\t\n\t\n\n\t\n\t\n\n\t\n\n\t\n\n\t\n\n\t\n\t\n\n\t\n\t\n\t\n\n\t\n\n\t\n\n\t\n\t\t\n\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t',
 '\n\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\tLuton waste chance to start great escape in draw with Everton\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\n\t\t\n\t\n\n\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\t',
 '\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\n\t\n\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\tWill Conroy\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n',
 '\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t3 May 2024, 10:53pm\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\n\n\t\n\t\t\n\n\t\t\t\n\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n',
 '\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\n\t\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\t\n\n\n\n\t\t\n\t\n\n\n\t\t\n\t\n\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t',
 '\t\n\t\t\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t\n\t\t\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\t\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\t\n\t\n\t\n\t\n\t\n\n\t\n\n\t\n\t\n\t\n\n\t\n\t\n\n\t\n\t\n\n\t\n\n\t\n\n\t\n\n\t\n\t\n\n\t\n\t\n\t\n',
 '\n\t\n\n\t\n\n\t\n\t\t\n\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\n\t\t\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\tSeven things you may have missed in the local elections\n\n\t\t\t',
 '\t\n\t\t\t\t\n\t\t\t\n\t\t\n\t\n\n\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\n\t\n\t\n\n\t\t',
 '\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\t\t\n\n\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\tDominic Penna\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t3 May 2024, 10:37pm\n\t\t\t\t\t\t\t\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n',
 '\n\t\t\t\n\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\n\n\t\n\t\t\n\n\t\t\t\n\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\n\t\t\t\t\n\t\n\t\t\n',
 '\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\t\n\t\t\n\t\t\n\t\n\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\t\n\t\t\t\n\n\t\t\n\t\n\n\n\n\t\t\n\t\n\n\n\t\t\n\t\n\n\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\n\n\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\n\n\t\t\t\t\n\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t\t\t\n\t\t\t\t\t\n\t\t\t\t',
 '\n\n\t\t\t\n\n\t\t\n\n\t\n\n\n\n\n\n\n\n    \n   \n  \n  \n   \n    \n     \n      \n       \n        The Telegraph\n       \n     \n     \n       Back to top\n       \n        \n       \n     \n     \n      Follow us on:\n      \n       \n  ',
 '       \n          Facebook icon\n         \n       \n         \n          Instagram icon\n         \n       \n         \n          X icon\n         \n       \n         \n          Snapchat icon\n         \n       \n',
 '         \n          LinkedIn icon\n         \n       \n         \n          YouTube icon \n         \n      \n     \n    \n   \n   \n    \n     \n      \n       Help Centre\n       About us\n       Telegraph Extra\n  ',
 '     Reader Prints\n       Branded Content\n       Syndication and Commissioning\n       Fantasy Sport\n       UK Voucher Codes\n       Betting Offers\n       Tax Strategy\n       Broadband and Mobile Deals\n',
 '       The Chelsea Magazine Company\n       Newsletters\n       Download the Telegraph App\n       Privacy\n       Terms & Conditions\n       Modern Slavery\n       Advertising terms\n       Guidelines\n     ',
 " \n      \n       © Telegraph Media Group Limited 2024\n      \n     \n    \n   \n  \n  \n\twindow.addEventListener( 'DOMContentLoaded', function() {\n\t\t_satellite.pageBottom();\n\t});\n\n\t\t\n\t\t\t\n\t\t\t\t\n\t\t\t\n\t \n\t\t\n\t\t\t\n\t",
 "\t\t\t\n\t\t\t\n\t\n  window.RUM_BASE = '/';\nimport { sampleRUM } from '/.rum/@adobe/helix-rum-js@^1/src/index.js';\nsampleRUM('lazy');\nsampleRUM('cwv');\n\n "]"""
    instruct = "Find relevant sentences from text_dump with given the target sentence"
    question = f"target sentence:'Adam douglas was born in Cambrige', text_dump:{sentences}"
    answer = llmQuestion(tokenizer, model, instruct, question, 8192, 8192)