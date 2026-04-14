import os
from dotenv import load_dotenv
from google import genai

load_dotenv()
client = genai.Client()
for m in client.models.list():
    if 'embed' in m.name.lower():
        print(m.name)
