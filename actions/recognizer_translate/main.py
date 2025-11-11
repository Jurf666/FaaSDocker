from googletrans import Translator
import os, json
import time

translator = Translator() #

def main(event):
    extracted_text = event.get('text', '') #
    
    if not extracted_text.strip():
        return {"translated_text": ""}

    try:
        # 注意：googletrans 库可能需要访问外网
        translated_text = translator.translate(extracted_text, dest='en').text #
    except Exception as e:
        print(f"Translate Error: {e}. Defaulting to original text.")
        translated_text = extracted_text #

    return {"translated_text": translated_text}