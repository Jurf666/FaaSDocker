from googletrans import Translator
import time

translator = Translator()

# --- 主逻辑 ---

extracted_text = store.fetch(['text'])['text']

# 由于网络问题，这里保留原来的 Mock 逻辑
# translated_text = translator.translate(extracted_text, dest='en').text 
translated_text = extracted_text

time.sleep(0.1)
store.post('translated_text', translated_text)