import os, json, re

# (DFAFilter 类的代码 保持不变，粘贴在此处)
class DFAFilter():
    def __init__(self):
        self.keyword_chains = {}
        self.delimit = '\x00'
    def add(self, keyword):
        if not isinstance(keyword, str):
            keyword = keyword.decode('utf-8')
        keyword = keyword.lower()
        chars = keyword.strip()
        if not chars:
            return
        level = self.keyword_chains
        for i in range(len(chars)):
            if chars[i] in level:
                level = level[chars[i]]
            else:
                if not isinstance(level, dict):
                    break
                for j in range(i, len(chars)):
                    level[chars[j]] = {}
                    last_level, last_char = level, chars[j]
                    level = level[chars[j]]
                last_level[last_char] = {self.delimit: 0}
                break
        if i == len(chars) - 1:
            level[self.delimit] = 0
    def parse(self, path):
        # 假设关键字文件在 /proxy/actions/recognizer_censor/spooky_keywords
        full_path = os.path.join(os.path.dirname(__file__), path)
        try:
            with open(full_path) as f:
                for keyword in f:
                    self.add(keyword.strip())
        except FileNotFoundError:
            print(f"Warning: Keyword file '{full_path}' not found. Censor will not filter anything.")
            self.add("example_bad_word") # 添加一个默认词
    def filter(self, message, repl="*"):
        if not isinstance(message, str):
            message = message.decode('utf-8')
        message = message.lower()
        ret = []
        replaced = 0
        start = 0
        while start < len(message):
            level = self.keyword_chains
            step_ins = 0
            for char in message[start:]:
                if char in level:
                    step_ins += 1
                    if self.delimit not in level[char]:
                        level = level[char]
                    else:
                        ret.append(repl * step_ins)
                        replaced += 1
                        start += step_ins - 1
                        break
                else:
                    ret.append(message[start])
                    break
            else:
                ret.append(message[start])
            start += 1
        return ''.join(ret), replaced

# --- DFAFilter 类结束 ---

# 假设关键字文件与 main.py 放在一起
gfw = DFAFilter()
gfw.parse("spooky_keywords") #

def main(event):
    text_content = event.get('text', '') #
    
    word_filter, filter_count = gfw.filter(text_content, "*") #
    
    illegal = False
    if filter_count >= 1: #
        illegal = True
    
    return {"illegal": illegal, "filter_count": filter_count} #