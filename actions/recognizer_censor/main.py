# -*- coding:utf-8 -*-
import os

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
        with open(path) as f:
            for keyword in f:
                self.add(keyword.strip())

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

# --- 主逻辑 ---

# 假设 keyword 文件在 /proxy/actions/... 
# 或者你需要在 Dockerfile 里把 spooky_keywords 复制到固定位置
# 这里假设它和 main.py 在一起，proxy.py 的 cwd 是 /proxy/exec
# 为了安全，建议把 spooky_keywords 放在 /proxy 根目录
gfw = DFAFilter()
keyword_path = '/proxy/exec/actions/recognizer_censor/spooky_keywords'
if os.path.exists(keyword_path):
    gfw.parse(keyword_path)
else:
    # Fallback or create empty
    print("Warning: spooky_keywords file not found.")

text_content = store.fetch(['text'])['text']

word_filter, filter_count = gfw.filter(text_content, "*")

illegal = False
if filter_count >= 1:
    illegal = True

store.post('illegal', illegal)