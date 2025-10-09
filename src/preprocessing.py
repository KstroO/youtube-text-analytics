import nltk
from nltk.corpus import stopwords
from typing import List
from soynlp.tokenizer import RegexTokenizer
import regex
import emoji
import re

# nltk.download('stopwords'), download the first time
# -------- setup
stopwords_latin = set(stopwords.words('english'))
stopwords_latin.update(set(stopwords.words('spanish')))

stopwords_ko = {
        '이', '그', '저', '것', '수', '등', '들', '는', '은', '가', '에', '의', 
        '도', '로', '를', '을', '하다', '되다', '있다', '되', '하', '고', '아', 
        '이다', '것이다', '에서', '까지'
    }

other_stopwords = {'https','www','com','co', 'href', 'br', 'youtube', 'watch', 'quot', 'kyb', 'cvso','amp'}
stopwords_latin.update(other_stopwords)
stopwords_ko.update(other_stopwords)

tokenizer_ko = RegexTokenizer()

# Detect script (Hangul vs Latin)
korean_re = re.compile(r'[\u3131-\uD79D]')

def detect_script(text: str) -> str:
    """Return 'korean' if Hangul detected, else 'latin' (or 'other')."""
    if not isinstance(text, str) or text.strip() == "":
        return "other"
    return "korean" if korean_re.search(text) else "latin"

def extract_emojis(text: str) -> List[str]:
    """Return list of emojis found in the comment."""
    if not isinstance(text, str):
        return []
    
    text = regex.sub(r'<3+', '❤', text)  # normalize "<3" or "<333" into ❤

    return [char for char in text if emoji.is_emoji(char)]

def tokenize_mixed(text: str, keep_stopwords: bool=True) -> List[str]:
    """Tokenize and clean text depending on script."""
    tokens = []
    for word in text.split():
        if korean_re.search(word):  
            # Korean pipeline
            w = re.sub(r'[^ㄱ-ㅎㅏ-ㅣ가-힣]', '', word)
            tks = tokenizer_ko.tokenize(w)
            if not keep_stopwords:
                tks = [t for t in tks if t not in stopwords_ko and len(t) > 1]
        else:
            # Latin pipeline
            w = re.sub(r'[\W\d_]+', ' ', word.lower())
            tks = w.split()
            if not keep_stopwords:
                tks = [t for t in tks if t not in stopwords_latin and len(t) > 1]
        tokens.extend(tks)
    return tokens

def extract_mentions(comment: str) -> List[str]:
    """
    Extracts all mentions from a YouTube comment.
    Mentions start with '@' and are followed by valid characters (alphanumeric, underscore, or dot).
    """
    pattern = r'@[\w.]+'
    return re.findall(pattern, comment)

def extract_hashtags(comment: str) -> List[str]:
    """
    Extracts all hashtags from a YouTube comment.
    Hashtags start with '#' and are followed by alphanumeric characters or underscores.
    """
    pattern = r'#\w+'
    return re.findall(pattern, comment)