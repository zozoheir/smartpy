import re
from random import sample

#import edlib
import nltk
from nltk.corpus import words
from sklearn.feature_extraction.text import TfidfVectorizer

#[nltk.download(i, quiet=True) for i in ['wordnet', 'stopwords', 'punkt', 'words']]


def cleanText(text: str, remove_non_alpha=True, remove_stop_words=True, lower_case=True):
    """
    Main cleaning function for any word_list.
    Includes: tokenizing, only keep alpha, lower case, remove stop words
    """
    # split into words
    words = nltk.word_tokenize(text)
    if remove_non_alpha:
        # remove all tokens that are not alphabetic
        words = [word for word in words if word.isalpha()]
    if remove_stop_words:
        stop_words = set(nltk.corpus.stopwords.words('english'))
        words = [w for w in words if not w in stop_words]
    # lower case
    if lower_case:
        words = [i.lower() for i in words]
    return words


def removePunctuation(text):
    return re.sub(r'[^\w\s]', '', text)


def getNouns(text: str):
    return [word for (word, pos) in nltk.pos_tag(nltk.word_tokenize(text)) if 'V' not in pos[0]]


#def getLevenshteinRatioSimilarity(s1: str, s2: str):
#    return 1 - edlib.align(s1, s2)['editDistance'] / (len(s1) + len(s2))


def getMentions(strings_to_detect: list, clean_sentence: str, levenshtein_ratio_threshold):
    # Implement ngram logic
    # Iterate through strings to detect
    detected_strings = []
    for string in strings_to_detect:
        string_first_letter = string[0]
        # we filter for the first letter to optimize speed a bit
        lower_sentence = [i for i in clean_sentence.split(' ') if str(i).startswith(string_first_letter)]
        matches = []
        for sentence_word in lower_sentence:
            if getLevenshteinRatioSimilarity(string.lower(), sentence_word.lower()) >= levenshtein_ratio_threshold:
                matches.append(string.lower())
        detected_strings = detected_strings + matches
    return list(set(detected_strings))


def removeHashTags(text):
    words = text.split(' ')
    return ' '.join([i for i in words if "#" not in i])


def removeAtReferences(text):
    words = text.split(' ')
    return ' '.join([i for i in words if "@" not in i])


def generateNRandomWords(n_words):
    return ' '.join(sample(words.words(), n_words))


def getCosineSimilarity(clean_text1, clean_text2):
    tfidf = TfidfVectorizer().fit_transform([clean_text1, clean_text2])
    return ((tfidf * tfidf.T).A)[0, 1]


def getFirstLetterByWord(text):
    words = text.split()
    return [word[0] for word in words]


def hasNumbers(inputString):
    return bool(re.search(r'\d', inputString))
