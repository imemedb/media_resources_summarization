from keywords import TextrankKeywordsExtractor

TEXT = (
    "Challenges in natural language processing frequently involve"
    "speech recognition, natural language understanding, natural language"
    "generation (frequently from formal, machine-readable logical forms),"
    "connecting language and machine perception, dialog systems, or some"
    "combination thereof."
)

KEYWORDS = ["natural language", "machine"]


def test_textrank_keywords():

    key_extractor = TextrankKeywordsExtractor()
    assert set(key_extractor.predict(TEXT)) == set(KEYWORDS)
