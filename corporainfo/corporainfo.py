from ccc import Corpora
from ccc import Corpus

def corpora_info(registry_path:str) -> str:
    
    corpora = Corpora(registry_path=registry_path)

    print (corpora)


def corpus_attributes(registry_path:str,corpus_name:str) -> str:
    corpus = Corpus(registry_path=registry_path, corpus_name=corpus_name)
    attributes = corpus.attributes_available
    print(attributes)
    
    
def main():
    print("Welcome to InsightsNet CWB-CCC based corpus exploration tool.")
    print("corpora_info('registry file path of IMS CWB')")
    print("corpus_attributes('registry file path of IMS CWB','Indexed Corpora')")

if __name__ == '__main__':
    main()