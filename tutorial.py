
# Corpora and Corpus Information

from corporainfo import corpora_info
from corporainfo import corpus_attributes

corpora_info("/home/test/Documents/cwb/registry")

corpus_attributes("/home/test/Documents/cwb/registry", "GP_DE")


####################################################################


# Corpus Query

from query import word_query

word_query("/home/test/Documents/cwb/registry","GP_INT","Let")

from query import pos_query
pos_query("/home/test/Documents/cwb/registry","GP_INT","NOUN")

from query import cql_query
cql_query("/home/test/Documents/cwb/registry","GP_INT",'[word="CliMAtE"%c][word="Change"]')



#####################################################################




