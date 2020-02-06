""" 형태소 분석기
    명사 추출 및 빈도수 체크
"""

from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp
#create or get Spark Session

spark = sparknlp.start()

# sparknlp.version()
# spark.version

#download, load, and annotate a text by pre-trained pipeline

pipeline = PretrainedPipeline('recognize_entities_dl', 'en')
result = pipeline.annotate('Harry Potter is a great movie')
# Your testing dataset
text = """
The Mona Lisa is a 16th century oil painting created by Leonardo. 
It's held at the Louvre in Paris.
"""

# Annotate your testing dataset
result = pipeline.annotate(text)

# What's in the pipeline
print(list(result.keys()))
# Output: ['entities', 'stem', 'checked', 'lemma', 'document',
# 'pos', 'token', 'ner', 'embeddings', 'sentence']

# Check the results
print(result['token'])
#Output: ['Mona Lisa', 'Leonardo', 'Louvre', 'Paris']