## test replacements of specific tokens in files with encoding issues
import glob
files = ['C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/TCOF2_prefabV3/asuploaded/film_tre_15.conllu', 'C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/TCOF2_prefabV3/asuploaded/armee_mer_15.conllu']
# rawconllu = CoNLL.conll2doc("")
mytokenlist2 = []
for file in tqdm(files):
  rawconllu = CoNLL.conll2doc(file)
  for sent in rawconllu.sentences:
    for token in sent.tokens:
      if "ï¿½" in token.text:
        result = file, token.text
        mytokenlist2.append(result)

targettokens = list(set([result[1] for result in mytokenlist2]))
len(targettokens)
extras = [targettoken for targettoken in targettokens if targettoken not in uniquetokens]
uniquetokens = list(set(mytokenlist))     
len(uniquetokens)
tempfile = 'C:/Users/Data/CorpusOraux/film_tre_15.txt'
with open(tempfile, 'w', encoding='UTF-8') as t:
  for uniquetokn in extras:
    line = uniquetokn + '\n'
    t.write(line)

from lxml import etree
tree = etree.parse

with open('C:/Users/Data/armee_mer_15.xml',encoding='UTF-8') as t:
  data = t.read()
! pip install chardet  
import chardet

with open("your_file.txt", "rb") as f:
    raw_data = f.read()
    detected_encoding = chardet.detect(data)

print(detected_encoding)
repl_df = pd.read_excel('C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/TCOF2_prefabV3/asuploaded/replacements.xlsx', header=None)
repl_df.columns = ['bad','good', 'lem']
code_corr_dict = {}
for i in range(len(repl_df)):
  bad, good, lem = repl_df.iloc[i, 0], repl_df.iloc[i, 1],repl_df.iloc[i, 2]
  key = bad
  value = good, lem
  code_corr_dict[key] = value
  


  testname  = 'C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/TCOF2_prefabV3/asuploaded/film_tre_15test.conllu'
files = ['C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/TCOF2_prefabV3/asuploaded/film_tre_15.conllu', 'C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/TCOF2_prefabV3/asuploaded/armee_mer_15.conllu']
# rawconllu = CoNLL.conll2doc("")
mytokenlist2 = []
counter = 0
file = files[0]
outfile = file.replace('.conllu', 'test2.conllu')
for file in tqdm(files[0]):
with open(outfile, 'w', encoding='UTF-8') as w:  
  rawconllu = CoNLL.conll2doc(file)
  for sent in rawconllu.sentences:
    metachunk = '\n' + "\n".join([chunk for chunk in sent.comments[:-1]]) + '\n'
    _ = w.write(metachunk)
    for token in sent.tokens:
      tokenlist = token.to_conll_text().split('\t')
      if tokenlist[1] in code_corr_dict.keys():
        dict_values = code_corr_dict[tokenlist[1]]
        tokenlist[1],tokenlist[2] = dict_values[0],dict_values[1] 
      tstring_print = '\t'.join([chunk for chunk in tokenlist]) + "\n"  
      _ = w.write(tstring_print)


