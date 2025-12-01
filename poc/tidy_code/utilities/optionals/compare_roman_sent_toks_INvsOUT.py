# helper to compare token counts IN and OUT of original. This now takes place as a step in the export to XML but can be run separately here

import pandas as pd
import platform, re, json, glob, io, os, stanza, conllu, glob
import numpy as np 
from tqdm import tqdm
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from lxml import etree

def import_prepare_for_stanza(input_file):
    '''
    take a conllu file and prepare for annotation with stanza, export
    Args : 
        input_file : path to file to tag
    Returns:
        source_doc : stanza document
        all_sentsout : list of sentences to pass to stanza
    outputfile : filename for stanza export
    '''
    source_doc = CoNLL.conll2doc(input_file)

    return source_doc

def get_wordcount(conll_data):
    punct_count =0
    for sentence in conll_data.sentences:
        for token in sentence.tokens:
            if (token.to_conll_text().split('\t')[3]) =="PUNCT":
                punct_count+=1
    
    count_allwords = conll_data.num_tokens
    wordcount = count_allwords - punct_count
    #print(f"All_words == {count_allwords}")
    #print(f"All_punct == {punct_count}")
    #print(f"Wordcount == {wordcount}")
    return wordcount
all_results, details = [], []
for i in tqdm(range(1, 100)):
  # if i not in ( 26, 26):
    # file with Prefab_Conllu == conllfile : parse as conllu
    # conll_file = path + filename 
    # conll_data = import_prepare_for_stanza(conll_file)

    # specify file with XML dd tags, open, parse xmltree
    # xml_file = path + filename.replace('-xml.conllu','.xml')
    if 10 > i:
      i = "0"+ str(i)
    xml_files = glob.glob(f'/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/processing_romans_tranches/sent_to_lexicoscope/GE/GE_archives_with_7000_with_sent_meta_no_linebreak/GE{i}/GEN.fr.*.xml')
    xml_file = [file for file in xml_files if "-mod.xml" not in file]
    if len(xml_file)>0:
      conll_file = f'/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/GE_with_commas/GE{i}-7000tv8.conllu'
      conll_data = import_prepare_for_stanza(conll_file)
      newfilepath = conll_file.replace('7000tv8.conllu','testXML.xml')
      xml_file = xml_file[0]
      with open(xml_file, 'r') as f:
          xml_data = f.read()
  
      tree = etree.parse(xml_file)    
      root = tree.getroot()
      s_tags = root.findall(".//s")
      xmllen = (len(s_tags))
      conll_len=  len(conll_data.sentences)
      wordcount = get_wordcount(conll_data)
      ## also need to get new word count and overwrite value in input xml
  
      words_number_element = root.findall('.//wordsNumber')
      xmlwordcount= [words_number_element[i].items() for i in range(len(words_number_element))]
      # words_number_init = words_number_element
      titles = root.findall('.//title')
      years = root.findall('.//pubDate')
      detail = [(xml_file, title.values()[0], year.values()[0], xmlw_count[0][1]) for title, year, xmlw_count in zip(titles ,years, xmlwordcount)]
      details.append(detail)
      # words_number_element.set('value', str(wordcount))
      words =0
      for this_set in xmlwordcount:
        words = words + int(this_set[0][1])
      # print(words)
      xmlwordcount = words
      conll_wordcount = conll_data.num_tokens
      delta  = 1 - (int(conll_wordcount)/int(xmlwordcount))
      result = xml_file, conll_file, xmllen, conll_len, xmlwordcount, conll_wordcount, delta
      all_results.append(result)
    # enumerate over s_tags, 

    # for s, s_tag in enumerate(s_tags):
        # set tag_.text to joined LC of conll_text for token in sent.tokens
        # s_tag.text = "\n" + "\n".join([token.to_conll_text() for token in conll_data.sentences[s].tokens]) + "\n"


    # export to xml
    # newfilepath = xml_file.replace('.fr.xml','-prefab.fr.xml')
    # tree.write(newfilepath, encoding = 'utf-8', xml_declaration=True)    



# [title.values() for title in titles]
# [year.values() for year in years]
# xml
# 
# filenames = glob.glob(path + "*.conllu")
# basenames = [os.path.basename(filename) for filename in filenames]

# View(pd.DataFrame(all_results))
df = pd.DataFrame(all_results)
df.columns = ['xml_file', 'conll_file', 'xmllen', 'conll_len', 'xmlwordcount', 'conll_wordcount', 'delta']
df.to_excel("/Users/Data/GE_comps_files.xlsx")

tidy_list = ([cell for well in details  for cell in well])
detaildf = pd.DataFrame(tidy_list)
detaildf.columns = ['xml_file', 'title_ouvrage', 'year', 'words']
detaildf.to_excel("/Users/Data/GE_comps_details.xlsx")

# detaistst = details
# for item in detaistst:
#   print([(ite[0], ite[1], ite[2], ite[3], "\n") for ite in item])
# 
# 
# for item in tidy_list:
#   print(item)
#   print("\n")
#   
# HS5,   
  
