## 
from lxml import etree
import pandas as pd
from tqdm import tqdm
## get concordance lines
# requete_for_lexicoscope = '''<n=1, w=A,#1>'''
# download csv file
lexicoscope_concord_file = '/Users/Data/Concordance_Table-3.csv'
lexicoscope_concord_file = '/Users/Data/PhraseoRomAndAlignedAInitialSentsConcTable.csv'
lexicoscope_concord_file = '/Users/Data/Roman_AlignedDE_AInitial.csv'
lexicoscope_concord_file = '/Users/Data/WikiV3_Concordance_Table-4.csv'
col_names_list = ["numQuery","sentId","left","node","right","author","collection","corpusId","pubdate","publisher","pubplace","source_language","sourcefilename","sub_genre","title","type","wordsnumber","year"]
all_conc_data = pd.read_csv(lexicoscope_concord_file, sep=";", usecols=[1])
View(all_conc_data.tail())

# make dict
xml_dict = {}
# loop over concord values to get filename, sentID, and add as key:value to dict
for concord_location in all_conc_data.values:
  # concord_location = all_conc_data.values[133]
  xml_file, sent_ID = concord_location[0].split('.xml_')
  xml_file = xml_file + ".xml"
  # Check if xml_file is already a key in the dictionary
  if xml_file in xml_dict:
      xml_dict[xml_file].append(sent_ID)  # Add ID to the existing list
  else:
      xml_dict[xml_file] = [sent_ID]  # Create a new entry with ID in a list
  
# now have dict with list of all files and sents needed
# for phraserorom
base_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/phraseorom_prefabV2/'

# for fr-DE 
base_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/prefab_fr-deV2/'

# for DE_fr
base_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/prefab_de-frV2/'
# for wiki
base_path = '//Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/'


full_dict = xml_dict
# get the conll from the xml files
key_list = list(xml_dict.keys())

# iterate over dict to extract conllu
with open(f'{base_path}new_sents_to_process_check_version.conllu', 'w', encoding='UTF-8') as k:
  for key in xml_dict.keys():
    target_file = base_path + key
    tree = etree.parse(target_file)
    # parse tree and get first sent id : if not s1, reindex then proceed : lexicoscope uses 0ed s_indexes
    for s, s_el in enumerate(tree.findall(".//s")):
        s_el.set('id', str("s") + str(s+1))
    
    for value in tqdm(xml_dict[key]):
      # make sentID, get conll, parse to make sent_meta_text lines, write to file for each sent      
      s_value = "s" + str(int(value)+1 +offset)  
      meta_for_conll = f"\n# sent_ID = {key}__{s_value}"
      conll_extracted = tree.xpath(f'.//s[@id="{s_value}"]')[0].text
      conll_to_parse = meta_for_conll + conll_extracted
      conll_doc = CoNLL.conll2doc(input_file=None, input_str=conll_to_parse)
      meta_textlines = "\n# texte_orig = " + " ".join([token.text for token in conll_doc.iter_tokens()]) + "\n# texte_prefab = " + " //".join([token.text for token in conll_doc.iter_tokens()])
      prepared_sent = meta_for_conll + meta_textlines + conll_extracted
      _ = k.write(prepared_sent)

errorlist = []
#### for Wiki: use conllu file ratherthan wiki
with open(f'{base_path}new_sents_to_process_check_versionPart2.conllu', 'w', encoding='UTF-8') as k:
  for key in xml_dict.keys():
    target_file = base_path + key
    target_conll_doc = target_file.replace('.xml','_after_depparse.conllu')
    if os.path.exists(target_conll_doc) is False:
      errorlist.append(target_conll_doc)
  
    if os.path.exists(target_conll_doc):
      conll_doc = CoNLL.conll2doc(target_conll_doc)
      for value in tqdm(xml_dict[key]):
            # make sentID, get conll, parse to make sent_meta_text lines, write to file for each sent      
            s_value = "s" + str(int(value) +offset)  
            meta_for_conll = f"\n# sent_ID = {key}__{s_value}"
            target_sentence = conll_doc.sentences[int(value)]
            meta_textlines = "\n# texte_orig = " + " ".join([token.text for token in target_sentence.tokens]) + "\n# texte_prefab = " + " //".join([token.text for token in target_sentence.tokens])
            conll_extracted = "\n" + "\n".join([token.to_conll_text() for token in target_sentence.tokens])+"\n"
            prepared_sent = meta_for_conll + meta_textlines + conll_extracted  
            _ = k.write(prepared_sent)      

# without concordance list
conll_target_files = glob.glob(base_path + "*after_depparse.conllu")
with open(f'{base_path}new_sents_to_process_check_version_test3.conllu', 'w', encoding='UTF-8') as k:
  for target_file in tqdm(conll_target_files):
    target_file_forKey = target_file.replace(base_path,'').replace('after_depparse.conllu','') 
    conll_doc = CoNLL.conll2doc(target_file)    
    for s, sent in enumerate(conll_doc.sentences):
      # sent = conll_doc.sentences[0]
      if sent.tokens[0].text == "A":
        meta_for_conll = f"\n# sent_ID = {target_file_forKey}_{sent.comments[0]}_sequential={s}"
        meta_textlines = "\n# texte_orig = " + " ".join([token.text for token in sent.tokens]) + "\n# texte_prefab = " + " //".join([token.text for token in sent.tokens])
        conll_extracted = "\n" + "\n".join([token.to_conll_text() for token in sent.tokens])+"\n"
        prepared_sent = meta_for_conll + meta_textlines + conll_extracted  
        _ = k.write(prepared_sent)      

len(conll_doc.sentences) == 24581 for N_01
len(conll_doc.sentences) == 140677 for N_00
== 140677 + 24581 =165258

def override_parse(target_file):
  try:
    tree = etree.parse(target_file)
    return tree
  
  except etree.XMLSyntaxError as e:
    tree = etree.XML(xml_content, etree.XMLParser(recover=True))
    
    pub_URLelement = tree.findall('.//pubURL')
    for element in pub_URLelement:
      element.text = "override"
    
    return tree



