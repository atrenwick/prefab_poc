#!/usr/bin/env python
# coding: utf-8

# # Traitement des romans : 1_Romans_prep
# # premier script : transformation xml -> conllu
# Dans ce premier ipynb, on va transformer les fichier XML source en conllu.
# Reccomendation : ce script traite environ 1 fichier par seconde sur disque dur externe. 
# Le lancement de doublons du ipynb en parallelle n'est pas nécessaire.
# 

# In[ ]:


# devnote : version 1.0 du 2024-05-24.
# devnote : separate function for special case with no <body>, <text> : see 1_2_PrepareRomans.ipynb @ end
# devnote : separate function for ces : see function `convert_ces_texts_to_prefab_conll` in 1_2_Summarise_Rom_Corpus.ipynb


# In[1]:


# imports
import os, sys, re, glob, conllu, platform, io
import pandas as pd
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from xml.etree import ElementTree as ET
from tqdm import tqdm


# In[2]:


# functions : these shouldn't need modification

def run_xml_to_conll_processors(source_file, version):
  '''
  function that calls other functions to facilitate bulk conversion of files from XML to conllu, taking account of elements present (V1 looks for tc elements, V2 doesn't)
  args :
    input : source_file : absolute path to the file to be opened and processed
    version : number, defining whether version1 or version2 of the processor function should be used. can be integer or string, valid values are either 1 or 2.
      Version1 looks for p,s, tc and t tags to get to tokens
      Version2 looks for p, s and t tags to get tokens (NO tc tags)
  returns : no return object, exports tempfile and outputfile
  Notes : if need version1 and version2 is used, only metalines will be present in output
  '''
  temp_file, conll_name = prepare_filenames(source_file)
  prune_tidy_xml(source_file, temp_file)
  if version =="2" or version ==2:
    convert_xml_to_prefab_conll_v2(temp_file, conll_name)
  if version =="1" or version ==1:  
    convert_xml_to_prefab_conll_v1(temp_file, conll_name)
    
def prepare_filenames(source_file):
  '''
  function to take source_name and create names for temporary file (-mod.xml suffix) and name for conllu output file
  Note : this function searches for specific patterns in the source_file ; if the pattern is not found, the temp file will be made and saved to the unmodified path, overwriting the input document.
  args :
    source_file : absolu path to source file to operate on
    returns :
      temp_file : temporary file trimmed of specific tags and elements
      conll_name : name of conll file to be exported
  
  '''
  source_file = source_file
  temp_file = re.sub('fr.xml$', 'fr-mod.xml', source_file)
  conll_name = re.sub("fr.xml$", "fr-xml.conllu", source_file)
  return temp_file, conll_name


def prune_tidy_xml(source_file, temp_file):
  '''
  function to take xml as string, and use regex replacements to remove:
    - a) < > & and similar HTML items that cause issues in parsing
    - b) XML elements that will not be used in tokenisation : <header>, <?xml>, as well as tags for <doc>, <corpus>
    - c) all lines of annotation in <d> elements, which store annotations, not tokens
    - d) HTML entities and OCR-error induced strange characters 
  Once these are removed, the XML is written to temp_file
  Args : 
    input : source_file : original XML file initially taken as input
        temp_file : temp file which has been trimmed
    Return : no return object, file written to destination
  
  '''
  with open(source_file, 'r', encoding="UTF-8") as f:
    xml_tidy = f.read()

  xml_tidy = xml_tidy.replace("&glt;", "<").replace("&gt;", ">").replace('l="&"','l="_"').replace('">&</t>','">_</t>') # Add other entities as needed, amp cf &??
  xml_tidy = re.sub('<header>\n(.+\n)+</header>', "", xml_tidy)
  xml_tidy = re.sub('<d t=.+?>',"",xml_tidy)
  xml_tidy = re.sub('"<None>"','"None"',xml_tidy)
  xml_tidy = re.sub("\<\?xml.+?>", "", xml_tidy)
  xml_tidy = re.sub("<doc>|</doc>|<corpus>|</corpus>", "", xml_tidy)
  xml_tidy = re.sub(r"\x07", "_", xml_tidy)
  xml_tidy = re.sub(r"\x04", "_", xml_tidy)
  xml_tidy = re.sub("■", "_", xml_tidy)
  with open(temp_file, "w", encoding="UTF=8") as f:
    f.write(xml_tidy)
    #print("Tidy xml file exported")


def convert_xml_to_prefab_conll_v1(temp_file, conll_name):
  '''
  Note: V1 of converter to transform tidied xml to conllu for prefab, parsing tree previously pruned and tidied
  the input file is opened and iterates over p, s, tc and t tags 
  Args :
    temp_file absolute path to temporary file to created
    conll_name : absolute path to conll_file to output
  '''
  # create list to store error files, if any
  error_files = []
  # open the file to write to
  with open(conll_name, "w", encoding="UTF-8") as conll:
    # open try loop to attempt to parse temp_file as tree. on fail == exception, add name to list, go to next
    try:
      tree = ET.parse(temp_file)
      # get the root of the tree : not used later, but nice to have for further dev
      root = tree.getroot()
      # get the body element of the tree
      body_element = tree.find('.//body')
      # get, and iterate over p elements in body
      p_element_count = len(tree.findall('.//p[@id]'))
      for p_element in body_element.iter('p'):
       if p_element_count > 0:
        # get id of p
        p_id = p_element.get('id') 
        # if is first p element, don't add new line beforehand, then write this p tag as meta
        if p_id == 1:
          balise_p = '# sent_on_page = ' + str(p_id) 
        else:
          if not (p_id is None):
           balise_p = '\n# sent_on_page = ' + str(p_id )
       if p_element_count == 0:
        balise_p = "\n"
       conll.write(balise_p)

       # Iterate over s elements within p
       for s_element in p_element.iter('s'):
        # get sent number, not adding line before if ==1, then write as meta
        s_id = s_element.get('id') 
        if s_id == 1:
          balise_sent = '# sent_id = ' + s_id +'\n'
        else:
          balise_sent = '\n# sent_id = ' + s_id +'\n'
        conll.write(balise_sent)   
        # Iterate over tc elements within s : the tc elements contain t elements, with tokens, as well as other elements, c, which contained annotations : the children of elements c have been removed as they're not needed here
        for tc_element in s_element.iter('tc'):
         # Iterate over t elements within tc, resetting sent_tokens and token_strings at start of each sentence
         # this is where tokens are stored
         sent_tokens = [] 
         # this is where lines for conllu are stored - tokens as well as tabs, _ etc
         token_strings = []  

         for t_element in tc_element.iter('t'):
          # get token id
          t_id = t_element.get('id') 
          # get token text
          t_text = t_element.text.strip() 
          # build line for conllu
          token_line = str(int(t_id)+1) + '\t' + str(t_text) + "\t_\t_\t_\t_\t_\t_\t_\t_\n"
          # send tokens to this list
          sent_tokens.append(t_text) 
          # send token line of conll here
          token_strings.append(token_line) 
         # now we've dealt with each token of sent, can make meta lines
         # sent_meta_raw is tokens separated by spaces == standard conllu meta
         sent_meta_raw = "# sent_text_raw = "+ " ".join([t_text for t_text in sent_tokens])+ "\n" 
         conll.write(sent_meta_raw)
         # sent_meta_prefab is tokens separated by spacesAND two // == allows for PLE ID and easy comparison of tokenisations
         sent_meta_prefab = "# sent_text_prefab = "+" //".join([t_text for t_text in sent_tokens]) + "\n"
         conll.write(sent_meta_prefab)
         # iterate over the list of token_strings == conllu lines for the sentence, writing them
         for token_string in token_strings:
          conll.write(token_string)
    except ET.ParseError as e:
     print(f"Parse error in {temp_file}: {e}")
     # Add the file to the error list
     error_files.append(temp_file) 
#     continue # Skip to the next iteration of the loop

def convert_xml_to_prefab_conll_v2(temp_file, conll_name):
  '''
  Note: V2 of converter to transform tidied xml to conllu for prefab, parsing tree previously pruned and tidied
  the input file is opened and iterates over p tags if present, and s then t tags 
  Args :
    temp_file absolute path to temporary file to created
    conll_name : absolute path to conll_file to output
  '''
  # create list to store error files, if any
  error_files = []
  # open the file to write to
  with open(conll_name, "w", encoding="UTF-8") as conll:
     # open try loop to attempt to parse temp_file as tree. on fail == exception, add name to list, go to next
    try:
     tree = ET.parse(temp_file)
      # get the root of the tree : not used later, but nice to have for further dev
     root = tree.getroot()
      # get the body element of the tree
     body_element = tree.find('.//body')
      # get, and iterate over p elements in body
     p_element_count = len(tree.findall('.//p[@id]'))
     for p_element in body_element.iter('p'):
       if p_element_count > 0:
        # get id of p
        p_id = p_element.get('id') 
        # if is first p element, don't add new line beforehand, then write this p tag as meta
        if p_id == 1:
          balise_p = '# sent_on_page = ' + str(p_id) 
        else:
          if not (p_id is None):
           balise_p = '\n# sent_on_page = ' + str(p_id )
       if p_element_count == 0:
        balise_p = "\n"
       conll.write(balise_p)

       # Iterate over s elements within p
       for s_element in p_element.iter('s'):
        # get sent number, not adding line before if ==1, then write as meta
        s_id = s_element.get('id') 
        balise_sent = '\n# sent_id = ' + s_id +'\n'
        conll.write(balise_sent)   
        # Iterate over tc elements within s : the tc elements contain t elements, with tokens, as well as other elements, c, which contained annotations : the children of elements c have been removed as they're not needed here
        # this is where tokens are stored
        sent_tokens = [] 
        # this is where lines for conllu are stored - tokens as well as tabs, _ etc
        token_strings = []  
        for conll_line in s_element.text.strip().split("\n"):
         # Iterate over t elements within tc, resetting sent_tokens and token_strings at start of each sentence
         # get token id
         t_id = conll_line.split("\t")[0] 
         # get token text
         t_text = conll_line.split("\t")[1] 
         # build line for conllu
         token_line = str(int(t_id)) + '\t' + str(t_text) + "\t_\t_\t_\t_\t_\t_\t_\t_\n" 
         # send tokens to this list
         sent_tokens.append(t_text) 
         # send token line of conll here
         token_strings.append(token_line) 
        # now we've dealt with each token of sent, can make meta lines
        # sent_meta_raw is tokens separated by spaces == standard conllu meta
        sent_meta_raw = "# sent_text_raw = "+ " ".join([t_text for t_text in sent_tokens])+ "\n" 
        conll.write(sent_meta_raw)
        # sent_meta_prefab is tokens separated by spacesAND two // == allows for PLE ID and easy comparison of tokenisations
        sent_meta_prefab = "# sent_text_prefab = "+" //".join([t_text for t_text in sent_tokens]) + "\n"
        conll.write(sent_meta_prefab)
        # iterate over the list of token_strings == conllu lines for the sentence, writing them
        for token_string in token_strings:
          conll.write(token_string)
    except ET.ParseError as e:
     print(f"Parse error in {temp_file}: {e}")
     # Add the file to the error list
     error_files.append(temp_file) 
#     continue # Skip to the next iteration of the loop



def import_parse_prefab_connl(input_file):
 """import and parse connlu file to list of lists
 Args:
  input_file: path to a connlu file
 Returns:
  parsed_data : data parsed from the connlu
 """
 with open(input_file, "r", encoding='UTF-8') as f:
   conllu_data = f.read()
 
 parsed_data = conllu.parse(conllu_data)
 
 return parsed_data

def write_sample_conllu_for_stanza_from_roman(all_target_data, outputfile):
 '''
 write data to conllu 
  Args: 
   outputfile : path to file to output
   all_target_data : parsed conllu data == lists of tokens parsed as tokenlists   
  Returns:
   file at specified filepath
 '''
 with io.open(outputfile, 'w', encoding='UTF-8') as f:
  ## global.columns = ID FORM LEMMA XPOS UPOS feats HEAD DEPREL NONE1 NONE2

  for s in range(len(all_target_data)):
   current_sentence = all_target_data[s]
   id_string = "\n#sent_id = " + str(current_sentence[0])
   meta_text_raw = "\n#text_raw = " + current_sentence[1].metadata['sent_text_raw']
   meta_text_pf = "\n#text_pf = " + current_sentence[1].metadata['sent_text_prefab'] + "\n"
   sent_meta = id_string + meta_text_raw + meta_text_pf
   f.write(sent_meta)
   tokenlist = current_sentence[1]
   for m in tokenlist:
    outputstring = str(m['id']) + '\t' + m['form']+ '\t_\t_\t_\t_\t_\t_\t_\t_\n'
    f.write(outputstring)
  
  f.close()

## end of functions
############################################################################################
  


# # start of lines needing modification for each run 

# In[13]:


### add the absolute path to the folder that contains the XML files to be taken as input
# this tells the script which folder we want to work with
folder = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/processing_romans_tranches/sent_to_lexicoscope/PO/PO_archives_with_7000_with_sent_meta_no_linebreak/PO37/'


# In[16]:


# specify the file extension and any elements on the end of filenames that we want to process
# Note1 : * indicated any sequence of characters ; to add searching in a subfolder, add /*/
source_files = glob.glob(folder + '*.xml')
print(len(source_files))


# In[17]:


# 1.3 For each source file, run the function that runs the processors which get, tidy and export the conllu files
### if XML files are XML, specify 1 in line 245. If XML files are xml-conllu, specify 2 in line 245
for source_file in tqdm(source_files):
  run_xml_to_conll_processors(source_file, 2)

############################################################################################
################ end of lines needing modification for each run ############################
############################################################################################
  


# # End of this notebook. We have now taken XML files and converted them to conllu. The next step is retokenisation : go to notebook 4_Retokenisation
