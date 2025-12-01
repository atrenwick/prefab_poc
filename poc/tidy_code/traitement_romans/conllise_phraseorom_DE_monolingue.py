# imports
import os, sys, re, glob, conllu, platform, io
import pandas as pd
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from xml.etree import ElementTree as ET
from tqdm import tqdm

    
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
  temp_file = re.sub(r'\.de\.(.+?).xml', r'.de.\1-mod.xml', source_file)
  conll_name = re.sub(r"\.de\.(.+?).xml", r'.de.\1-mod.conllu', source_file)
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
  Notes : replacing HTML entities present as tokens: eg &quot; present as token rather than ":
    these need to be escaped, but carefully to leave other values untouched, so use >TARGET</t> and when dealing with ", smorl value needsto be set to another value to avoid illegal """ successions
  
  
  '''
  with open(source_file, 'r', encoding="UTF-8") as f:
    xml_tidy = f.read()
  len(xml_tidy)
  xml_tidy = xml_tidy.replace("&lt;", "lt;").replace("&gt;", "gt;").replace('l="&"','l="_"').replace('">&</t>','">_</t>').replace('smorl="""','smorl="a"') # Add other entities as needed, amp cf &??
  # xml_tidy = re.sub('<header>\n(.+\n)+</header>', "", xml_tidy)
  xml_tidy = re.sub(r'<header>\n(.+?\n|\n)+?.+?</header>', "", xml_tidy)
  xml_tidy = re.sub(r'>&quot;</t>','>\"</t>',xml_tidy)
  xml_tidy = re.sub(r'>-&quot;</t>','>-\"</t>',xml_tidy)
  xml_tidy = re.sub(r'>&quot;(.+?)&quot;</t>',r'>"\1"</t>',xml_tidy)

  xml_tidy = re.sub('<d t=.+?>',"",xml_tidy)
  xml_tidy = re.sub('"<None>"','"None"',xml_tidy)
  xml_tidy = re.sub("\<\?xml.+?>", "", xml_tidy)
  # xml_tidy = re.sub("<doc>|</doc>|<corpus>|</corpus>", "", xml_tidy)
  xml_tidy = re.sub("<doc>|</doc>|<corpus>|</corpus>|<TEI.2>|</TEI.2>", "", xml_tidy)
  xml_tidy = re.sub('&','&amp;',xml_tidy)
  xml_tidy = re.sub('&amp;quot;','__',xml_tidy)
  xml_tidy = re.sub(r'smorl=""(.+?)"" ',r'smorl="\1" ',xml_tidy)  
  xml_tidy = re.sub(r'smorl="(.+?)"" ',r'smorl="\1" ',xml_tidy)  
  xml_tidy = re.sub(r'smorl=""(.+?)" ',r'smorl="\1" ',xml_tidy) 
  xml_tidy = re.sub('smorl=" e=" "','smorl="" e=" "', xml_tidy)
  xml_tidy = re.sub(r"\x07", "_", xml_tidy)
  xml_tidy = re.sub(r"\x04", "_", xml_tidy)
  xml_tidy = re.sub("■", "_", xml_tidy)
  with open(temp_file, "w", encoding="UTF=8") as f:
    f.write("<xml>")
    f.write(xml_tidy)
    f.write("</xml>")
    #print("Tidy xml file exported")
tree = ET.parse(temp_file)

error_files = []
def convert_xml_to_prefab_conll(temp_file, conll_name):
  with open(conll_name, "w", encoding="UTF-8") as conll:
          # open try loop to attempt to parse temp_file as tree. on fail == exception, add name to list, go to next
          try:
              tree = ET.parse(temp_file)
              # get the root of the tree : not used later, but nice to have for further dev
              root = tree.getroot()
              # get the body element of the tree
              body_elements = tree.findall('.//body')
              for body_element in body_elements:
                  # get, and iterate over p elements in body
                  p_element_count = len(body_element.findall('.//p[@id]'))
                  for p_element in body_element.iter('p'):
                    if p_element_count > 0 and len(p_element.findall('.//')) >0:
                      p_id = p_element.get('id')  # get id of p
                      # if is first p element, don't add new line beforehand, then write this p tag as meta
                      if  p_id == 1:
                          balise_p = '# sent_on_page = '  + str(p_id) 
                          conll.write(balise_p)
                      else:
                          if not (p_id is None):
                            balise_p = '\n# sent_on_page = '  + str(p_id )
                            conll.write(balise_p)

                    # Iterate over s elements within p
                    for s_element in p_element.iter('s'):
                      s_id = s_element.get('id')  # get sent number, not adding line before if ==1, then write as meta
                      if  s_id == 1:
                          balise_sent = '# sent_id = '  + s_id +'\n'
                      else:
                          balise_sent = '\n# sent_id = '  + s_id +'\n'
                      conll.write(balise_sent)     
                      # Iterate over tc elements within s : the tc elements contain t elements, with tokens, as well as other elements, c, which contained annotations : the children of elements c have been removed as they're not needed here
                      for tc_element in s_element.iter('tc'):
                        # Iterate over t elements within tc, resetting sent_tokens and token_strings at start of each sentence
                        sent_tokens = [] # this is where tokens are stored
                        token_strings = []    # this is where lines for conllu are stored - tokens as well as tabs, _ etc

                        for t_element in tc_element.iter('t'):
                          t_id = t_element.get('num')  # get token id
                          t_text = t_element.text.strip()  # get token text
                          token_line = str(int(t_id)+0) + '\t' +  str(t_text) + "\t_\t_\t_\t_\t_\t_\t_\t_\n" # build line for conllu
                          sent_tokens.append(t_text) # send tokens to this list
                          token_strings.append(token_line) # send token line of conll here
                        # now we've dealt with each token of sent, can make meta lines
                        # sent_meta_raw is tokens separated by spaces == standard conllu meta
                        sent_meta_raw =  "# sent_text_raw =  "+ " ".join([t_text for t_text in sent_tokens])+ "\n" # ma
                        conll.write(sent_meta_raw)
                        # sent_meta_prefab is tokens separated by spacesAND two // == allows for PLE ID and easy comparison of tokenisations
                        sent_meta_prefab = "# sent_text_prefab =  "+" //".join([t_text for t_text in sent_tokens]) + "\n"
                        conll.write(sent_meta_prefab)
                        # iterate over the list of token_strings == conllu lines for the sentence, writing them
                        for token_string in token_strings:
                          _ = conll.write(token_string)
          except ET.ParseError as e:
            print(f"Parse error in {temp_file}: {e}")
            error_files.append(temp_file)  # Add the file to the error list
  #          continue  # Skip to the next iteration of the loop
        

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
  if version == "de":
    convert_xml_to_prefab_conll(temp_file, conll_name)

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
folder = '/Volumes/2To/prefab_temp/romans_DE/phraseorom/POL/'

# In[16]:


# specify the file extension and any elements on the end of filenames that we want to process
# Note1 : * indicated any sequence of characters ; to add searching in a subfolder, add /*/
source_files = glob.glob(folder + '/*.xml')
print(len(source_files))
source_files = [file for file in source_files if '.meta.xml' not in file]
print(len(source_files))

# In[17]:


# 1.3 For each source file, run the function that runs the processors which get, tidy and export the conllu files
### if XML files are XML, specify 1 in line 245. If XML files are xml-conllu, specify 2 in line 245
for source_file in tqdm(source_files):
  run_xml_to_conll_processors(source_file, 'de')









###### now have all conllised, ready for stanza
import stanza
stanza.download(lang="de", package="gsd")

stanza.Pipeline
from stanza.utils.conll import CoNLL
test_file = '/Volumes/2To/prefab_temp/romans_DE/phraseorom/FY/FY.de.MEYER-mod.conllu'

input_doc = CoNLL.conll2doc(test_file)
len(input_doc.sentences[23])

nlp = stanza.Pipeline(lang="de", package="gsd", tokenize_pretokenized=True, tokenize_no_ssplit=True, )
annotated_doc = nlp(input_doc)

file_out = test_file.replace('mod','out')
CoNLL.write_doc2conll(annotated_doc, file_out)


###
# now have stanza analyses as conllu, insert into xml

## this script runs in two parts : 
## Part 1 inserts conllu annotations into XML and exports to file. Specific writer-loops are present for PhraseoRom texts and for aligned DE-FR texts.
## Part 2 takes the exported XML files and gets the date, title, author and wordcount of each novel/ouvrage within the XML files and makes an Excel document, to easily verify that all required texts are present


# imports
import pandas as pd
import platform, re, json, glob, io, os, stanza, conllu, glob
import numpy as np 
from tqdm import tqdm
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from lxml import etree


log = []
working_suffix = '-7300v2v2tv8.conllu'

## get list of items to process as chunks
item_list = glob.glob(conll_path + "*.conllu")
item_list =[ item.replace(conll_path,'').replace(working_suffix,'') for item in item_list]





orig_XML = '/Volumes/2To/prefab_temp/romans_DE/phraseorom/SF/SF.de.KRUSCHEL.xml'
'/Volumes/2To/prefab_temp/romans_DE/phraseorom/SF/SF.de.SCHATZING.xml'

## tidy functions

def make_de_filenames(orig_XML):
  
  de_analysed_conllu = orig_XML.replace('.xml', '_prefab_out.conllu')  
  updated_input_tree_file = orig_XML.replace('.xml','updated.xml')
  final_outputname = orig_XML.replace('.xml', '_for_prefab.xml')    

  return de_analysed_conllu, updated_input_tree_file, final_outputname

def prepare_xml_for_insertion(orig_XML, updated_input_tree_file):
  # storage for lines modified
  processed_lines = []    
  # open, readin, modify xml
  with open(orig_XML, 'r', encoding='UTF-8') as f:
    # loop over lines, removing items that cause XMLparseErrors within data to be overwritten 
    for line in f:
      tidy_line = re.sub('\"\"\"',r'"_quot_"', line)
      tidy_line = re.sub('>&</t>', '>_and_</t>',tidy_line)
      tidy_line = re.sub('l=\"&\"', r'l="and"',tidy_line)
      tidy_line = re.sub('smorl=\"&\"', r'smorl="and"', tidy_line)
      tidy_line = re.sub('&', r'_and_',tidy_line)
      tidy_line= re.sub('smorl="-""','smorl="-"', tidy_line)
      tidy_line= re.sub('smorl=""-"','smorl="-"', tidy_line)

      processed_lines.append(tidy_line)
  # once lines tidy, join, remove declaration  
  cleaned_xml = ''.join(processed_lines)
  cleaned_xml = re.sub('<\?xml version=\"1.0\" encoding=\"utf-8\"\?>', '', cleaned_xml)
  # write tidied data to file with placeholder xml tags to encompass all elements  
  with open(updated_input_tree_file, 'w', encoding='UTF-8') as w:
    _ = w.write("<teiCorpus>\n")
    for line in cleaned_xml:
      _ = w.write(line)
    
    _ = w.write("\n</teiCorpus>")
    # no return object

def make_tidy_conll_line(token):
  '''
  helper function to remove XPOS tags from printing
  '''
  raw_list = token.to_conll_text().split()
  raw_list[4] = "_"
  token_conll_line = "\t".join([item for item in raw_list])
  return token_conll_line

def make_dict_for_sblocks_by_pos(input_conllfile):
  '''
  take a conllu doc from end of annotation pipeline and make dictionary of ids,strings to write to sblocks in XML
  '''
  ## import and parse conll file
  input_doc = CoNLL.conll2doc(input_conllfile)
  # make a blank dictionary to start from
  dictforoutput = {}
  # sentence = input_doc.sentences[283+2]
  # iterate over sentences:
  for ind, sentence in enumerate(input_doc.sentences):
    # using the dd_id which is the constant reference to the source sentence, and trim
    sent_id = sentence.comments[-3].replace('# sent_id = ','')
    # dev_version of string to print : LC + header of with dd_id to facilitate matching IDs
    # are you sure you want to run this line # sent_as_conllu = '\n\n' + sentence.comments[-1] + '\n' + '\n'.join([token.to_conll_text() for token in sentence.tokens]) + '\n'
    
    # production version : add line breaks before sentences sent to conll strings ready to print
    sent_as_conllu = '\n\n' + '\n'.join([make_tidy_conll_line(token) for token in sentence.tokens]) + '\n'
    counter = str(ind+1)
    dictforoutput[counter] = [sent_as_conllu]    

  if len(dictforoutput) ==2:
    print('Erreur : only 2 dictionary entries :: are s_id_dd the [-2]th meta ?')
  
  checksums = len(input_doc.sentences), len(dictforoutput)
  return dictforoutput, checksums

def add_s_text_from_dict_by_position(dictforoutput, updated_input_tree_file):
  #updating function to iterate without match :: use position
  tree = etree.parse(updated_input_tree_file)
  s_els = tree.findall('.//s')
  xml_checksum = len(s_els)
  errorcount = 0
  # get all s elements
  # iterate over element end index:text from dict, tqdm and zippind::
  for s_el, (index, new_text) in (zip(s_els, dictforoutput.items())):
    # get text from dict by index
    s_el.text = new_text[0]
    # trim crud
    drop_these = s_el.findall('.//tc') +  s_el.findall('.//dc')
    for dropitem in drop_these:
      s_el.remove(dropitem)
  
  return tree, xml_checksum

## now tidied file ready for removal of tc, dc blocks and insertion of conll data 

# step0 : make filenames from inputfilename
de_analysed_conllu, updated_input_tree_file, final_outputname = make_de_filenames(orig_XML)
# step1 : prepare_xml 
prepare_xml_for_insertion(orig_XML, updated_input_tree_file)
# step2 : prepare conll, tidy,m 
dictforoutput = make_dict_for_sblocks_by_pos(de_analysed_conllu)
# step3 : add s text into tree  
tidy_tree = add_s_text_from_dict_by_position(dictforoutput, updated_input_tree_file)
# step4 : print
tidy_tree.write(final_outputname, encoding='UTF-8', xml_declaration=True, pretty_print=True)  

base_path = '/Volumes/2To/prefab_temp/romans_DE/phraseorom/SF/'
target_files = [file for file in glob.glob(base_path + '/*.xml') if (".meta.xml" not in file and '-mod.xml' not in file)]

for target_file in tqdm(target_files[37:]):
  de_analysed_conllu, updated_input_tree_file, final_outputname = make_de_filenames(target_file)
  prepare_xml_for_insertion(target_file, updated_input_tree_file)
  dictforoutput, checksums = make_dict_for_sblocks_by_pos(de_analysed_conllu)
  tidy_tree, xml_checksum = add_s_text_from_dict_by_position(dictforoutput, updated_input_tree_file)
  tidy_tree.write(final_outputname, encoding='UTF-8', xml_declaration=True, pretty_print=True)  
  report = os.path.basename(target_file), checksums[0],checksums[1], xml_checksum 
  log.append(report)
View(pd.DataFrame(log))
log=[]
######### after export, run this to make an inventory of what was exported and where for PhrasroRom files which can contain multiple ouvrages, but works on all XML output.


all_results, details = [], []
target_folder = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/romans/PREFAB_XML_ROMANS_ALL/NUM/'
xml_files = xml_files + glob.glob(target_folder+ '*.xml')
len(xml_files)
for xml_file in tqdm(xml_files):
   tree = etree.parse(xml_file)  
   root = tree.getroot()
   s_tags = root.findall(".//s")
   xmllen = (len(s_tags))
   words_number_element = root.findall('.//wordsNumber')
   xmlwordcount= [words_number_element[i].items() for i in range(len(words_number_element))]
   # words_number_init = words_number_element
   titles = root.findall('.//title')
   years = root.findall('.//pubDate')
   detail = [(xml_file.replace(target_folder, ''), title.values()[0], year.values()[0], xmlw_count[0][1]) for title, year, xmlw_count in zip(titles ,years, xmlwordcount)]
   details.append(detail)
   # words_number_element.set('value', str(wordcount))
  # enumerate over s_tags, 
# denest the list comprehension and export to xlsx
tidy_list = ([cell for well in details for cell in well])
detaildf = pd.DataFrame(tidy_list)
detaildf.columns = ['xml_file', 'title_ouvrage', 'year', 'words']
detaildf.to_excel("/Users/Data/Aligned_details.xlsx")
# print a sorted list of years to sure no false positives
sorted(detaildf.year.value_counts().index)

###



de_file_test = '/Users/Data/ANR_PREFAB/Code/DE_Wiktionary/de_conll_test.conll'


    
tree = etree.parse('/Users/Data/ANR_PREFAB/Code/DE_Wiktionary/de_conll_test.xml')
sents = tree.findall("s")
with open(de_file_test, 'w', encoding='UTF-8') as w:
  for sent in sents:
    sentidline1 = f"# sent_id = {sent.get('sent_id')}\n"
    sentidline2 = f"# sent_uid = {sent.get('sent_u_id')}\n"
    sentidline3 = f"# seg_u_id = {sent.get('seg_u_id')}\n"
    sentidline4 = f"# speaker = {sent.get('speaker')}\n"
    metalines = f'\n{sentidline1}{sentidline2}{sentidline3}{sentidline4}'
    _ = w.write(metalines)
    l_blocks = sent.findall("l")
    if l_blocks is not None:
      for l, l_block in enumerate(l_blocks):
        line = str(l+1) + "\t" + l_block.text + "\n" 
        _ = w.write(line)
  nlp = stanza.Pipeline(lang="de", package="gsd", tokenize_pretokenized=True, tokenize_no_ssplit=True, )

import stanza
nlp = stanza.Pipeline(lang="de", package="gsd", processors="tokenize,pos,lemma,mwt,depparse", tokenize_pretokenized=True, ss_split=True)
raw_doc = CoNLL.conll2doc(de_file_test)

annotated_doc = nlp(raw_doc)
CoNLL.write_doc2conll(annotated_doc, de_file_test.replace('test','testout'))

for sentence in raw_doc.sentences:
  for t, token in enumerate(sentence.tokens):
    
    sentence =raw_doc.sentences[1]


input_file = '/Volumes/2To/prefab_temp/romans_DE/ROMANS POL (ALL - FR)/test_texts/New Folder With Items/A-Renar-Rotschopf-JRRout.conllu'
store = []
input_doc = CoNLL.conll2doc(input_file)
end_sent = "</s>\n"
for s, sentence in tqdm(enumerate(input_doc.sentences)):
  new_id = f'<s id="{str(s+1)}">\n'  
  store.append(new_id)
  for token in sentence.tokens:
    tok_list = token.to_conll_text().split("\t")
    tok_list[4] =""
    line = "\t".join([item for item in tok_list]) + "\n"
    store.append(line)
  store.append(end_sent)

with open(input_file.replace('conllu','.xml'), 'w', encoding='UTF-8') as p:
  for chunk in store:
    _ = p.write(chunk)
