# don't_run ; this is version 1.2
from stanza.utils.conll import CoNLL
# # input_file = 'C:/Users/Data/PREFAB/corpusV3prep/SN38-7230v2tv8.conllu'
# # input_file = 'C:/Users/Data/PREFAB/corpusV3prep/NUM/322/322-6230v2tv8.conllu'
# input_doc = CoNLL.conll2doc(conll_file)
# greek_letters_raw = ['α', 'β', 'γ', 'δ', 'ε', 'ζ', 'η', 'θ', 'ι', 'κ', 'λ', 'μ', 'ν', 'ξ', 'ο', 'π', 'ρ', 'σ', 'τ', 'υ', 'φ', 'χ', 'ψ', 'ω']
# # get all sents where a greek letter appears in id == all sents alpha and alpha plus sequents
# sent_integers = []
# for s, sentence in enumerate(input_doc.sentences):
#   if sentence.comments[0][-1] in greek_letters_raw:
#     sent_integers.append(s)
# 
# # filter to remove cases where only alpha prsent
# filtered_ints = []
# for i in range(1, len(sent_integers)):
#     if sent_integers[i] - sent_integers[i-1] == 1 or sent_integers[i-1] - sent_integers[i] == 1:
#         filtered_ints.extend([sent_integers[i-1], sent_integers[i]])
# tidy_list = list(set(filtered_ints))
# tidy_list = sorted(tidy_list)
# s=0 
# print_me = []
# 
# while s < len(input_doc.sentences):
#   sentence = input_doc.sentences[s]
#   std_out = '\n\n\n' + sentence.comments[0]+ "\n"+ "\n".join([token.to_conll_text() for token in input_doc.sentences[s].tokens])+ '\n' # std for first item
#   next_sent = input_doc.sentences[s+1]
#   dd_id = sentence.comments[-1].replace('# s_id_dd = ','')
#   sent_idnext = next_sent.comments[0].replace('# sent_id = ','')
# 
#   if sent_idnext.startswith(dd_id) and s not in tidy_list:
#     # if we have a greek sentence, make a list of s
#     greeklist, counter = [s],0
#     while s+1 in tidy_list:
#       greeklist.append(s)
#       s+=1
#       counter +=1
#     s = s + counter
#     output = ""
#     for g in greeklist:
#       sent_int = "\n".join([token.to_conll_text() for token in input_doc.sentences[g].tokens])+ '\n'
#       output = output + sent_int
#     print_me.append(output)
#   else:
#     print_line = std_out
#     print_me.append(print_line)
#     s+=1
# len(print_me)    
# 
# wrutetest = 'C:/Users/Data/PREFAB/corpusV3prep/SN38-writetest.conllu'
# with open(wrutetest, 'w', encoding='UTF-8') as w:
#   for chunk in print_me:
#     _ = w.write(chunk)
# 
# print_me[48]
# if 
# 
# def process_greek_sents(s):
#   while (s + counter) in tidy_list:
#   
  


#write to XML update
#1. umport conll, make dict
#2. use dict to add s_text and add children to s
#3. promote children
#4. renumber and export
#simpler : iterate over sents
#open doc, wipe s.text
#then, for sentiid_dd, use sentidDD as sent to which to append txt to any existingtxt
#from tqdm import tqdm


    #### 11k sent/sec
#len(dictforoutput)
#len()
#error of sent in source :: s14711, s33367, s36553 in SN38 :: all present in 6130,all gone in 6230 : need to ensure alpha, beta etc are inserted when beta exists : at moment excluded
#from lxml import etree
# input_tree_file = 'C:/Users/Data/PREFAB/corpusV3prep/NUM/322/amélie nothomb_stupeur et tremblements-PREFABv2.fr.xml'



#print(errorcount)
#print(successcount)
# 500k /s

## tidy run section
input_conllfile = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/target/162-7230v2v2tv8.conllu'
input_tree_path = input_conllfile.replace('-7230v2v2tv8.conllu','/').replace("target/",'NUMv3/')
input_tree_file = glob.glob(input_tree_path + "*PREFABv2*xml")[0]
output_file_path = input_tree_file.replace('PREFABv2','PREFABv3')

dictforoutput = make_dict_for_sblocks(input_conllfile)
tree = add_s_text_from_dict(dictforoutput, input_tree_file)
promoted_tree = promote_s_blocks(tree)
tidy_tree = sequentialise_ids(promoted_tree)
export_tree(tidy_tree, output_file_path)

## tidy functions
def make_dict_for_sblocks(input_conllfile):
  '''
  take a conllu doc from end of annotation pipeline and make dictionary of ids,strings to write to sblocks in XML
  '''
  ## import and parse conll file
  input_doc = CoNLL.conll2doc(input_conllfile)
  # make a blank dictionary to start from
  dictforoutput = {}

  # iterate over sentences:
  for sentence in tqdm(input_doc.sentences):
    # using the dd_id which is the constant reference to the source sentence, and trim
    sent_id = sentence.comments[-1].replace('# s_id_dd = ','')
    # dev_version of string to print : LC + header of with dd_id to facilitate matching IDs
    # are you sure you want to run this line # sent_as_conllu = '\n\n' + sentence.comments[-1] + '\n' + '\n'.join([token.to_conll_text() for token in sentence.tokens]) + '\n'
    
    # production version : add line breaks before sentences sent to conll strings ready to print
    sent_as_conllu = '\n\n' + '\n'.join([token.to_conll_text() for token in sentence.tokens]) + '\n'
    # add key,sent to dictionary if the key is not present
    if sent_id not in dictforoutput.keys():
      dictforoutput[sent_id] = [sent_as_conllu]
    # if the key is present, in the dictionary, convert to list, then append current sent to existing sent as list, then write this list as the value for the key
    else:
      existingsent = [dictforoutput[sent_id]]
      existingsent.append(sent_as_conllu) 
      dictforoutput[sent_id] = existingsent
  return dictforoutput

def add_s_text_from_dict(dictforoutput, input_tree_file):
  #import tree, get s elements
  tree = etree.parse(input_tree_file)
  s_els = tree.findall('.//s')
  # len(s_els)
  errorcount = 0
  # successcount = 0
  # len1count, len2ormorecount = 0,0
  # # s_el = s_els[233]
  # enumerate over s_elements
  for i, s_el in tqdm(enumerate(s_els)):
    # overwrite any text presnt
    s_el.text = ""
    # get id of current s block, 
    s_id = s_el.get('id')
    # verify if this ID is in the keys
    if s_id in dictforoutput.keys():
      # the values in the dictionary are lists. if the list is 1 item long, we have 1 sentence to process
      if (len(dictforoutput[s_id])) ==1:
        # use the 0th item in the list to write the s_el.text
        s_el.text = dictforoutput[s_id][0]
      # if the list is more than 1 item long, we have the original sentence to process as well as the new sentences split off from it
      if (len(dictforoutput[s_id])) >1:
        s_el.text = dictforoutput[s_id][0][0] # get first sent and write it specifying the 0th item in this list
        # then iterate over items 1 to the end of the list, adding a s_block as a child, with the item x in the list as the text
        for x in range(1, len(dictforoutput[s_id])):
          new_s_block = etree.Element('s')
          new_s_block.text = dictforoutput[s_id][x]
          s_el.append(new_s_block)

    ## if the s_id is not in the keys, increment the errorcount by 1
    else:
      errorcount +=1
      print(f'error at {s_id}, errs = {errorcount}')

  return tree

def promote_s_blocks(tree):
  '''
  find cases where an s block has a child/children that are s blocks. if so, promote the children to the same level as the parents
  '''
  # Find all <s> blocks in the XML tree
  s_blocks = tree.findall('.//s')
  
  # Iterate over each <s> block
  for s_block in s_blocks:
    # Find all <s> children of the current <s> block
    child_s_blocks = s_block.findall('./s')
    id_to_inherit = s_block.get("id")
    # If there are child <s> blocks, promote them to be siblings
    for child_s_block in child_s_blocks:
      # Insert the child <s> block as a sibling after the current <s> block
      child_s_block.set("id", id_to_inherit)
      s_block.addnext(child_s_block)
    
  return tree

def sequentialise_ids(tree):
  '''
  take a tree and iterate over s blocks, adding a newID attribute, setting this to a counter iterating over s blocks to yield sequential ids as ints
  '''
  s_blocks = tree.findall('.//s')
  counter=1
  for s_block in s_blocks:
    s_block.set('newID', str(counter))
        
        # Increment the counter for the next <s> block
    counter += 1
  return tree

def export_tree(tidy_tree, output_file_path):

  tidy_tree.write(output_file_path, encoding='UTF-8', xml_declaration=True, pretty_print=True)  



## alignement :
import pandas as pd
from lxml import etree
# make dict from xml with key value == oldID,newID, 
tidy_tree = etree.parse('/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/NUMv3/162/jean-christophe grange_les rivières pourpres-PREFABv3.fr.xml')
s_blocks = tidy_tree.findall(".//s")
tree_results = {}
for q, s_block in enumerate(s_blocks):
  orig_id = s_block.get("id")
  new_id = s_block.get("newID")
  tree_results[orig_id] = new_id
  
# fichier contenant l'alignment FR_DE pour les rivières pourpres
alignment_file = '/Users/Data/alignement_pourpores.tsv'
# import as pd df, nommer le colonnes
alignment_data = pd.read_csv(alignment_file, sep="\t", header=None)
alignment_data.columns = ['FR_source','DE']

# fonction pour une liste de valeurs pour
def transform_to_int_list(value):
    # Check if the value is empty (None, NaN, or an empty string)
    if pd.isna(value) or value == '':
        return []
    
    # If the value is a single integer, return it as a list
    if isinstance(value, int):
        return [value]
    
    # If the value is a string of integers separated by commas or spaces
    if isinstance(value, str):
        # Split the string by commas or spaces, then convert each to an integer
        return [int(x) for x in value.replace(',', ' ').split()]
    
    # If the value is already a list of integers, return it as is
    if isinstance(value, list):
        return value

    # Fallback for unexpected types, return an empty list
    return []


# creer une nouvelle colonne avec les id des phrases FR en liste
alignment_data['FR_list'] = alignment_data['FR_source'].apply(transform_to_int_list)

# creer une colonne où stocker les nouvelles données
alignment_data['newIDs'] = ""

# option2 with dict 17k/s
for index, row in tqdm(alignment_data.iterrows()):
    int_list = row['FR_list']
    # créer une liste pour stockage de nouveaux ID
    new_values = []
    for item in int_list:
      # ajouter s devant l'ID
      item =  's' + str(item)
      # rechercher ce bloc dans le tree, puis obtenir le nouvel ID
      this_id = tree_results.get(item)
      new_values.append(this_id)
      # ajouter à la liste
      # if s_blocks is not None:
      #   new_values.append(new_id)
    # ajouter la liste à la df    
    alignment_data.at[index, 'newIDs'] = new_values



alignment_data.tail()

outputwritetestdoc = '/Users/Data/outputwritetestdoc.tsv'
with open(outputwritetestdoc, 'w', encoding='UTF-8') as w:
  for i in range(len(alignment_data)):
    fr_out = " ".join([str(a) for a in (alignment_data.iloc[i, 3])])
    de_out =  alignment_data.iloc[i, 1] 
    all_out = str(fr_out) + "\t" + str(de_out) + '\n'
    _ = w.write(all_out)    
    
new_values = alignment_data['FR_list'].values    
'1' in new_values
extenso_list=[]
for sublist in new_values:
  for item in sublist:
    extenso_list.append(item)
    
missing = [str(i) for i in range(1, 11218) if i not in extenso_list]

