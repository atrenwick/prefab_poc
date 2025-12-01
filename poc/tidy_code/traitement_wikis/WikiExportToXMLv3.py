# new procedure to render xml for wikidiscissions
# imports
import json, re, os
import pandas as pd
from lxml import etree
from stanza.utils.conll import CoNLL

# functions
def make_s_dict_from_conll_data(conll_data):
  """
  make a dictionary of all the conll lines of a sentence with the sentid as key
  inputs : conll_data : a parsed conllu doc
  outputs : s_dict, a dictionary of sentid:value where value is the concatenation of all the conll_to_text strings for the sentence
  """
  regex1 = r'(# sent_id.+?)# sent_id.+$'
  s_dict = {}
  # make a dict of uuid : conll sentences, with s tags for each sent
  for x, sent in tqdm(enumerate(conll_data.sentences)):
    m_chunk = re.sub(regex1, r'\1', sent.comments[0])
    uuid_short = m_chunk.replace('# sent_id = ','')
    sent_attr_str = f'\n<s id="{str(x+1)}" uuid="{uuid_short}">'
    conlllines = f'{sent_attr_str}\n' + '\n'.join([token.to_conll_text() for token in sent.tokens]) + '\n</s>\n'
    s_dict[uuid_short] = conlllines
  return s_dict 

def organize_s_dict_by_metakey(s_dict):
    """
    sort sentences into a dictionary of paragraphs, where keys are paraIDs and values are lists of dictionaries of sentences of that paragraph
    """
    original_dict = s_dict
    # New dictionary to store results based on the metakey
    organized_dict = {}
    
    # Iterate over the original dictionary
    for key, value in original_dict.items():
        # Generate the metakey for the current key
        para_id = re.sub(r'(i.[0-9]+?_\d+?_\d+?-\d+)-.*', r'\1', key)
        metakey = para_id

        # Create a new dictionary for the current key:value pair
        holding_dict = {key: value}
        
        # Check if the metakey exists in the organized_dict
        if metakey not in organized_dict:
            # If not, create a new list containing the current moo_dict
            organized_dict[metakey] = [holding_dict]
        else:
            # If it exists, retrieve the current list and append the new moo_dict
            current_list = organized_dict[metakey]
            current_list.append(holding_dict)
            organized_dict[metakey] = current_list  # Reassign the updated list
    para_dict  = organized_dict
    return para_dict

def organize_para_dict_by_metakey(para_dict):
    """
    sort paragraph dictionaries into post_dictionaries, wich all paragraphs from a post going to a dict for this post
    """
    original_dict = para_dict
    # New dictionary to store results based on the metakey
    organized_dict = {}
    
    # Iterate over the original dictionary
    for key, value in original_dict.items():
        # Generate the metakey for the current key
        post_id = re.sub(r'(i.[0-9]+?_\d+?_\d+?)-.*', r'\1', key)
        metakey = post_id

        # Create a new dictionary for the current key:value pair
        holding_dict = {key: value}
        
        # Check if the metakey exists in the organized_dict
        if metakey not in organized_dict:
            # If not, create a new list containing the current moo_dict
            organized_dict[metakey] = [holding_dict]
        else:
            # If it exists, retrieve the current list and append the new moo_dict
            current_list = organized_dict[metakey]
            current_list.append(holding_dict)
            organized_dict[metakey] = current_list  # Reassign the updated list
    post_dict  = organized_dict
    return post_dict

def organize_post_dict_by_metakey(post_dict):
    """
    organise the post_dicts into a dictionary of threads, with threadID as key and value as list of dictionaries for posts
    """
    original_dict = post_dict
    # New dictionary to store results based on the metakey
    organized_dict = {}
    
    # Iterate over the original dictionary
    for key, value in original_dict.items():
        # Generate the metakey for the current key
        thread_id = re.sub(r'(i.[0-9]+?_\d+?)_.*', r'\1', key)
        metakey = thread_id

        # Create a new dictionary for the current key:value pair
        holding_dict = {key: value}
        
        # Check if the metakey exists in the organized_dict
        if metakey not in organized_dict:
            # If not, create a new list containing the current moo_dict
            organized_dict[metakey] = [holding_dict]
        else:
            # If it exists, retrieve the current list and append the new moo_dict
            current_list = organized_dict[metakey]
            current_list.append(holding_dict)
            organized_dict[metakey] = current_list  # Reassign the updated list
    thread_dict  = organized_dict
    return thread_dict

def write_thread_dict_to_file(thread_dict, outputfilename):
  '''
  write a thread to file by passing in thread_dict and name of output file
  args :
    thread_dict : dict in which keys are thread ids and values are lists of dicts for posts within these threads
    outputfilename : full path to file to be written as xml with all annotations
  '''
  with open(outputfilename, 'w',encoding='UTF-8') as z:
    # for each output xml file, write opening declaration
    _ = z.write(univ_head)
    # initialise counts for posts, paras. these will be printed in xml ; s values already exist
    post_count, para_count =0,0
    # for information purposes, won't be printed:
    sent_count, thread_count = 0,0
    # iterate over threads with the thread_dict
    for thread_key, thread_value in zip(thread_dict.keys(), thread_dict.values()):
      # for each thread, write a thread_header
      thread_head = make_thread_head(thread_key)
      _ = z.write(thread_head)
      thread_count +=1
      # iterate over posts within each thread using the dict(s) in thread_value
      for post_dict in thread_value:
        for post_key, post_value in zip(post_dict.keys(), post_dict.values()):
          # increment counter, make head, write head
          post_count +=1
          post_head = make_post_head(post_key, post_count)
          _ = z.write(post_head)
          # iterate over paragraphs within each post using the dict(s) in post_value
          for para_dict in post_value :
            for para_key, para_value in zip(para_dict.keys(), para_dict.values()):
              # increment counter, make head, write head
              para_count +=1
              para_head = make_para_head(para_key, para_count)
              _ = z.write(para_head)
              # iterate over sents within each para:
              for sent_dict in para_value:
                for sent_key, sent_value in zip(sent_dict.keys(), sent_dict.values()):
                  # add a linebreak to the start of sent value to make easier to read: sents are prepared as strings with necessary linebreaks present after tokens
                  line_to_print = "\n" + sent_value
                  _ = z.write(line_to_print)
                  sent_count +=1
                  # sent_value already contains the closing tag for the s block, so no need to add it here
              # write the footer of the para, p,  block
              _ = z.write(para_foot)
          #write the footer for the post, div, block
          _ = z.write(post_foot)
      # write the footer for the thread, doc, blocl
      _ = z.write(thread_foot)
    # write the footer for the entire xml file
    _ = z.write(univ_foot)
  report = f"{os.path.basename(outputfilename)} :: Printed {thread_count} threads : {post_count} posts : {para_count} paras : {sent_count} sents "        
  print(report)

def make_post_head(post_key, post_count):
  '''
  helper function to make a div element to serve as the header of each post, inserting the key and the post_count
  '''
  post_head = f'\n<div postid="{post_key}" id="{post_count}">'
  
  return post_head

def make_thread_head(thread_key):
  """
  concatenate the necessary strings and string literals to make a complete header block for a thread : the thread header is a TEI block
  Args :
  Input :
  thread_key 	: thread_id used as key in dict of threads
  
  Return : thread_head : XML for the header block of a thread, ready for export, as a string, 
  """
  ## head_chunk1 is the first of a series of string literals used to create the header, from TEI.2 declaration to the title
  head_chunk1 ='''<TEI.2>\n<doc>\n<header>\n<fileDesc>\n<titleStmt>\n<title threadtitle="_thread_title_" pagetitle="_page_title_"/>\n<author value="_author_name_"/>\n</titleStmt>\n<publicationStmt>\n<publisher value=""/>\n<pubPlace value=""/>\n<pubDate value=""/>\n<pubURL value="_url_"/>\n<idno type="wikipedia-id_pf">'''
  
  ## head_chunk4 is the 2nd of a series of string literals used to create the header
  head_chunk2 = '''</idno>\n</publicationStmt>\n<formatSource value=""/>\n</fileDesc>\n<profileDesc>\n<langUsage>\n<language ident="fr"/>\n</langUsage>\n<textDesc type="wikidiscussions" maxsilence="_silence_" schema="_schema_" Nparticipants="_type_interaction_" Nposts="_nposts_" Nusers="_nusers_" duration="_dur_hms_" duration_days="_dur_days_" duration_interval="_dur_interval_" datedebut="_dt_start_" datefin="_dt_fin_"/>\n<annotation value=""/>\n<wordsNumber value=""/>\n</profileDesc>\n</header>\n<text>\n<body>\n\n'''
  
  
  thread_head = str(head_chunk1)  + str(thread_key) + str(head_chunk2) 
  return thread_head

def make_para_head(para_key, para_count):
  '''
  helper funciton to make a p block to serve as a header for the para level annotations, inserting key and count
  '''
  para_head = f'\n<p paraid="{para_key}" id="{para_count}">'

  return para_head

def run_conll_to_basicxml_exporter(input_file):
  # get as conll_data
  print(f"Loading {os.path.basename(input_file)}")
  conll_data = CoNLL.conll2doc(input_file)
  # make s_dict
  s_dict = make_s_dict_from_conll_data(conll_data)
  # make para_dict
  para_dict = organize_s_dict_by_metakey(s_dict)
  # make post_dict
  post_dict = organize_para_dict_by_metakey(para_dict)
  # make thread_dict
  thread_dict = organize_post_dict_by_metakey(post_dict)
  
  # we now have a dictionary of all the threads for the current conll file
  # we now can pass this to the writer, which will add the xml tags we need to get a basic tree
  outputfilename= input_file.replace('/7301/', '/xml/').replace('-7301v3v3tv8.conllu','.xml')
  write_thread_dict_to_file(thread_dict, outputfilename)

def make_text_desc_dict():
  '''
  make a dictionary of id:data from the XLSX from CP on the Wikicorpus
  keys : thread_id
  values : list of URL, THREAD_TITLE, page_title, maxsilence_s, schema, Nparticipants	Nposts	Nusers	duration	duration_days	duration_interval	dateDebut	dateFin
  '''
  metadata_file = '/Users/Data/ANR_PREFAB/Data/Corpus/Wikis/metadata/EFG-FR-lexicoscope-metadata-2024-03-29.xlsx'
  metadata = pd.read_excel(metadata_file)
  text_desc_dict = {row['id']: row.iloc[2:].tolist() for _, row in tqdm(metadata.iterrows())}
  return text_desc_dict

def make_dict_from_source_xml(letter, user_dict):
  '''
  make a dict of url, title; thread id; who; whopseudo for each post and export, operating over folders of source xml
    keys : post_ids
    values: thread_id, page_title, thread_title, URL, source_file, indent_of_post, user_who, user_pseuro 

  '''
  # deal with namespaces
  namespaces = {
    'tei':'http://www.tei-c.org/ns/1.0',
    'xml': 'http://www.w3.org/XML/1998/namespace'
  }
  ns_fortitlelink = {"tei": "http://www.tei-c.org/ns/1.0"}
  # make storagelist
  this_dict = {}
  if len(letter)==0:
    letter = letter.upper()
  # specify folder of files over which to iterate
  source_pages = glob.glob(f'/Users/Data/ANR_PREFAB/F_talkPages/{letter}/*tei.xml')
  # specify savename for output file; use full path
  outputdictfilename = f'/Users/Data/ANR_PREFAB/F_talkPages/{letter}_folder_dictV2.json'
  for source_page in (tqdm(source_pages)):
    source_short = os.path.basename(source_page).replace('.tei.xml','')
    # parse tree
    source_tree = etree.parse(source_page)
    
    # Use XPath to find the title and target attributes
    # get title of page with title, and type as main, target == url
    title = source_tree.xpath('//tei:analytic/tei:title[@type="main"]', namespaces=ns_fortitlelink)
    # get the title and tidy
    if title:
      page_title = title[0].text.strip()
      page_title = re.sub('^Discussion:', '', page_title)
      # if there is no page title left after trimming, return absent
      if not page_title:
        page_title = '_absent_'
    # if there is no title, return absent
    if not title:
      pagetitle = '_absent_'
    
    
    target = source_tree.xpath('//tei:analytic/tei:ref/@target', namespaces=ns_fortitlelink)
    # if there is a URL, tidy it ; if not, return absent
    if target:
      url = target[0].strip()  
    else:
      url = '_absent_'
    #get posts 
    post_elements = source_tree.xpath('//tei:post', namespaces=namespaces)

    # loop over posts getting attributes
    for post in (post_elements):
      threadid = post.getparent().get('{http://www.w3.org/XML/1998/namespace}id')
      # get preceding sibling of post, which is head, and get text == thread title
      threadtitle = post.getprevious().text
      if not threadtitle:
        threadtitle = '_absent_'
      indent = post.get('indentLevel')
      post_who = post.get('who')
      post_whoname = user_dict[post_who]
      # when = post.get('when-iso')
      # xmlid = post.get('xml:id')
      post_xmlid = post.get('{http://www.w3.org/XML/1998/namespace}id')
      result = [threadid,page_title, threadtitle, url, source_short, indent, post_who, post_whoname]
      this_dict[post_xmlid] = result

  # dump json to dict
  with open(outputdictfilename,'w', encoding='UTF-8') as f:
      json.dump(this_dict, f)

def get_load_dict(letter):
  '''
  load the dictionary of data extracted from folders by LETTER ; 
  keys : post_ids
  values: thread_id, page_title, thread_title, URL, source_file, indent_of_post, user_who, user_pseuro 
  '''
  dict_path = '/Users/Data/ANR_PREFAB/F_talkPages/jsons/'
  input_dict_path = dict_path + letter.upper() + '_folder_dictV2.json'
  with open(input_dict_path, "r") as d:
      letter_dict = json.load(d)
  return  letter_dict

def insert_final_metas(input_file, text_desc_dict, letter_dict):
  '''
  take basicxml exported file as input. this file was exported by `run_conll_to_basicxml_exporter`
  letter_dictionary will be used to add user related metas at sentence level
  text_desc_dict will be used to add metas in text_desc element
  Inputs : inputfilepath, and the two dictionaries necessary
  Outputs : an xml file, with grep controlling new name
  '''
  tree = etree.parse(input_file)

  # remove author block : remove    
  author_blocks = tree.findall(".//author")    
  if author_blocks is not None:
    for author_element in author_blocks:
      parent = author_element.getparent()  # Get the parent element of <author>
      if parent is not None:
        parent.remove(author_element)  # Remove <author> from its parent
  # iterate over threads
  for this_thread in tree.findall('.//TEI.2'):
    # get thread id
    thread_id = this_thread.findall('.//idno')[0].text
    if thread_id in text_desc_dict.keys():
      # in_metas_list.append(thread_id)
      textDescEl = this_thread.find(".//textDesc")
      text_desc_data = [str(item) for item in text_desc_dict[thread_id]]
      if 'Nparticipants' in textDescEl.attrib:
        textDescEl.attrib['Nparticipants']
      # new_url = text_desc_data[0]
      textDescEl.set('newurl',text_desc_data[0])
      # thread_titlenew = text_desc_data[1]
      textDescEl.set('thread_titlecheck',text_desc_data[1])
      # talk_titlenew = text_desc_data[2]
      textDescEl.set('thread_titlecheck',text_desc_data[2])
      # maxsilencenew = text_desc_data[3]
      textDescEl.set('maxsilence',text_desc_data[3])
      # schemanew = text_desc_data[4]
      textDescEl.set('schema',text_desc_data[4])
      # typeInteraction = text_desc_data[5]
      textDescEl.set('typeInteraction',text_desc_data[5])
      # npostsNew = text_desc_data[6]
      textDescEl.set('Nposts',text_desc_data[6])
      # nUsersnew = text_desc_data[7]
      textDescEl.set('Nusers',text_desc_data[7])
      # durationnew  = text_desc_data[8]
      textDescEl.set('duration',text_desc_data[8])
      # durationdaysnew =text_desc_data[9]
      textDescEl.set('duration_days',text_desc_data[9])
      # durationinvervalnew=text_desc_data[10]
      textDescEl.set('duration_interval',text_desc_data[10])
      # datedebutnew = text_desc_data[11]
      textDescEl.set('datedebut',text_desc_data[11])
      # dateFinnew = text_desc_data[12]
      textDescEl.set('datefin',text_desc_data[12])
    # get divs over which to iterare for posts
    these_divs = this_thread.findall(".//div[@postid]")
    for current_div in these_divs:
      div_id = current_div.get('postid')
      # make list of values needed, get elements needed further down
      threadid, page_title, threadtitle, url, source_short, indent, post_who, post_whoname = letter_dict[div_id]
      header_block = current_div.xpath('ancestor::*/preceding-sibling::header')[0]
      url_block = header_block.findall('.//pubURL')[0]
      url_block.set('value', url)
      title_statement = header_block.findall('.//titleStmt')[0]
    
      
      # titles : add for page and thread unless blank
      title_block = title_statement.findall('.//title')[0]
      threadtitle = threadtitle.strip()
      if len(str(threadtitle)) >0:
        threadtitle_print = threadtitle
      else:
        threadtitle_print = "_fil_sans_titre_"
      title_block.set('threadtitle',threadtitle_print)
      title_block.set('pagetitle',page_title)
      title_block.getnext()
  
      
      # for child paras, add Loc metaline to conll
      child_paras = current_div.findall('p')
      for child_para in child_paras:
        sents = child_para.findall('s')
        for sent in sents:
          prepended_sent = f'\n# Loc={post_whoname}' + sent.text
          sent.text = prepended_sent
    
  outputfile = input_file.replace('.xml','v3.xml')
  tree.write(outputfile, pretty_print=True, xml_declaration=True,encoding='UTF-8')
  print(outputfile)

def split_tree(source_file_path, challenge_value, reports, letter):
  '''
  take a finalised xml tree with all metas inserted from both dicts. split the tree into two
  yield two trees, one with all text_desc fields == avec metas, and one where there was no text_desc_dict key match : these have placeholder metadata in text_desc.
  both trees are exported
  report printed of division of sents, paras, posts, threads between SANS and AVEC
  '''
  tree = etree.parse(source_file_path)
  root = tree.getroot()
  
  # Create a copy of the tree for the second tree
  second_tree_root = etree.Element(root.tag)  # Create a new root element for the second tree
  second_tree = etree.ElementTree(second_tree_root)
  
  # Find all <targetelements> in the tree
  target_elements = tree.findall(".//textDesc")
  
  # Loop over all <targetelements> and apply the conditions
  for target_element in target_elements:
      # Get the value of targetattribute
      target_value = target_element.get('maxsilence')
  
      # Find the <tei_chunk> ancestor of the <targetelements>
      tei_chunk = target_element.xpath('ancestor::TEI.2')[0] if target_element.xpath('ancestor::TEI.2') else None
  
      if tei_chunk is not None:
          if target_value != challenge_value:
              # If the value is not equal to challenge_value, remove <tei_chunk> from the first tree
              parent = tei_chunk.getparent()
              if parent is not None:
                  parent.remove(tei_chunk)
  
              # Add <tei_chunk> to the second tree's root
              second_tree_root.append(tei_chunk)
  
  # Write the modified trees to the output files
  first_tree_output = source_file_path.replace('v3','_sansmetas')
  second_tree_output = source_file_path.replace('v3','_avecmetas')
  tree.write(first_tree_output, pretty_print=True, xml_declaration=True, encoding='UTF-8')
  second_tree.write(second_tree_output, pretty_print=True, xml_declaration=True, encoding='UTF-8')
  treelist = [tree, second_tree]
  labels = ['sans','avec']
  for this_tree, thislabel in zip(treelist, labels):
    sents = len(this_tree.findall(".//s"))
    paras = len(this_tree.findall(".//p"))
    posts = len(this_tree.findall(".//div"))
    threads = len(this_tree.findall(".//TEI.2"))
    report = f"{letter}", thislabel, threads, posts, paras, sents  
    reports.append(report)


###############################################################################
###############################################################################
#############       END OF FUNCTION DEFINITIONS             ###################
###############################################################################
###############################################################################
## declarations
# # updates to str lits for heads, footers
univ_head = '''<?xml version='1.0' encoding='UTF-8'?>
<teiCorpus>
'''

# univ_foot is the universal footer, closing all XML documents
univ_foot = '''
</teiCorpus>
'''

thread_foot = '''</body>
</text>
</doc>
</TEI.2>\n\n'''

post_foot = '''\n</div>\n'''
para_foot = '''\n</p>\n'''


input_files = glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/7301/R*02*-7301v3v3tv8.conllu')
these_files = input_files

# input_files = these_files[2:5]
for input_file in tqdm(input_files):
  run_conll_to_basicxml_exporter(input_file)
  


####################          end of first part          ####################################
####
####
# load metadata, sent to dict for text_desc info
# dicts for usernames, letterfolder
# if need to make dicts from source_xml files are in dict_path
# functionto load dict
# dicts already made from source_xml files are in dict_path
  ''' dict_path = /Users/Data/ANR_PREFAB/F_talkPages/jsons/'''
# get dict made from source_xml,  
##### function to insert metas from folder dict and for text_desc
# source_file_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/xml/Z_00v3.xml'
# define challenge value : is maxsilence hasn't been overwritten, it will still be _silence_
######
# reports = []
# 
# exam_conllu = CoNLL.conll2doc('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/xml/done/w_sans.conllu')
# with open('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/xml/done/w_sans.text', 'w', encoding='UTF-8') as ks:
#   for sent in exam_conllu.sentences:
#     strign = " ".join([token.text for token in sent.tokens])+"\n"
#     _ = ks.write(strign)
####################################################################################################
####################################################################################################

text_desc_dict = make_text_desc_dict()
challenge_value = "_silence_"
path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/xml/done/avec_sent_meta_only_no_suffixes/'
for letter in ["B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
# letter = "B"
# reports = []
# specify letter, load dict, check length
  letter_dict = get_load_dict(letter)
  # get list of xml files relevant for letter dict
  input_files = glob.glob(path +letter + '_0*.xml')
  
  print(f"Letter == {letter} :: DictLen == {len(letter_dict)} :: files == {len(input_files)}")
  
  for input_file in tqdm(input_files):
    insert_final_metas(input_file, text_desc_dict, letter_dict)
    # pass filepath to variable for splitter
    source_file_path = input_file.replace('.xml','v3.xml')
    split_tree(source_file_path, challenge_value, reports, letter)
    print(reports[-2])
    print(reports[-1])
  
# write reportwhen done
# with open((path + "report_extras.txt"), 'w', encoding='UTF-8') as s:
#   for report in reports:
#     report_as_string = "\t".join([str(item) for item in report]) + "\n"
#     _ = s.write(report_as_string)
#     



## postprocessing to rename, renumber, remove where necessary, 

input_path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/xml/done/avec_sent_meta_only_no_suffixes/G_02_avecmetas.xml'
input_files = glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/xml/done/avec_sent_meta_only_no_suffixes/r_0*xml')

for input_path in input_files:
  tidy_textDesc(input_path)

def tidy_textDesc(input_path):
  counter = 0
  
  input_tree = etree.parse(input_path)
  desc_blocks = input_tree.findall(".//textDesc")
  for desc_block in desc_blocks:
    if 'newurl' in desc_block.attrib:
      url_value = desc_block.get('newurl')
      desc_block.set('url', url_value)
    for this_attr in ['Nparticipants', 'thread_titlecheck', 'newurl']:
      if this_attr in desc_block.attrib:
        del desc_block.attrib[this_attr]
        counter +=1
  
  d_counter = 0
  p_counter = 0
  s_counter=0
  for body_chunk in input_tree.findall('.//body'):
    for div_block in body_chunk.findall(".//div"):
      d_counter +=1
      div_block.set('id', str(d_counter))
      for para_block in div_block.findall(".//p"):
        p_counter +=1
        para_block.set('id', str(p_counter))
        for s_block in para_block.findall(".//s"):
          s_counter +=1
          s_block.set('id', str(s_counter))
      
        
  output_tree = input_tree
  output_tree.write(input_path, pretty_print=True, xml_declaration=True,encoding='UTF-8')
  print(f'{os.path.basename(input_path)} :: {counter}') 
