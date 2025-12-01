## is it easier, faster, more complete to take folderdict, 7301, and make everything its own doc, then move the branches
dict_file = '/Users/Data/ANR_PREFAB/F_talkPages/jsons/Y_folder_dictV2.json'
# dict_file = 'C:/Users/Data/CorpusWikis/WikiV4/dicts/9_folder_dictV2.json'
import json
with open(dict_file, 'r', encoding='UTF-8') as f:
  exemp_dict = json.load(f)
keylist = list(exemp_dict.keys())
exemp_dict[keylist[7]]
letter, letter_dict = get_load_dict(file)
## 

# make dict with explicit allattrs same but more detailed than hetting who version
# deal with namespaces
namespaces = {
  'tei':'http://www.tei-c.org/ns/1.0',
  'xml': 'http://www.w3.org/XML/1998/namespace'
}
ns_fortitlelink = {"tei": "http://www.tei-c.org/ns/1.0"}
  # make storagelist
this_dict = {}
letter="z"
letter = letter.upper()
import glob, os, re
from tqdm import tqdm
from lxml import etree
# load users dict
with open('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/idUsers_F_talk_2019.json', 'r', encoding='UTF-8') as f:
  user_dict = json.load(f)
# source_pages = glob.glob("C:/Users/Data/CorpusWikis/WikiV4/Z/*.xml")
# outputdictfilename = 'C:/Users/Data/CorpusWikis/WikiV4/Zfoldertest.json'
source_pages = glob.glob(f'/Users/Data/ANR_PREFAB/F_talkPages/{letter}/*tei.xml')
outputdictfilename = f'/Users/Data/ANR_PREFAB/F_talkPages/{letter}_folder_dictV2test.json'
# iterate over source xml pages to add user_info to page info and consolidate to single dict of folder
for source_page in (tqdm(source_pages)):
    # parse tree
    source_short = os.path.basename(source_page).replace('.tei.xml','')
    source_tree = etree.parse(source_page)
    
    # Use XPath to find the title and target attributes
    # get title of page with titole, and type as main, target == url
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

with open(outputdictfilename, 'w', encoding='UTF-8') as p:
  json.dump( this_dict, p)







from stanza.utils.conll import CoNLL

# readin final conllu file
# input_file = 'C:/Users/Data/CorpusWikis/WikiV4/7301/Z_00-7301v3v3tv8.conllu'
input_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/7301/Z_00-7301v3v3tv8.conllu'
conll_data = CoNLL.conll2doc(input_file)

regex1 = r'(# sent_id.+?)# sent_id.+$'
s_dict = {}
# make a dict of uuid : conll sentences, with s tags for each sent
for x, sent in tqdm(enumerate(conll_data.sentences)):
  m_chunk = re.sub(regex1, r'\1', sent.comments[0])
  uuid_short = m_chunk.replace('# sent_id = ','')
  sent_attr_str = f'<s id="{str(x+1)}" uuid="{uuid_short}">'
  conlllines = f'{sent_attr_str}\n' + '\n'.join([token.to_conll_text() for token in sent.tokens]) + '\n</s>\n'
  s_dict[uuid_short] = conlllines


# these functions are good !
para_dict = organize_s_dict_by_metakey(s_dict)
post_dict = organize_para_dict_by_metakey(para_dict)
def organize_s_dict_by_metakey(s_dict):
  
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

### end of good function  
  
def organize_post_dict_by_metakey(post_dict):
  
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

thread_dict =   organize_post_dict_by_metakey(post_dict)

# now have dict with key = uuid, value = s block ready to print.
# now iterate overkeys

len(para_dict)
# now have dict of para ids and all sents in the para. on print, can add <p> tag

post_dict = {}
for key, value in zip(para_dict.keys(), para_dict.values()):
  uuid = key
  post_id = re.sub(r'(i.[0-9]+?_\d+?_\d+?)-.*', r'\1', uuid)
  if post_id in post_dict.keys():
    current_contents = post_dict[post_id]
    update = [current_contents, value]
    post_dict[post_id] = update

  else:
    post_dict[post_id] = [value]

len(post_dict)

## turn posts into threads
thread_dict = {}
for key, value in zip(post_dict.keys(), post_dict.values()):
  uuid = key
  thread_id = re.sub(r'(i.[0-9]+?_\d+?)_.*', r'\1', uuid)
  if thread_id in thread_dict.keys():
    current_contents = thread_dict[thread_id]
    update = [current_contents, value]
    thread_dict[thread_id] = update

  else:
    thread_dict[thread_id] = [value]

len(thread_dict)

# now have all conll in sents in paras
outputfilename = '/Users/Data/wirtetest2.xml'
def write_thread_dict_to_file(thread_dict, outputfilename):
  # just need to use doc cf thread, and insert headers before doc
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





s_blocks = tree.xpath('.//s[@uuid]')
for b, block in enumerate(s_blocks):
  uuid = block.get('uuid')
  post_id = re.sub(r'(i.[0-9]+?_\d+?_\d+?).*', r'\1', uuid)
  # ehen extract
  if post_id in letter_dict.keys():
    threadid,page_title, threadtitle, url, source_short, indent, user_who, user_pseudo = letter_dict[post_id]
    id_details = re.sub(r'i.[0-9]+?_\d+?_(\d+)', r'\1', uuid)
    post_no, para_no, sent_no = id_details.split('-')
    details = [uuid, thread_id, page_title, threadtitle, url,source_short, indent, user_who, user_pseudo, post_no, para_no, sent_no]
    alldets.append(details)
    # w_id = user_data[2]


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

# post_count = 2
# post_head = make_post_head(post_key, post_count)


thread_head = make_thread_head(thread_key)
  
para_head = make_para_head(para_key, para_count)
def make_post_head(post_key, post_count):
  post_head = f'\n<div postid="{post_key}" id="{post_count}">'
  
  return post_head


def make_thread_head(thread_key):
  """
  concatenate the necessary strings and string literals to make a complete header block for a thread
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
  para_head = f'\n<p paraid="{para_key}" id="{para_count}">'

  return para_head

