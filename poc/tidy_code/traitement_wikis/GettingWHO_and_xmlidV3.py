## superceded by prepare_DepparseV3as_run_copy for preparation, and WikiExportToXMLv3 for xmlisation

from lxml import etree
import json
from tqdm import tqdm
import re

file = "/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/wikidisc_prefabv2_D_00.xml"


with open(file,'r', encoding='UTF-8') as fin:
  data = fin.read()
data = data[39:]
data_for_etree = re.sub('<pubURL value=\".+?\"/>','<pubURL value=""/>', data)
temp_file = file.replace('.xml','_test2.xml')
with open(temp_file, 'w', encoding='UTF-8') as k:
  k.write(data_for_etree)
  

letter = os.path.basename(file).replace('wikidisc_prefabv2_','')[0]
dict_path = '/Users/Data/ANR_PREFAB/F_talkPages/jsons/'
input_dict_path = dict_path + letter + '_folder_dictV2.json'
with open(input_dict_path, "r") as d:
    letter_dict = json.load(d)

#yes
def add_all_annotations(temp_file, letter_dict):
  tree = etree.parse(temp_file)
  alldets = []
  uuid_errors = []
  true_count =0
  false_count = 0
  s_blocks= tree.xpath(".//s[@uuid]")
  for b, block in tqdm(enumerate(s_blocks)):
    uuid = block.get('uuid')
    post_id = re.sub(r'(i.[0-9]+?_\d+?_\d+?).*', r'\1', uuid)
    if post_id in letter_dict.keys():
      true_count +=1
  
      talk_id, page_title, threadtitle, url, source_short, indent, user_who, user_pseudo = letter_dict[post_id]
      thread_title_tidy = threadtitle.strip()
      if thread_title_tidy in (""," "):
        thread_title_tidy = "_threadtitle_absent_"
      block.xpath('ancestor::doc//title')[0].set('value', page_title)
      block.xpath('ancestor::doc//pubURL')[0].set('value', url)    
      id_details = re.sub(r'i.[0-9]+?_\d+?_(\d+)', r'\1', uuid)
      post_no, para_no, sent_no = id_details.split('-')
      block.set("talk_id", talk_id)
      block.set("threadtitle", thread_title_tidy)
      block.set("post_id", post_id)
      block.set("post_no", post_no)
      block.set("paranum", para_no)
      block.set("sentnum", sent_no)
      block.set("indent", indent)
      block.set("user_who", user_who)
      block.set("user_pseudo", user_pseudo)
      block.set("id", str(b+1))
      
    else:
      false_count +=1
  temp_annotations = tree
  return temp_annotations

# add annotations from dictionaries to tree
annotations_tree = add_all_annotations(temp_file, letter_dict)

# not need : promotion done with xslt
#    def promote_s_to_sent(tree):
      '''
      promote and rename annotations from s blocks nested in p blocks to sent blocks and remove s blocks
      and then return a tree
      '''
      for p, p_element in tqdm(enumerate(tree.findall('.//p'))):
          # Find the <s> block inside the <p> block
          s_element = p_element.find('s')
          
          # If <s> block exists and has the "sentnum" attribute
          if s_element is not None and 'sentnum' in s_element.attrib:
              # Create a new <sent> element
              sent_element = etree.Element('sent')
              # set the id attribute to the p value +1 from the enumerator to give sequence
              sent_element.set('id', str(int(p+1)))
              # Set the "sentnum" attribute of the <sent> element to the value from the <s> block
              sent_element.set('sentnum', s_element.attrib['sentnum'])
              sent_element.set('paranum', s_element.attrib['paranum'])
              sent_element.set('uuid', s_element.attrib['uuid'])
              sent_element.set('talk_id', s_element.attrib['talk_id'])
              sent_element.set('post_id', s_element.attrib['post_id'])
              sent_element.set('post_no', s_element.attrib['post_no'])
              sent_element.set('indent', s_element.attrib['indent'])
              sent_element.set('user_who', s_element.attrib['user_who'])
              sent_element.set('user_pseudo', s_element.attrib['user_pseudo'])
              
              sent_element.text = s_element.text
              s_element.text = ""
              
              # Find the index of the <s> element in the parent <p> element
              s_index = p_element.index(s_element)
                  
              # Insert the new <sent> element before the <s> element
              p_element.insert(s_index, sent_element)        
              p_element.remove(s_element)
      return tree
#    # promote s blocks
#    s_promoted_tree = promote_s_to_sent(annotations_tree)
#    p_promoted_tree = promote_paragraphs(s_promoted_tree)
#    def promote_paragraphs(s_promoted_tree):
      '''
      promote para to be parent of sent
      '''
      root = s_promoted_tree.getroot()  # Get the root element of the tree
      for p_element in tqdm(root.findall('.//p')):
          # Find the <body> element within the <doc>
          sent_element = p_element.find("sent")
          this_index = p_element.index(sent_element)
          if sent_element is not None and 'talk_id' in sent_element.attrib and 'post_id' in sent_element.attrib and 'post_no' in sent_element.attrib :
            para_el = etree.Element('para')
            para_el.set('id', sent_element.attrib['paranum'])
            para_el.append(sent_element)
            p_element.insert(this_index, para_el)
      
      p_promoted_tree = s_promoted_tree
      return p_promoted_tree
#
#
annotations_tree.write(temp_file.replace('test2','test3'), xml_declaration=True, pretty_print=True, encoding='UTF-8')
    

# now everythign written to temp3, promote::



## take annotations tree with all annotaitons, pre pruning
tranform_file = '/Users/Data/ANR_PREFAB/Code/code_for_gitlab/traitement_wikis/transformation_stylesheet.xml'
with open(tranform_file, 'r') as xslt_file:
    xslt_content = xslt_file.read()
# Parse the XSLT stylesheet
xslt_tree = etree.XML(xslt_content)

# consolidated_s_tree.write(temp_file.replace('test2','test3trdult'), xml_declaration=True, pretty_print=True, encoding='UTF-8')

def tidy_transformed_tree(transformed_tree):
  # now blocks renamed, time to move attributes
  div_count, para_count = 0,  0
  div_els = transformed_tree.findall(".//div")
  for div_el in div_els:
    div_count +=1
    div_el.set('id', str(div_count))
    para_els = div_el.findall(".//para")
    for para_el in para_els:
      para_count +=1
      sent_els = para_el.findall(".//sent")
      for sent_el in sent_els:
        postid = sent_el.get('post_id')
        post_no = sent_el.get('post_no')
        sent_num = sent_el.get('sentnum')
        sent_id = sent_el.get('id')
        paranum = sent_el.get('paranum')
      para_el.set('id', str(para_count))
      para_el.set('paranum', paranum)
    div_el.set('post_id', postid)
  tidied_transformed_tree = transformed_tree  
  return tidied_transformed_tree  

tree_in.write(temp_file.replace('test2','test3trdult_in'), xml_declaration=True, pretty_print=True, encoding='UTF-8')


def consolidate_para_blocks(input_tree):
    '''
    consolidate para blocks from diff divs into single div
    '''
    
    # Parse the XML file
    # tree = etree.parse(xml_file_path)
    
    root = input_tree.getroot()
    
    # Create a dictionary to keep track of divs with the same post_id
    div_dict = {}

    # Iterate over all <div> elements
    for div in root.xpath('//div'):
        post_id = div.get('post_id')
        if post_id:
            if post_id not in div_dict:
                div_dict[post_id] = []
            div_dict[post_id].append(div)

    # Consolidate <para> blocks
    for divs in div_dict.values():
        if len(divs) > 1:
            # The first div where there is a match
            primary_div = divs[0]
            primary_para_blocks = primary_div.xpath('./para')

            # Iterate over the other divs with the same post_id
            for div in divs[1:]:
                para_blocks = div.xpath('./para')
                
                # Move all para blocks from current div to primary div
                for para in para_blocks:
                    primary_div.append(para)
                
                # Remove the now-empty div
                div.getparent().remove(div)

    output_tree = input_tree
    return output_tree
  
  
def consolidate_sent_blocks(input_tree):
    '''
    consolidate sent blocks from diff paras into single para
    '''
    # Parse the XML file
    root = input_tree.getroot()
    for div in root.xpath('//div'):
        # Create a dictionary to keep track of paras with the same paranum within this <div>
        para_dict = {}

        # Iterate over all <para> elements within the current <div>
        for para in div.xpath('./para'):
            paranum = para.get('paranum')
            if paranum:
                if paranum not in para_dict:
                    para_dict[paranum] = []
                para_dict[paranum].append(para)

        # Consolidate <sent> blocks within this <div>
        for paras in para_dict.values():
            if len(paras) > 1:
                # The first para where there is a match
                primary_para = paras[0]
                primary_sent_blocks = primary_para.xpath('./sent')

                # Iterate over the other paras with the same paranum
                for para in paras[1:]:
                    sent_blocks = para.xpath('./sent')

                    # Move all sent blocks from the current para to the primary para
                    for sent in sent_blocks:
                        primary_para.append(sent)

                    # Remove the now-empty para
                    para.getparent().remove(para)
    output_tree = input_tree
    return output_tree
  
def add_pseudo_to_conll(sent_el):
    conll_text = sent_el.text
    conll_as_list = conll_text.split("\n")
    user_pseudo = sent_el.attrib['user_pseudo']
    pseudo_metaline = f"# Loc={user_pseudo}"
    conll_as_list.insert(3, pseudo_metaline)
    conll_out = "\n".join([chunk for chunk in conll_as_list])
    return conll_out
  
transform = etree.XSLT(xslt_tree)
transformed_tree = transform(annotations_tree)
tidied_transformed_tree = tidy_transformed_tree(transformed_tree)
consolidated_p_tree = consolidate_para_blocks(tidied_transformed_tree)
consolidated_s_tree = consolidate_sent_blocks(consolidated_p_tree)
trim_tree_after_consolidations(consolidated_s_tree, output_file)


output_file = "/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/wikidisc_prefabv2_D_00_done.xml"
def trim_tree_after_consolidations(consolidated_s_tree, output_file):
delcount = 0
tree = consolidated_s_tree
  # process divs, tidying and deleting
div_els = tree.findall(".//div")
for d, div_el in enumerate(div_els):
  if 'post_id' in div_el.attrib:
    del div_el.attrib['post_id']
    delcount +=1
  div_el.set('id', str(int(d)+1))    
  
  
  # process paras, tidying and deleting  
para_els = tree.findall(".//para")
for p, para_el in enumerate(para_els):
  para_el.set('id', str(int(p)+1))
  if 'paranum' in para_el.attrib :
    del para_el.attrib['paranum']
    delcount +=1
  
  # process sents, tidying and deleting  
sent_els = tree.findall(".//sent")
for s, sent_el in enumerate(sent_els):
  conll_out = add_pseudo_to_conll(sent_el)
  sent_el.text = conll_out
  these_attrs = [ 'indent', 'user_pseudo', 'user_who']
  for this_attr in these_attrs:
    if this_attr in sent_el.attrib:
      del sent_el.attrib[this_attr]
      delcount +=1
  sent_el.set("id", str(int(s)+1))            
  
  auth_blocks = tree.findall(".//author")
  for auth_block in auth_blocks:
      parent = auth_block.getparent()
      parent.remove(auth_block)
      delcount +=1
  
  text_descs = tree.findall(".//textDesc")
  for td_block in text_descs:
    del td_block.attrib['sub_genre']
    delcount +=1  
    
  print(f'{delcount} attributs supprimés…') 
  tree.write(output_file, xml_declaration=True, pretty_print=True, encoding='UTF-8')
  

## not quite done, just need to change para to p, sent to s, rename attributes in textdesc, remove author

   <author value="nom_auteur_si_connu"/>
 <textDesc sub_genre="wikidiscussions" thema="" type="wikidiscussions" maxsilence="118860" schema="A@B@" Nparticipants="dialogue" Nposts="2" Nusers="2" duration="1 day, 9:01:00" duration_days="1" duration_interval="1" datedebut="2017-12-15 01:05:00" datefin="2017-12-16 10:06:00"/>

