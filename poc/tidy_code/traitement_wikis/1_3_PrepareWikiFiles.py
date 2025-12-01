## updated on 2024-07-19
## imports
from bs4 import BeautifulSoup
import glob, re, platform
import unidecode, os
from tqdm import tqdm
from spacy.lang.fr import French
from spacy.pipeline import Sentencizer
# initialise and add custom values to punct that ends sentences, then add sentencizer to pipe
nlp_s = French()
tokenizer_s = nlp_s.tokenizer
Sentencizer.default_punct_chars.append(";")
Sentencizer.default_punct_chars.append("…")
nlp_s.add_pipe('sentencizer')


def get_post_text(file_path, para_data):
  '''
  this is the most up-to-date version of this function as it includes the ModeleNote
  take an XML file from the wikidiscussions corpus, extract paragraph elements, tidy and return a list of posts
  Args :
  	input :
  		file_path : absolute path to file to open and process
  		para_data : list of para_data for current_batch
  	Returns :para_data : list of para_data after processing of all files in batch
  '''
  # specify connection to open
  with open(file_path, 'r') as g:
    xml_content = g.read()
  # Parse the XML into a BeautifulSoup object with the lxml parser
  soup = BeautifulSoup(xml_content, features='xml') 
  ## find elements with signed and licence tags
  signed_elements = soup.find_all('signed')
  lic_elements = soup.find_all('licence') 
  ### Remove these elements and their descendents with element.decompose()
  for element in signed_elements:
    element.decompose() 
  for element in lic_elements:
    element.decompose() 
  # not used:
  # selected_posts = soup.select("p")
  # get all the posts in the BeautifulSoup object
  posts = soup.find_all("post")

  # Iterate through each post 
  for post in posts:
    # Get the post's xml:id attribute, creator(# new), date-time(# new), who(# new), indent level(# new)
    # Get p elements of each post
    post_id = post.get("xml:id")
    post_creat = post.get("creation")
    post_dt = post.get("when-iso")
    post_who = post.get("who")
    post_indentlevel = post.get("indentLevel") 
    # Find all p elements within the current post
    paragraph_elements = post.find_all("p")
    # make a droplist by adding the integer p to the droplist of any of the conditions are met
    droplist = []
    for p, paragraph_element in enumerate(paragraph_elements):
      # get text from HTML
      post = paragraph_element.text.strip()
      # if the text starts with actu|diff, or bot-added message or has creator == bot, add to droplist
      if post.startswith("(actu | diff)") == True:
        droplist.append(p)
      if post.startswith("SVP, lisez la FaQ pour connaître les erreurs corrigées par le bot")==True:
        droplist.append(p)
      if post_creat == "bot":
        droplist.append(p)
  
    # set list of values to exclude as valid sentences even if they are missed by the post-level checks
    exclusion_list = ['', '--','--', '!--', '… ', '-', '"\'… "\'','Message déposé automatiquement par un robot le (CET).','Message déposé automatiquement par un robot le (CEST).']  
  
    #extract the text with text.strip, and use list comprehension to yield a list of sentences in each post
    post_texts = [paragraph_element.text.strip() for paragraph_element in paragraph_elements]

    ## list comprehesions to remove noise : tables, IP addresses, URLs… : these operate at the level of posts
    # here be dangers - this line will remove all accents :: #$@#post_texts = [unidecode.unidecode(post) for post in post_texts] ## danger : enabling this will remove all accents !
    post_texts = [re.sub("\(diff\).+?octets\)", "", post) for post in post_texts]
    post_texts = [re.sub("\(diff\).+?bloquer\)", "", post) for post in post_texts]
    post_texts = [re.sub("htt(p|ps):.+? ", "URL ", post) for post in post_texts]
    post_texts = [re.sub("\xa0|nbsp;|thinsp;|\x202F|\u202f|\u200a|\u200e", " ", post) for post in post_texts]
    post_texts = [re.sub("\xa0|nbsp;|thinsp;|\x202F", " ", post) for post in post_texts]
    post_texts = [re.sub("■", "", post) for post in post_texts]
    post_texts = [re.sub("Boîte déroulante\|titre=.+?\|contenu=", "", post) for post in post_texts]
    
    post_texts = [re.sub("\.\.\.", "… ", post) for post in post_texts]
    post_texts = [re.sub('\| class="wikitable".+',"",post) for post in post_texts]
    post_texts = [ re.sub("(?:[0-9]{1,3}\.){3}[0-9]{1,3} \(discuter \| bloquer\) \(\d{,3} \d{,3} octets\) ", "",post)for post in post_texts]
    post_texts = [ re.sub("(?:[0-9]{1,3}\.){3}[0-9]{1,3}", "IPADDRESS",post)for post in post_texts]    
    post_texts = [ re.sub('"\'htt(p|ps).+? (.+?"\')', "URL ", post) for post in post_texts]
    post_texts = [ re.sub("(?:[0-9]{1,3}\.){3}[0-9]{1,3} \(discuter \| bloquer\) \(\d{,3} \d{,3} octets\) ", "",post)for post in post_texts]
    post_texts = [ re.sub("(?:[0-9]{1,3}\.){3}[0-9]{1,3}", "IPADDRESS",post)for post in post_texts]    
    post_texts = [ re.sub("http:.+?( |$)", " URL ",post) for post in post_texts]
    post_texts = [ re.sub('htt(p|ps.+? )', " URL ", post) for post in post_texts]
    post_texts = [ re.sub('www.+? \)', " URL ", post) for post in post_texts]
    post_texts = [re.sub(r'(\d+)note - type - WikipediaModel_e_note ',r"\1e ",post) for post in post_texts]
    post_texts = [re.sub('note - type - WikipediaModel_e_note '," ",post) for post in post_texts]
    post_texts = [re.sub(r'note - type - WikipediaModel.+?note'," ", post) for post in post_texts]

    post_texts = [re.sub('note - type - WikipediaModel_,_note',' ',post) for post in post_texts]
    post_texts = [re.sub(r'note - type - WikipediaModel.*?_note'," ", post) for post in post_texts]
    post_texts = [re.sub('([dsljtmncç])(\")', "\\1 \\2", post) for post in post_texts]
    post_texts = [re.sub('\|align="left " \|','', post) for post in post_texts]
    post_texts = [re.sub("([\|)\.»])", "\\1 ", post) for post in post_texts]
    post_texts = [re.sub(r"^\| ", "", post) for post in post_texts]
  
    # create an ID based on the input filename
    # ID modified to consist of post-ID followed by $creat to name creator, then $dt for datetime of creation of post, $who for name/code ; $indent for indent level then $para to add para number
    ID = f'{post_id}$creat={post_creat}$dt={post_dt}$who{post_who}$indent={post_indentlevel}$para'
    # list comprehension to yield a tupe of unique identifier for each sentence and the sentence, then append to list
    ## proceeds excluding cases on two conditions : integer p is not in the droplist and if post text is not in exclusion list
    ## ID part of tuple is the concatenation of the ID string and the paragraph number within the post, which is added by p zfilled to 3 figs.
    tidy_data = [(f'{ID}-{str(p+1).zfill(3)}', post) for p, post in enumerate(post_texts) if (post not in exclusion_list and p not in droplist)]
    para_data.append(tidy_data)
    
    ## helper for debugging, writing para_data straight to file before tokenisation, sentencization
    # with open("/Users/Data/testout.conllu",'w', encoding="UTF-8") as k:
    #  for item in para_data:
    #   for meta, string in item:
    #    out = str(meta) + "\t" + str(string) + "\n"
    #    k.write(out)
   


def export_tokenized_wiki_to_conllu(sent_data_flat, tokenised_sents, folder, batch_no):
  '''
  export the sentencized and tokenized documents to conllu files
  Args :
    input : 
      sent_data_flat : list of sentences sent to the tokenizer, holding the conllu metastrings
      tokenised_sents : list of sentences returned by the tokeniser, holding the tokens
      folder : parent folder which contains source XML, as well as the directory in which files are to be written
      batch_no : int, number of current batch
    Returns :
     no return object, writes files to destination
  '''
  # set output file name: batch
  batch_no = str(batch_no).zfill(2)
  # set folder, Check if the folder already exists
  folder_path_output = folder.replace('F_talkPages/', 'F_talkPages/connlu/vtest/')
  if not os.path.exists(folder_path_output):
    os.makedirs(folder_path_output)
  # make name for output conllu based on folder path, remmoving final slash and adding batch_no + extension
  output_conll = re.sub(r"/$", f"/{folder_path_output[-2:-1]}_{batch_no}.conllu", folder_path_output)    
  # with open to open connection
  with open (output_conll, 'w', encoding="UTF-8") as m:
    for s in range(len(sent_data_flat)):
      # get list of tokens from tokenised_sentences
      token_list = tokenised_sents[s]
      # get metaline from sent_data_flat
      sentID_meta = "\n# sentID = " + sent_data_flat[s][0]
      # create metalines with raw and prefab tokenisation with LCs + join
      sent_text_meta_raw = "\n# text_raw = " + " ".join([token for token in token_list])
      sent_text_meta_prefab = "\n# text_prefab = " + "// ".join([token for token in token_list]) + "\n"
      sent_text_metas= sentID_meta + sent_text_meta_raw + sent_text_meta_prefab
      # write metas to file, then iterate over tokens in token list and write tokenID, token and blanks for 10 column conllu
      m.write(sent_text_metas)
      for t in range(len(token_list)):
        token_string = str(t+1) +"\t"+token_list[t] + "\t_\t_\t_\t_\t_\t_\t_\t_\n" 
        m.write(token_string)

  print(f"File exported : {output_conll}")  


def make_batches(source_files, batch_size):
  """Groups a list of files into chunks of a given size.

  Args:
    my_files (list): A list of file names or paths.
    group_size (int, optional): The number of files per group. Defaults to 10.

  Returns:
    list: A list of lists, where each sublist is a group of files.
  """

  batches = []
  for i in range(0, len(source_files), batch_size):
    batches.append(source_files[i:i + batch_size])

  return batches



def run_wiki_pipe(folder, batch_size):
  '''
  run the whole wiki.xml to tokenised pipeline for a folder
  Args : 
   input : 
    folder : folder containing XML files from wikidump to be processed
    batch_size : int, determining the number of source XML files to consider as a batch and consolidate into a single conllu file
  Notes : batch size of 5000 yields 104.conllu files of 50-150MB, and parsed files of 150-250 MB. While these can be processed by HOPS and Stanza, errors become problematic wiht HOPS and its subtokenisation limit of 510 subtokens.
  '''
  
  # 1.2 get all FR source language files as xml, divide into batches
  # this currently gets tei.xml specifically, not -g
  source_files = glob.glob(folder + '*tei.xml')

  ### create batches, and then loop over batches for the following steps until conllu is written :
  ### this avoids enormous files
  batches = make_batches(source_files, batch_size)
 
  for batch_no, batch in tqdm(enumerate(batches)):
    source_files = batch
    # 1.3 create list to store results, and pass list of files, and loop with the function to compile the para block data
    para_data=[]
    for m in tqdm(range(len(source_files))):
      file_path = source_files[m]
      get_post_text(file_path, para_data)

    # 1.4 flatten para_data to denest files with multiple paragaphs and add IDs
    para_data_flat = [(a, b) for sublist in para_data for a, b in sublist]

    
    # 1.4.1 run spacy sentencizer on paragraphs to yield sentences, and make tuple with these sents and their sentID, adding a+1 as a suffix to get number of sent in each para
    sent_data = []
    for s in range(len(para_data_flat)):
      data_out = [( str(para_data_flat[s][0]) +"$sent="+ str(a+1) , sentence.text_with_ws) for a, sentence in enumerate(nlp_s(para_data_flat[s][1]).sents)]
      sent_data.append(data_out)

    # 1.4.2 flatten this list of sentenceIDs and sentences 
    sent_data_flat = [(a, b) for sublist in sent_data for a, b in sublist]

    # 1.5 make a list of just the sentences
    flat_sentences = [sent_data_flat[i][1] for i in range(len(sent_data_flat))]
    ##### new lines added here to tidy
    flat_sentences_step = [ re.sub(r"( )+", r" ", flat_sentence) for flat_sentence in flat_sentences]
    flat_sentences_step = [ re.sub('"\'htt(p|ps).+? (.+?"\')', "URL ", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub('^ | $', "", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub("(?:[0-9]{1,3}\.){3}[0-9]{1,3} \(discuter \| bloquer\) \(\d{,3} \d{,3} octets\) ", "",flat_sentence)for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub("(?:[0-9]{1,3}\.){3}[0-9]{1,3}", "IPADDRESS",flat_sentence)for flat_sentence in flat_sentences_step]    
    flat_sentences_step = [ re.sub(r"( )+", r" ", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub("\xa0", " ",flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub("http:.+?( |$)", " URL ",flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub("Message déposé automatiquement par un robot le \(C(E|ES)T\).", "",flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [re.sub(r'(\d+)note - type - WikipediaModel_e_note ',r"\1e ",flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [re.sub('note - type - WikipediaModel_e_note '," ",flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [re.sub('note - type - WikipediaModel.+?note'," ", flat_sentence) for flat_sentence in flat_sentences_step]

    flat_sentences_step = [ re.sub('htt(p|ps.+? )', " URL ", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub('www.+? \)', " URL ", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub("\n|\r", " ", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences_step = [ re.sub(r"( )+", r" ", flat_sentence) for flat_sentence in flat_sentences_step]
    flat_sentences = flat_sentences_step


    # 1.5.3 tokenise each sentence with the French tokenizer from spacy
    tokenised_sents = [[token.__str__() for token in tokenizer_s(flat_sentence)] for flat_sentence in flat_sentences]

    # export here by passing in the list of sentence annotations, the list of tokenised sentences as well as the folder being processed and the batch number
    export_tokenized_wiki_to_conllu(sent_data_flat, tokenised_sents, folder, batch_no)

# option to run all for single letter, specifying batch size : processes ≈ 9k source files/min
#for g in ['S','R','Q','P','O','M','N','L','J','I','H','G','F','E','D','C','B','A']:
for g in ['Y','X','W','V','U','T','S','R','Q','P','O','M','N','L','K','J','I','H','G','F','E','D','C','B','A','0','9','8','7','6','5','4','3','2','1','Char']:
#for g in ['Z']:
  folder = f'/Users/Data/F_talkPages/{g}/'
  run_wiki_pipe(folder, 50000)
   

