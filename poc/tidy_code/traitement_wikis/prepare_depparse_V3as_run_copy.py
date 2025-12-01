# 6022 to 6030 for wikis
## this uses the same funcitons as the romans script,  rather than make lots of specifications to activate/deactivate dd processing and xml tags, 
input_conll_file = glob.glob(live_path + "*6022v2tv8.conllu")[0]
output_conll_file  = input_conll_file.replace('6022','6030')

# prepare for depparse V3 : takes 6022 as input :: for romans

from tqdm import tqdm
from stanza.utils.conll import CoNLL
import itertools, glob, re
from lxml import etree


# define list of different hyphens/tirets/traits d'union
hyphen_list = ("–","-", "—","−")
# define list of pronouns
pronlist = ("le","les","la", "moi","toi","soi", "lui","leur","en","y", "je", "tu", "il", "elle", "on", "ça", "ca", "nous", "vous", "ils", "elles","en","ce","ci","là", "j'", "t'", "c'")
# generate permutations of 't' with different hyphens
epen_t_forms =[str(item[0]) + str(item[1]) for item in list(itertools.product(hyphen_list, ['t']))]


with open("/Users/Data/ANR_PREFAB/Code/code_for_gitlab/traitement_wikis/prob_solver_dict.json", 'w', encoding='UTF-8') as j:
          json.dump(prob_solver_dict, j)

# wiki also need sspecial processor to deal with entre-t-il rendered as 1 token in 6022: split to v, pron
# -t -il not sent to 1x token
def remove_empty_tokens(current_sent):
  '''
  find and remove empty tokens from sentence
  Input : current_sent : a sentence from a CoNLL doc
  returns : sent_tidy : a string consisting of all the tidied line-level strings of a sentence
  '''
  # LC to get tokens if the token's text is a space
  targets = list([t for t, token in enumerate(current_sent.tokens) if token.text == " "])
  ## if there are targets
  if targets :
    # get sentence comments
    sent_meta = current_sent.comments
    # build list of tokens using list comprehension and exclusion based on integer position of empty tokens
    sent_mod = [current_sent.tokens[t].to_conll_text() for t, token  in enumerate(range(len(current_sent.tokens))) if t not in targets]
    
    # enumerate over lines, splitting the conll line at tab, setting the correct index values and sending back to string, and store strings in list after meta_strings
    for m, modline in enumerate(sent_mod):
      elements = modline.split("\t")
      elements[0] = str(m+1)
      elements[6] = str(m+1)
      print_line = "\t".join([item for item in elements])
      sent_meta.append(print_line)
    # after all lines have been processed, join all the lines of the sentence into a tidy_sentence
    sent_tidy = "\n" + "\n".join([item for item in sent_meta]) + "\n"
  
  # if there are no empty tokens
  else:
    # make a list of comments followed by all token lines sent to strings and combined 
    this_list = current_sent.comments +[ current_sent.tokens[t].to_conll_text() for t, token  in enumerate(range(len(current_sent.tokens))) ]
    # make a tidy sentence by joining all the items in the list and adding necessary linebreaks before and after
    sent_tidy = "\n" + "\n".join( [str(item) for item in this_list]  ) + "\n"
  
  return sent_tidy  

def load_and_tidy_conll(source_filepath):
  '''
  load a conllu file of written corpora, tidy the sentences and then parse the tidied sentences as a doc
  args:
    source_filepath : absolute path to a conll file.
  returns : source_doc : a CoNLL doc object of tidied sentences from the source file
  '''
  tidy_doc = []
  # read the file as a conll doc
  conll_data_in = CoNLL.conll2doc(source_filepath)
  # iterate over sentences removing empty tokens and tidying
  for current_sent in tqdm(conll_data_in.sentences):
    sent_tidy = remove_empty_tokens(current_sent)
    tidy_doc.append(sent_tidy)
  # consolidate all strings into a single object to pass in as conll_string
  conll_string_to_parse = "".join([item for item in tidy_doc])  
  # parse the string of all the tidied sentences as a conll doc
  source_doc = CoNLL.conll2doc(input_file=None, input_str = conll_string_to_parse)

  return source_doc

def make_token_conll_list(sent, t):
  '''
  split a string of conll annoations into a list of 10 items and return list
  '''
  tok_list= sent.tokens[t].to_conll_text().split("\t")
  return  tok_list

def make_hyphen_pron_tokstrfrom_4(sent, t):
  '''
  operating over four tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  '''
  elements_A = sent.tokens[t+0].to_conll_text().split("\t")
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  elements_D = sent.tokens[t+3].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  new_element = [elements_A[0], str(elements_A[1] + elements_B[1] + elements_C[1]+ elements_D[1])]+ elements_D[2:]
  hyphen_pron_token_list = [element for element in new_element]
  return hyphen_pron_token_list

def make_hyphen_pron_tokenstr_from3(sent, t):
  '''
  operating over three tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  '''
  elements_A = sent.tokens[t+0].to_conll_text().split("\t")
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  new_element = [elements_A[0], str(elements_A[1] + elements_B[1] + elements_C[1])]+ elements_C[2:]
  hyphen_pron_token_list = [element for element in new_element]
  return hyphen_pron_token_list

def make_pardes_tokenstr_from3(sent, t):
  '''
  operating over three tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  special-case processor to correct par-dessus tokenised as three tokens, taking all the token text from each token and concatenating them to form the text of the new token 
  inputs : sent: a sentence of a CoNLL doc 
  t : an integer defining a token index within the sentence
  returns:
    new_element_list : a list of 10 elements that form a conll line
  '''
  elements_A = sent.tokens[t+0].to_conll_text().split("\t")
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  new_form =  str(elements_A[1] + elements_B[1] + elements_C[1])
  new_element = [elements_A[0], new_form, new_form.lower()]+ elements_C[3:]
  new_element_list = [element for element in new_element]
  return new_element_list

def run_twogram_hyphen_processing(input_dict):
  '''
  run the processing pipeline for two-gram hyphen processing
  args :
    input : input_dict : a dictionary created by the run_threegram hyphen processor. Keys in this dictionary are sentence IDs, values are a list, the 0th element holding all the metadata strings for the sentence, and the 1th element holds the list of strings that constitute the conll lines
  returns : output_dict : a dictionary of keys == sentenceIDs and values, where values are lists, the 0th item == sentence metadata and the 1th item == strings for each conll line to print
            input_dict_size : length of the input dictionary
            output_dict_size : length of the output dictionary. Included as these sizes should be identical to each other as well as the number of sentences parsed on input
  '''
  output_dict = {}
  input_dict_size = len(input_dict)
  # iterate over key,value pairs for the input dict
  for key,value in tqdm(zip(input_dict.keys(), input_dict.values())):
      # get metadata from the 0th item in the list that is each value
      metas = value[0]
      # explicitate that the new key is the same as the existing key
      new_key = key
      # metas_tidy = metas.replace('_zzqz_','\n')
      # _ = w.write(spacer)
      # _ = w.write(metas_tidy)
      # _ = w.write(spacer)
      # get the sentence lines from the 1th item in the value
      sent_lines = value[1]
      # make tuples of token, POStag for each token in the sentence, based on index position of these elements in the conll lines
      data = [(el[1], el[3]) for el in sent_lines]
      # for el in sent_lines:

      # initialise the counter and the list of strings that constitute the ouput sentence
      t = 0
      current_sent = []
      # verify that there are tokens to process in data
      if isinstance(len(data), int) and len(data) > 0:
        # if there is 1 token in the sentence
        if len(data) ==1:
          # if we are dealing with the 0th token in the sentence
          if t ==0 :
            # make the token string for the line based on t, append this string to the list
            tok_list = make_tokstr_from_dict(sent_lines, t)
            current_sent.append(tok_list)
            # make a tuple of metas and the list of strings in the sentence
            result = metas, current_sent
            # use the new key to store the tuple in the output dictionary
            output_dict[new_key] =   result
      
      # set jump to false
      jump =  False
      if len(data) > 1:
          # while t is less than 2 from the end of the sentence
          while t < len(data)-2:
              # print(t)
              # if the current token has the POS of either aux or verb and the token at t+1 is in the list of hyphens and the token at t+2 has the POS pronoun
              if data[t][1] in ("AUX",'VERB') and data[t+1][0] in hyphen_list and data[t+2][1] == "PRON":
                # aciton if verb or ayx
                # make a token string for the token that is the verb/aux and add this to the sentence
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                # _ = w.write(tok_str)
                # make a token string for the hyphen and the following token, all based on the value of t 
                hyphen_pron_tokenstr = make_hyphen_pron_tokenstr_fromdict(sent_lines, t)
                current_sent.append(hyphen_pron_tokenstr)
                # _ = w.write(hyphen_pron_tokenstr)
                # having examined and processed tokens at t=verb/AUX, t+1 as punct and t+2 = pron, increase the counter to the value of the next unprocessed token
                t+=3
                # set jump to true to note that index is being increased by 3
                jump = True

              # if the POS of token t is neither verb nor AUX,  do this:
              else:
                # make token string for the current token and append to list        
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                # _ = w.write(tok_str)
                # increase the counter by 1
                t+=1 
          # if the current value of t is 2 less than the length of the sentence, use this loop to avoid setting the index to a value beyond the end of the sentence. Since there are only 2 tokens left, there aren't any 3grams to find    
          if t  ==len(data) -2:
                # make the token string for token t, append this to the list
                tok_str = make_tokstr_from_dict(sent_lines, t)
                # _ = w.write(tok_str)
                # _ = w.write(spacer)
                current_sent.append(tok_str)
                # increase the counter by 1
                t+=1
                # make token string for the final token in the sentence and append it to the list
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                
          # if the jump value is true and we have jumped to the final word in the sentence
          if jump and ((t == len(data)-1)  ):
                # make the token string and append it to the list, and set jump back to False
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                jump = False            
          # whichever of the if statements ran, get the metas and the processed sent and make a tuple      
          result = metas, current_sent
          # add this tuple to the dictionary as the value for the new_key  
          output_dict[new_key] =   result
  
  output_dict_size = len(output_dict)
  return output_dict, input_dict_size, output_dict_size

def make_tokstr_from_dict(sent_lines, t):
  '''
  return a list of 10 conll columns as a list for token t in sentence sentlines
  called by run_twogram_hyphen_processing
  '''
  target = sent_lines[t]
  tok_str = [item for item in target]    
  return tok_str

def make_hyphen_pron_tokenstr_fromdict(sent_lines, t):
  '''
  extract sent_lines from a dictionary, getting elements from tokens t+1 and t+2 to make a consolidated tidy line
  called by run_twogram_hyphen_processing
  '''
  hyphen_elements = sent_lines[t+1]
  pron_elements = sent_lines[t+2]
  new_element = [hyphen_elements[0], str(hyphen_elements[1] + pron_elements[1])]+ pron_elements[2:]
  hyphen_pron_tokenstr = [element for element in new_element]
  return hyphen_pron_tokenstr

def process_threegram_hyphens(data, sent):
  '''
  helper to run while loop for tokens of sentence, returning a list of conll lines as strings
  '''
  
  t=0
  sent_proc = []
  if isinstance(len(data), int) and len(data) > 0:
    if len(data) ==1:
      if t ==0 :
        tok_list = make_token_conll_list(sent, t)
        sent_proc.append(tok_list)
        #print(t, "SuccessA")
    jump=False    
  if len(data) > 1:
    while t < len(data)-2:
      if t < len(data)-3 and data[t][1]=="PUNCT" and data[t][0] in hyphen_list and data[t+2][1]=="PUNCT" and data[t+2][0] in hyphen_list and data[t+1][0] in ("t","T") and data[t+3][0] in pronlist:
        # try loop if have -t-il en 4 toks and if index0 and 2 are both punct and both in hyphen list, and index1 is a T and index4 is a pron, do this:
        hyphen_pron_token_list = make_hyphen_pron_tokstrfrom_4(sent,t)
        sent_proc.append(hyphen_pron_token_list)
        t+=4
        if (t >=len(data)):
          return sent_proc
        
        jump=True
      if t < len(data)-2 and data[t][1] == "PRON" and data[t+1][1] == "PUNCT" and data[t+2][1] == "PRON" and data[t][0] in epen_t_forms and data[t+2][0] in pronlist:
        hyphen_pron_token_list = make_hyphen_pron_tokenstr_from3(sent, t)
        sent_proc.append(hyphen_pron_token_list)
        #print(t, "SuccessB")
        t+=3 # t = verb, t+1
        jump=True
      elif t < len(data)-2 and data[t][0] in ('Par','par') and data[t+1][1] == "PUNCT" and data[t+2][1] in("ADP","ADV")  and data[t+1][0] in hyphen_list and data[t+2][0] in ('dessus','dessous', 'derrière','devant','devers'):
        new_element_list= make_pardes_tokenstr_from3(sent, t)
        sent_proc.append(new_element_list)
        #print(t, "SuccessC")
        # _ = w.write(hyphen_pron_tokenstr)
        t+=3 # t = verb, t+1 = punct, t+2 = pron
        jump=True
      else:
        tok_list = make_token_conll_list(sent, t)
        sent_proc.append(tok_list)
        #print(t, "SuccessD")
        # _ = w.write(tok_list)
        t+=1 
    if t  ==len(data) -2:
        tok_list = make_token_conll_list(sent, t)
        sent_proc.append(tok_list)
        #print(t, "SuccessE")
        # _ = w.write(tok_list)
        t+=1
        tok_list = make_token_conll_list(sent, t)
        sent_proc.append(tok_list)
        #print(t, "SuccessF")
        # _ = w.write(tok_list)
        # _ = w.write(spacer)
    if jump and ((t == len(data)-1)  ):
        tok_list = make_token_conll_list(sent, t)
        sent_proc.append(tok_list)
        jump = False
        
  return sent_proc

def run_threegram_hyphen_processing(source_doc):
  '''
  read conll from file, iterate over sentences processing threegram_hyphens, outputting to dictionary
  input_dict : dict of key = metadata lines from sentID and orig tokenisation
  '''
  
input_dict, errorlist, spacer = {},[], "\n"
sent_count_in = len(source_doc.sentences)
for k, sent in tqdm(enumerate(source_doc.sentences)):
    data = [(token.text, token.to_conll_text().split('\t')[3])  for token in sent.tokens]
    metas = sent.comments
    meta_key = metas[0]
    sent_proc = process_threegram_hyphens(data, sent)
    if meta_key in input_dict.keys():
      error_log = [k, meta_key]
      errorlist.append(error_log)
    result = metas, sent_proc
    input_dict[meta_key] = result

  if len(errorlist)>0:
    print(errorlist)

  return   input_dict, sent_count_in

source_doc = testdoc

# dict_file_wikid = "/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3==v7/D/D_00/D_00-dict.json"
# with open(dict_file_wikid, 'r', encoding="UTF-8") as d:
#       output_dict = json.load(d)

# listofkeys = list(output_dict.keys())
def run_conjoined_processor(output_dict):
  global_count=0
  for key, value in tqdm(zip(output_dict.keys(), output_dict.values())):
    # value = output_dict['#sent_id = i.1896092_28_29-001-1']
    lines_in = value[1]
    lines_out = []
    counter = 0
    for line in lines_in:
        if line[1] not in prob_solver_dict.keys():
          lines_out.append(line)
        if line[1] in prob_solver_dict.keys():
          counter +=1
        
          LEM, POS=  prob_solver_dict[line[1]]
          if line[1].endswith('il'):
            pron_len = 5
          else:
            pron_len = 7
          line_mod = [chunk for chunk in line]
          line_ext = [chunk for chunk in line]
          new_token = line[1][0:(len(line[1])-pron_len) ]
          line_mod[1] = new_token
          line_mod[2] = LEM
          line_mod[3] = POS
          line_mod[5] = "_"
          lines_out.append(line_mod)
          line_ext[1] = line[1][(len(line[1])-pron_len):]
          if pron_len ==5:
            line_ext[2] ="il"
          else:
            line_ext[2] ="elle"
          lines_out.append(line_ext)
    # if counter has started, a change has been made, so add it to the dict.
    if counter !=0:
      metas = output_dict[key][0]
      new_data = [metas, lines_out]
      output_dict[key] = new_data
      global_count = global_count+counter
  print(f'Global count = {global_count}')
  output_dict_updated = output_dict
  
  return output_dict_updated

line = lines_in[8]#[1] = "à-t-il"

#output_conllfile_name =output_conll_file
def write_conll_from_outputdict(output_dict_updated, output_conllfile_name):
  '''
  take output_dict, extract strings, make final meta lines, sequentialise index values and write to file
  '''
  with open(output_conllfile_name, 'w', encoding='UTF-8') as w:
    for key, value in tqdm(zip(output_dict_updated.keys(), output_dict_updated.values())):
        metas = value[0]
        sent_lines = value[1]
        #metas_tidy = metas.replace('_zzqz_','\n')
        metas_tidy = f"\n{metas[0]}\n" + f'{metas[1]}' + '\n'+ '#text_prefab = '  + "// ".join([line[1] for line in sent_lines]) + "\n"
        # need to regenerate prefab_text from string
        _ = w.write(metas_tidy)
  
        for x in range(len(sent_lines)):
          raw_line = sent_lines[x]
          raw_line[0] = str(x+1)
          raw_line[6] = str(x+1)
          if raw_line[3] != "PUNCT" : 
            raw_line[2] =  re.sub("'$","e", raw_line[2]) ## if lemma, ends wiht apostrophe, add E in its place
          print_line = "\t".join([item for item in raw_line])+"\n"
          _ = w.write(print_line)


def prepare_sentence(conll_sentence):
  '''
  called to make strings for sent splitter
  '''
  conll_sentence = [comment for comment in conll_sentence.comments] + [token.to_conll_text() for token in conll_sentence.tokens]
  return conll_sentence


def split_conll_sentence_with_suffix(this_sentence):
    """
    Splits a CoNLL formatted sentence into two at the occurrence of '…' character.
    The sent_id in comment lines is suffixed with Greek letters (α, β, γ, etc.).
    
    Args:
        conll_sentence (list of str): The input CoNLL sentence, where each element is a line.
    
    Returns:
        list of list of str: A list containing one or more CoNLL sentences depending on whether the special character was found.
    """
    # prepare the sentence
    conll_sentence = prepare_sentence(this_sentence)

    # initialise variables and list of letters to ensure unique sufixes
    split_sentences = []
    current_sentence = []
    found_split_char = False
    suffix_index = 0
    greek_letters_raw = ['α', 'β', 'γ', 'δ', 'ε', 'ζ', 'η', 'θ', 'ι', 'κ', 'λ', 'μ', 'ν', 'ξ', 'ο', 'π', 'ρ', 'σ', 'τ', 'υ', 'φ', 'χ', 'ψ', 'ω']
    # make list of all combinations of letters
    greek_letters = []
    for i in greek_letters_raw:
      for j in greek_letters_raw:
        # coerce values to strings and concatenate, then append to list
        pair = str(i)+str(j)
        greek_letters.append(pair)
    ## define as a tuple the list of punctuation items that will be recognised as the end of sentences beyond those already present    

    EOS_list = ("...",'…',';',':') # list of forms to recognise as end of sentences

    for line in conll_sentence:
        # Handle comment lines
        if line.startswith("#"):
            if "sent_id" in line:
                # if else with identical arms : split sentIDs to add greek suffixes to comment lines with send_ID, in all cases
                if found_split_char:
                    base_id = line.split("=")[1].strip()
                    new_line = f"#sent_id = {base_id}{greek_letters[suffix_index]}"
                    suffix_index += 1
                else:
                    base_id = line.split("=")[1].strip()
                    new_line = f"#sent_id = {base_id}{greek_letters[suffix_index]}"
                    suffix_index += 1
                ## append the metaline wiht the newly modified sentID to the list of strings that constitute the current sentence 
                current_sentence.append(new_line)
            # if this is a comment line but it does not contain sentID, append line unmodified
            else:
                current_sentence.append(line)
        # handle token lines ie non-comments
        else:
            # split the line of conll annotations into the 10 values 
            tokens = line.split("\t")
            # if there are multiple items in tokens, we have a valid line, not an empty one, and if the token text in in the EOS character list
            if len(tokens) > 1 and tokens[1] in EOS_list:
                # determine if we have an ellipse present as 3 periods rather than 1 char by testing length of the token text. if so, set the step to 1. remnant from dev.
                if len(tokens[1]) ==3:
                  special_step = 1
                else :
                  special_step =1
                # set found value to true, append the current line ie the current token to the current sent, and this sent to all_sentences
                found_split_char = True
                current_sentence.append(line)
                split_sentences.append(current_sentence)
                # Start a new sentence with the same comments, except the sID line
                current_sentence = [
                    l if not l.startswith("#sent_id") else f"#sent_id = {base_id}{greek_letters[suffix_index]}"
                    for l in conll_sentence if l.startswith("#")
                ]
                # increment the suffix_index to proceed along this list of suffixes
                suffix_index += special_step
            else:
                # if there are no tokens in the line or the line does not contain an EOS char, append the line unmodified
                current_sentence.append(line)
    ## if there is a current_sentence that was produced, add it to the list split_sentences
    if current_sentence:
        split_sentences.append(current_sentence)
    # if no conditions are met thus far and found value is still false, return the input sentence unmodified as a list of strings
    if not found_split_char:
        return [conll_sentence]
    
    return split_sentences

def get_tok_count(sent):
  '''
  count the number of tokens in a sentence based on the number of lines in a list, excluding comment lines
  args :
    input :
      sent : a sentence in a conll document
    return : count : an integer count of the tokens in the sentence
  '''
  count =0
  for line in sent:
    if line.startswith("#") is False:
      count +=1
  return int(count)

def run_line_reindexer(sent):
  '''
  renumber tokens both at the index for each token and the value in the Head column, after sentence splitting, to ensure all values are valid
  args : input:
      sent : a sentence in a CoNLL document
  Returns :
    tidy_sent : the input sentence with line index values and Head reindexed from 1
  '''
  tidy_sent = []
  index = 0
  for line in sent:
    if line.startswith("#"):
      tidy_sent.append(line)
    else:
      index +=1
      line_exp = line.split("\t")
      line_exp[0], line_exp[6] = index, index
      line = "\t".join(str(item) for item in line_exp)
      tidy_sent.append(line)

  return tidy_sent

def run_sentence_splitt_pipe(updated_conll_doc, mode):
    '''
    run the pipeline to split conll sentences
    inputs : 
      input_conll_doc : a parsed conll document
      mode : one of two strings. run will run the splitter. skip will not run th esplitter. in either case, the sentence preparer is used to make strings of the conll comments and lines
    returns :
      all_splits : a list of sentences after splitting on specific chars
      output_sent_count : count of sentences RECEIVED as input by this function
    '''
    # get the number of sentences taken as input
    output_sent_count = len(updated_conll_doc.sentences)
    all_splits = []

    if mode == "run":
      for this_sentence in tqdm(updated_conll_doc.sentences): 
        # make the list split_sentences for each input sentence
        split_sentences = split_conll_sentence_with_suffix(this_sentence)
        # append the first item in the list to all_splits. there is always 1 sentence, either the input sent shortened or the input sentence unmodified
        all_splits.append(split_sentences[0])
        # if there are 2 or more sentences in split_sentences, then starting at index 1 (first sentence has already been processed)
        if len(split_sentences) >1:
          for s in range(1, len(split_sentences)):
            # get the sentence
            sent = split_sentences[s]
            # get number of tokens to avoid adding in sents where ellipse was already end of sent
            tok_count = get_tok_count(sent)
            if tok_count >0:
              # if the sentence has tokens, then reset the index (column 0 and the Head (col6)) values then append this sentence to all_splits
              tidy_sent = run_line_reindexer(sent)
              all_splits.append(tidy_sent)
    if mode == "skip":
      for this_sentence in tqdm(updated_conll_doc.sentences): 
        conll_sentence = prepare_sentence(this_sentence)
        all_splits.append(conll_sentence)
      
    return all_splits, output_sent_count


def write_conllu_file_for_InitialAproces(conllu_file_for_InitialAproces, all_splits):
    '''
    write a file iterating over the list all_splits
    args :
        conllu_file_for_InitialAproces : absolute path to write file
        all_splits : list of sentences returned by run_sentence_splitt_pipe
        in the list, each item consists of a list of the lines of the 1 or more sentences split off from an input sentence
    '''
    with open(conllu_file_for_InitialAproces, 'w', encoding='UTF-8') as p:
      # iterate over the items in the list, each item corresponding to a sent in the input doc
      for block in all_splits:
        # for sentence == chunk present in current block make a string and print it
          print_line = "\n\n" + "\n".join([chunk for chunk in block]) + "\n\n"
          _ = p.write(print_line)


## run section
mynum = 2
#set mode to skip if don't want to split sentences == for oral, or aligned_not cha
# if mode == "run":
mode="run"
for mynum in ['T']:
  for myseq in ['00','01']:
    input_conll_file = f'/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/{mynum}/{mynum}_{myseq}/{mynum}_{myseq}-6022v3tv8.conllu'
    if os.path.exists(input_conll_file) ==False:
      input_conll_file = f'/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/{mynum}/{mynum}_{myseq}/{mynum}_{myseq}-6022v2tv8.conllu'  
for input_conll_file in files_group2:
    output_conll_file = input_conll_file.replace('6022','6130')
    source_doc = load_and_tidy_conll(input_conll_file) 
    input_dict, sent_count_in = run_threegram_hyphen_processing(source_doc)
    output_dict, input_dict_size, output_dict_size=   run_twogram_hyphen_processing(input_dict)
    output_dict_updated = run_conjoined_processor(output_dict)
    write_conll_from_outputdict(output_dict_updated, output_conll_file)
    print(f'{mynum}_{myseq} printed')


output_dict_updated['#sent_id = i.10009349_1_1-001-1']

# don't need to run sentence splitter, as already run, so we're already in 6310 state





files_group2 = glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/[BFGJDEMS]/*/*6022*.conllu') + glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V4__v8/Char/*/*6022*.conllu')

###### the following lines are for creating, or updating the prob_solver_dict, which contains the random forms where verb, -t- and subject haven't been split ; these are keys and lemma, POS tags are values for the dict, whcih is used above


#### need to find all cases of conjoined, so can lemmatise
#input_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3==v7/D/D_00/D_00-6022v3tv8.conllu'
#parsed_conll = CoNLL.conll2doc(input_file)
#prob_list = []
#for sent in tqdm(source_doc.sentences):
#  for token in sent.tokens:
#    if (token._text[-5:] in conjoined_ilforms) or (token._text[-7:] in conjoined_elleforms):
#      prob_list.append(token._text)
#    
#prob_cases = list(set(prob_list))
#prob_solver_dict = {}
#prob_solver_dict["entre-t-il"] = ["entrer", "VERB"]
#
#
#prob_solver_dict = {}
#prob_solver_dict["entre-t-il"] = ["entrer", "VERB"]
#prob_solver_dict['à-t-il'] = ["avoir", "AUX"]
#prob_solver_dict['tire-t-elle'] = ["tirer","VERB"]
#prob_solver_dict[ 'porte-t-elle'] = ["porter", "VERB"]
#prob_solver_dict['à-t-elle'] = ["avoir", "AUX"]
#prob_solver_dict['à-t-il'] = ["avoir", "AUX"]
#prob_solver_dict['À-t-il'] = ["avoir", "AUX"]
#prob_solver_dict['cache-t-il'] = ["cacher", "VERB"]
#prob_solver_dict['compte-t-elle']= ["compter", "VERB"]
#prob_solver_dict['Compte-t-elle'] = ["compter", "VERB"]
#prob_solver_dict['compte-t-il']= ["compter", "VERB"]
#prob_solver_dict['Compte-t-il']= ["compter", "VERB"]
#prob_solver_dict['coupe-t-elle']= ["couper", "VERB"]
#prob_solver_dict['couvre-t-elle']= ["couvrir", "VERB"]
#prob_solver_dict['couvre-t-il']= ["couvrir", "VERB"]
#prob_solver_dict['Couvre-t-il']= ["couvrir", "VERB"]
#prob_solver_dict['entre-t-elle']= ["entrer", "VERB"]
#prob_solver_dict['entre-t-il']= ["entrer", "VERB"]
#prob_solver_dict['garde-t-elle']= ["garder", "VERB"]
#prob_solver_dict['garde-t-il']= ["garder", "VERB"]
#prob_solver_dict['ouvre-t-il']= ["ouvrir", "VERB"]
#prob_solver_dict['porte-t-elle']= ["porter", "VERB"]
#prob_solver_dict['Porte-t-elle']= ["porter", "VERB"]
#prob_solver_dict['porte-t-il']= ["porter", "VERB"]
#prob_solver_dict['protège-t-elle']= ["protéger", "VERB"]
#prob_solver_dict['protège-t-il']= ["protéger", "VERB"]
#prob_solver_dict['tire-t-elle']= ["tirer", "VERB"]
#prob_solver_dict['tire-t-il']= ["tirer", "VERB"]
#prob_solver_dict['tourne-t-elle']= ["tourner", "VERB"]
#prob_solver_dict['traîne-t-il']= ["traîner", "VERB"]
#prob_solver_dict['vide-t-il']= ["vider", "VERB"]

  

#hyphen_list = ("–","-", "—","−")
# define list of pronouns
# generate permutations of 't' with different hyphens
#conjoined_t_forms =[str(item[0]) + str(item[1]) + str(item[0]) for item in list(itertools.product(hyphen_list, ['t']))]
#conjoined_ilforms = [conjoined_t_forms[i] + "il" for i in range(len(conjoined_t_forms))]
#conjoined_elleforms = [conjoined_t_forms[i] + "elle" for i in range(len(conjoined_t_forms))]
# + [conjoined_t_forms[i] + "elle" for i in range(len(conjoined_t_forms))]
