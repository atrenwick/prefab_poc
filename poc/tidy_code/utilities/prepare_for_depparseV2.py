## this script is used to take text from either ROMAN or WIKIS and prepare it for depparsing : 
# this involves sending leading punctuation to its own token, except for -là and -ci ; this involves a partial retokrnisation, and reindexation of the sentence

import pickle
from stanza.utils.conll import CoNLL
from tqdm import tqdm
import string, os, re
## these three lines on the punctuation set are redundant as have pickled file with punct set
#punctuation_set = set(string.punctuation)
#punctuation_set.add('…')
#punctuation_set.add('...')
def am_punctsent(current_sent):
  """
  helper function that returns one of two values depending on whether the sentence contains entirely punctuation or not
  Args : input : current_sent : a sentence objet in which the text of the tokens will be analysed
  	Return : if false is in the challenge_list, "normal_sent" is returned ; if False not in this list, punct_sent is returned
  """
  challenge_list = ([current_sent.tokens[i].text  in punctuation_set for i in range(len(current_sent.tokens))]) 
  if False in challenge_list:
    punct_answer = "normal_sent"
  if False not in challenge_list:  
    punct_answer = "punct_sent"
  return punct_answer



# functions for stanza processing
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

def make_tidylines(current_sent):
  '''
  make tidy lines of conll text applying the retokrnisation rules to put leadin gpunctuation on its own line except for specific cases
  Args : input: 
      current_sent : a sentence from source.doc
      returns : appends a tidy conll string to a list, tidyout, which is returned
  
  '''
  ## initialise storage for lines and sentence
  ## get metalines and append to head of sentence
  linesout, tidylines = [], []
  metalines = ("\n" + "\n".join([meta for meta in current_sent.comments[:3]])) + "\n"
  tidylines.append(metalines)
  
  # if token char0 in punctset and not last token of sent and token has multiple chars and token not in special list
  for t, token in enumerate(current_sent.tokens):
    ## iterate over tokens in the sentence, splitting the conll text into a list
    line_elements = token.to_conll_text().split("\t")
    if line_elements[2] == "" and line_elements[3] == "PUNCT":
      # deal with  cases where token is empty and POS is punct by sending forme to lemma
      line_elements[2] = line_elements[1]
      # append line, and continue
      mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
      linesout.append(mod_line)
      continue
    if line_elements[1] == "" and line_elements[3] == "PUNCT":
      # deal with case whre form is empty but POS is punct : sent lemma to form
      line_elements[1] = line_elements[2]
      # append line, and continue
      mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
      linesout.append(mod_line)
      continue
    
    # if this token is not the penult:
    if t < len(current_sent.tokens)-1:
      # if the token is not in this tuple of items, then append line and continue
      if line_elements[1] in ("...","…","-ci","-là","-t", ".."):
        mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
        linesout.append(mod_line)
        continue
        # print(mod_line, "1")
      # if the first char of the token is not in the punctuation set, then append line and cpntinue
      if line_elements[1][0] not in punctuation_set:
        mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
        linesout.append(mod_line)
        continue
        # print(mod_line, "2")
      # if the first char of the token is in the punctuation set
      if line_elements[1][0] in punctuation_set:
        if len(line_elements[1])==1:
          # if there are only two chars in the token, , then append line and cpntinue
          mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
          linesout.append(mod_line)
          continue
        if len(line_elements[1])>1:
          ## if there are more than 1 chars in the token and the 2nd char is not in the punct set,
          if line_elements[1][1] not in punctuation_set:
            ## get the 0th char as this_char, remove it from the token, return the char in spacer_line, and append the remainder of the token and continue
            this_char = line_elements[1][0]
            line_elements[1] = line_elements[1][1:]
            spacer_line = [this_char ,this_char, 'PUNCT', '_','_','_','_','_','_']
            # print(spacer_line, "2")
            linesout.append(spacer_line)
            mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
            # print(mod_line, "4")
            linesout.append(mod_line)
            continue
    
          if line_elements[1][1] in punctuation_set:
            char0 = line_elements[1][0:2]
            line_elements[1] = line_elements[1][2:]
            spacer_line = [char0 ,char0, 'PUNCT', '_','_','_','_','_','_']
            linesout.append(spacer_line)
            mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
            # print(mod_line, "5")
            linesout.append(mod_line)
            continue
      #    mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
      #    print(mod_line, "6")
      #    #linesout.append(mod_line)
    # if t is the penult, make like and append    
    if t == len(current_sent.tokens)-1:
      mod_line = [line_elements[i] for i in range(1, len(line_elements)) ]
      # print(mod_line , "7")
      linesout.append(mod_line)
      
  # for each line in linesout, join the specified elements, append
  for l, line in enumerate(linesout):
    tidy_line = "\t".join([str(l+1), line[0], line[1], line[2], line[3], line[4], str(l), line[6], line[7], line[8]]) + "\n"
    # print(tidy_line)
    tidylines.append(tidy_line)
  
  return tidylines

#  elements[1][0] in puctuation_set
# making the punctuaiton set:
# # # punctuation_set_list = list(punctuation_set)
# # punctuation_set = set(string.punctuation)
# # punctuation_set.add("…")
# # punctuation_set.add(";")
# # punctuation_set.add()
# # punctuation_set.add("—")
# # punctuation_set.add("-")
# # punctuation_set = ['?', "'", '#', '|', '~', ';', '[', '\\', '…', '^', '}', '<', ')', '-', '>', '/', '`', '*', '%', '+', '&', '!', '@', '"', ']', '$', '_', ',', '{', '—', '.', '(', '=', ':', elements[1][0]]
# # punctuation_set = set(puctuation_set)
# # file_path = '/Users/Data/ANR_PREFAB/Code/punctuation_set.pkl'
# # with open(file_path, 'wb') as file:
# #   pickle.dump(punctuation_set, file)
# 

exceptionalCases = ['-là', '-ci', ".","-ils","-il", "-elles", "-elle"]

def process_leading_punct(current_sent, exceptionalCases):
    """
    process sentences detatching leading punct when necessary
    args : input :
    current_sent : sentence on which to operate
    exceptionalCases : list of special cases where - are not to be detatched, and .
    returns : results : a listof strings, each representing a conllu sentence
    """
    # initialise results, index, make metadata for sentennce from concat of all comments
    results = []
    metachunk = "\n"+ current_sent.comments[0] + "\n" + current_sent.comments[1] + "\n" + current_sent.comments[2] + "\n"
    results.append(metachunk)
    ndx=0 
    # determine whether the sentence is entirely pucntuation or not
    sent_type = am_punctsent(current_sent)
    ## if sent is punctuation, then split to allow for reindexing, append, increment
    if sent_type == "punct_sent":
      for token in current_sent.tokens:
          #elements = current_sent.tokens[15].to_conll_text().split('\t')
          elements = token.to_conll_text().split('\t')
          modified_string = '\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:])
          results.append(modified_string)
          ndx+=1
    ## if the sent is a norma sent; ie not just punct::
    if sent_type == "normal_sent":
      for token in current_sent.tokens:
          #elements = current_sent.tokens[15].to_conll_text().split('\t')
          ## iterating over tokens, split each token into a list
          elements = token.to_conll_text().split('\t')
          ####
          ##           summary of rules 
          ## if the forme is putain and POS is PROPN, ADJ or ADJ, set lemma = putain, POS = INTJ
          ## if form is hum and POS is NOUN or PROPN, set lemma to hum, POS to INTJ, 
          ## if form is d'accord in 1 token and POS is not ADV, set POS to ADV and lemma to lower
          ## if form is faut and verb in faillir, set POS to verb and lemma to falloir to denoise
          ## if lem is empty and POS is punct, set lem based on forme
          ####
          if elements[1].lower() == "putain" and elements[3] in ("PROPN","ADJ","ADV"):
            elements[2] = "putain"
            elements[3] = "INTJ"
          if elements[1].lower() == "hum" and elements[3] in ("NOUN","PROPN"):
            elements[2] = elements[2].lower()
            elements[3] = "INTJ"
          if elements[1].lower() == "d\'accord" and elements[3] != "ADV":
            elements[2] = elements[2].lower()
            elements[3] = "ADV"
          if elements[1].lower() == "faut" and elements[2] in ("faillir","PROPN"):
            elements[2] = "falloir"
            elements[3] = "VERB"
          if elements[3] == "PUNCT" and elements[2] == "":
            elements[2] = elements[1]
            
          # print(elements)
          # Check if elements[1] is in exceptionalCases and length is either 1 or greater than 1 :: return same data except for index
          if  len(elements[1]) >1  and elements[1] in exceptionalCases:
              results.append('\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:]))
              continue
          if len(current_sent.tokens) ==1  and elements[1] in exceptionalCases:
              results.append('\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:]))
              continue
  
          special_outputs = []
          #### if the form is in the list, return a spacer line with the punct and the token on modline :::: is this supposed to do this to epxception list items
          # Check if the first element starts with consecutive punctuation
          if elements[1] in ("-y", "-en", "—y", "—en", "-en", "-y"):
            this_char = elements[1][0]
            elements[1] = elements[1][1:]
            spacer_line = "\t".join([f'{this_char}',f'{this_char}', 'PUNCT', '_','_',str(ndx),'_','_',elements[-1]])
            results.append(spacer_line)
            ndx +=1
            mod_line = ('\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:]))
            results.append(mod_line)
            continue
  
          else : 
            if len(elements[1]) >1:
              if  elements[1][0] in punctuation_set:
                  punc_chars = []
                  i = 0
                  # elements[1][0][i]
                  #### if there is more than one char in the token and the first char is in the punct set
                  # Collect consecutive punctuation characters
                  while i < len(elements[1])-1 and elements[1][i].isalpha() is False:
                      punc_chars.append(elements[1][i])
                      i += 1
                      #### while current char is not penult and is not alpha, append to list, removing from token
                  final_column = elements[-1]
                  
                  # Create special output strings for each punctuation character
                  for punc_char in punc_chars:
                      special_output = f"{punc_char}\t{punc_char}\tPUNCT\t_\t_\t{str(ndx)}\t_\t_\t{final_column}"
                      special_outputs.append(special_output)
                      ndx+=1
              
                  # Remove the collected punctuation characters from the first element
                  elements[1] = elements[1][i:]
            
  # ndx=4        
          modified_string = '\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:])
          if len(special_outputs) >0:
            for output in special_outputs:
              results.append(output)
          results.append(modified_string)
          ndx+=1
    return results


def make_tokstr_from_dict(sent_lines, t):
  '''
  return a list of 10 conll columns as a list for token t in sentence sentlines
  '''
  target = sent_lines[t]
  tok_str = [item for item in target]    
  return tok_str

def combine_hyph_lines(current_elements, next_elements):
  '''
  take two conll lines and combine them by takind index from line 0, string from line 0, adding latter to string from line1 and remainder of annotaitons from string 2
  returns a list of the 10 elements that constitute a conllu line
  '''
  new_element = [current_elements[0], str(current_elements[1] + next_elements[1])]+ next_elements[2:]
  report = str(current_elements[1] + next_elements[1])
  return new_element, report

def make_token_conll_list(sent, t):
  '''
  split a string of conll annoations into a list of 10 items and return list
  '''
  tok_list= sent.tokens[t].to_conll_text().split("\t")
  return  tok_list
  

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

def make_hyphen_pron_tokenstr(sent, t):
  '''
  operating over two tokens, get the index and first char from tokenA and add tokenA before all chars of tokenB form, use all other annotations from token B
  '''
  hyphen_elements = sent.tokens[t+1].to_conll_text().split("\t")
  pron_elements = sent.tokens[t+2].to_conll_text().split("\t")
  new_element = [hyphen_elements[0], str(hyphen_elements[1] + pron_elements[1])]+ pron_elements[2:]
  hyphen_pron_tokenstr = [element for element in new_element]
  return hyphen_pron_tokenstr


def make_hyphen_pron_tokenstr_fromdict(sent_lines, t):
  '''
  extract sent_lines from a dictionary, getting elements from tokens t+1 and t+2 to make a consolidated tidy line
  '''
  hyphen_elements = sent_lines[t+1]
  pron_elements = sent_lines[t+2]
  new_element = [hyphen_elements[0], str(hyphen_elements[1] + pron_elements[1])]+ pron_elements[2:]
  hyphen_pron_tokenstr = [element for element in new_element]
  return hyphen_pron_tokenstr









def make_pardes_tokenstr_from3(sent, t):
  '''
  operating over three tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  '''
  elements_A = sent.tokens[t+0].to_conll_text().split("\t")
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  new_form =  str(elements_A[1] + elements_B[1] + elements_C[1])
  new_element = [elements_A[0], new_form, new_form.lower()]+ elements_C[3:]
  new_element_list = [element for element in new_element]
  return new_element_list
  

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
        
    if len(data) > 1:
      while t < len(data)-2:
        if data[t][1] == "PRON" and data[t+1][1] == "PUNCT" and data[t+2][1] == "PRON" and data[t][0] in epen_t_forms and data[t+2][0] in pronlist:
          hyphen_pron_token_list = make_hyphen_pron_tokenstr_from3(sent, t)
          sent_proc.append(hyphen_pron_token_list)
          #print(t, "SuccessB")
          t+=3 # t = verb, t+1
          jump=True
        elif data[t][0] in ('Par','par') and data[t+1][1] == "PUNCT" and data[t+2][1] in("ADP","ADV")  and data[t+1][0] in hyphen_list and data[t+2][0] in ('dessus','dessous', 'derrière','devant','devers'):
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
      if jump is True and ((t == len(data)-1)  ):
          tok_list = make_token_conll_list(sent, t)
          sent_proc.append(tok_list)
          jump = False
         
    return sent_proc

def process_punct_and_tidy(source_doc):
  outputstrings = []
  '''
  run leading punct processor and tidy output, returning list of strings to print 
  '''
  for current_sent in tqdm(source_doc.sentences):
    results = process_leading_punct(current_sent, exceptionalCases)
    q=0
    for result in results:
        result = re.sub('_unknownpostag_','_', result)
      
        if result.startswith("\n#") is False:
          this_string = (str(q+1) + "\t" + str(result) +"\n")
          q+=1
          outputstrings.append(this_string)
        else:
          outputstrings.append(result)
  return outputstrings

def print_outputstrings(outputstrings, output_file):  
  with open(output_file,'w', encoding="UTF-8") as p:
      for sent in tqdm(outputstrings):
        for line in sent:
          line = re.sub("_unknownpostag_", "_", line)
          line = re.sub(r'\t\t(.+?)\t(PUNCT|SYM)', r'\t\1\t\1\t\2', line)
          _ = p.write(line)
          # print(line)

keylist = list(input_dict.keys())[598]
sent = input_dict[keylist]
holding_item = ["\t".join([item for item in line]) for line in sent]
holding_str = "\n".join([item for item in holding_item])
source_doc_manual = CoNLL.conll2doc(input_file = None, input_str=holding_str)
source_doc = source_doc_manual
def run_threegram_hyphen_processing(source_filepath):
  '''
  read conll from file, iterate over sentences processing threegram_hyphens, outputting to dictionary
  input_dict : dict of key = metadata lines from sentID and orig tokenisation
  '''
  
  source_doc = CoNLL.conll2doc(source_filepath)
  input_dict, errorlist, spacer = {},[], "\n"
sent = source_doc.sentences[1965]
  for k, sent in tqdm(enumerate(source_doc.sentences)):
      data = [(token.text, token.to_conll_text().split('\t')[3])  for token in sent.tokens]
      metas = sent.comments[:-2]
      meta_key = "_zzqz_".join([item for item in metas])
      sent_proc = process_threegram_hyphens(data, sent)
      if meta_key in input_dict.keys():
        error_log = [k, meta_key]
        errorlist.append(error_log)
      input_dict[meta_key] = sent_proc
  return   input_dict
##
def run_twogram_hyphen_processing(input_dict):
  output_dict = {}
  for key,value in tqdm(zip(input_dict.keys(), input_dict.values())):
      metas = key
      new_key = key
      # metas_tidy = metas.replace('_zzqz_','\n')
      # _ = w.write(spacer)
      # _ = w.write(metas_tidy)
      # _ = w.write(spacer)
      sent_lines = value
      data = [(el[1], el[3]) for el in sent_lines]
      # for el in sent_lines:
      t = 0
      current_sent = []
      while t < len(data)-1:
          # print(t)
          # if have verb or aux
          if data[t][1] in ("AUX",'VERB') and data[t+1][0] in hyphen_list and data[t+2][1] == "PRON":
            # aciton if verb or ayx
            # print verb/AUX
            tok_str = make_tokstr_from_dict(sent_lines, t)
            current_sent.append(tok_str)
            # _ = w.write(tok_str)
            # print(tok_str)
            hyphen_pron_tokenstr = make_hyphen_pron_tokenstr_fromdict(sent_lines, t)
            current_sent.append(hyphen_pron_tokenstr)
            # _ = w.write(hyphen_pron_tokenstr)
            # print(hyphen_pron_tokenstr)
            t+=3 # t = verb, t+1 = punct, t+2 = pron
          # if neither verb nor action
          else:
            # print(t, "Action if pos[t] is not verb/aux")        
            tok_str = make_tokstr_from_dict(sent_lines, t)
            current_sent.append(tok_str)
            # _ = w.write(tok_str)
            # print(tok_str)
            t+=1 
      if t  ==len(data) -1:
            tok_str = make_tokstr_from_dict(sent_lines, t)
            # _ = w.write(tok_str)
            # _ = w.write(spacer)
            current_sent.append(tok_str)
      output_dict[new_key] =   current_sent
  return output_dict

def write_conll_from_outputdict(output_dict, output_conllfile_name):
  '''
  take output_dict, extract strings, make final meta lines, sequentialise index values and write to file
  '''
  with open(output_conllfile_name, 'w', encoding='UTF-8') as w:
    for key, value in tqdm(zip(output_dict.keys(), output_dict.values())):
        metas = key
        sent_lines = value
        metas_tidy = metas.replace('_zzqz_','\n')
        metas_tidy = "\n" + metas_tidy + '\n'+ '#text_prefab = '  + "// ".join([line[1] for line in sent_lines]) + "\n"
        # need to regenerate prefab_text from string
        _ = w.write(metas_tidy)
  
        for x in range(len(sent_lines)):
          raw_line = sent_lines[x]
          raw_line[0] = str(x+1)
          raw_line[6] = str(x+1)
          print_line = "\t".join([item for item in raw_line])+"\n"
          _ = w.write(print_line)

# def generate_hyphen_combinations(hyphen_list, pronlist):
#   combinations = list(itertools.product(hyphen_list, pronlist))
#   custom_cases = [str(item[0]) + str(item[1]) for item in combinations]
#   return custom_cases
# custom_cases = generate_hyphen_combinations(hyphen_list, pronlist)



#
#source_doc.sentences[29]
#i=1
#if set([current_sent.tokens[i].text in punctuation_set for i in range(len(current_sent.tokens))]) == "False"

# import itertools, glob
# 
# epen_t_forms =[str(item[0]) + str(item[1]) for item in list(itertools.product(hyphen_list, ['t']))]
# import string    

########################################################################################################################################################################
########################################################################################################################################################################
############################                END OF DEFINITIONS          #################################################################################################
########################################################################################################################################################################
########################################################################################################################################################################

  #all_processed = []
  #for current_sent in source_doc.sentences:
  #  tidylines = make_tidylines(current_sent)
  #  all_processed.append(tidylines)

  #new
# updated_exc_cases = list(set(exceptionalCases + custom_cases))


## runsection
## specify file where pickled punt set is stored, load
file_path = '/Users/Data/ANR_PREFAB/Code/punctuation_set.pkl'
with open(file_path, 'rb') as file:
  puctuation_set = pickle.load(file)
  punctuation_set = puctuation_set

# specify hyphen list and pron list members
hyphen_list = ("–","-", "—","−")
pronlist = ("le","les","la", "moi","toi","soi", "lui","leur","en","y", "je", "tu", "il", "elle", "on", "ça", "ca", "nous", "vous", "ils", "elles",
"en","ce","ci","là", "j'", "t'", "c'")

## run section : rather than run with funct, run as loop
# input_files = glob.glob('/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/lemmatised/*/*-6000tv8.conllu')
# input_files = glob.glob('/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/[VH]/[VH]_0[1]/[VH]_0[1]-6000tv8.conllu')
# input_files = [current_file] #sorted(glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/aligned_prefab_processing_archives/aligned_FR_DE_archive_zips/TS52/*6000tv8.conllu'))
input_files = glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3==v7/Z/Z_00/*-6000tv8.conllu')


# specify the input files to operate over
input_files = glob.glob('/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/phraseorom_prefabV2/new_sents_to_process/new_sents_to_process/new_sents_to_process-6000tv8.conllu')
# specify the path if saving to different directory from input
pathB = '/Users/Data/ANR_PREFAB/bounce/'
# iterator if desired for processing : load source file
for current_file in tqdm(input_files):
  output_file = current_file.replace('6000','mooo_moo_6022v3test_tues2')
  pathA = os.path.dirname(output_file)
  pathB = pathA
  output_file = output_file.replace(pathA, pathB)
  source_doc = import_prepare_for_stanza(current_file)
  # when source loaded, process leading punctuation and tidy
  outputstrings = process_punct_and_tidy(source_doc)
  print_outputstrings(outputstrings, output_file)


## this is the end of the first step ; files are imported, processed, exported.

#keylist = list(input_dict.keys())[598]

#this is step2 : dealing with hyphens
# step : process 3-grams from soure_doc, send to dict
############################################# processing loop that works to send -t- PRON to single token :
## clear source_doc if exists
if 'source_doc' in globals():
    del globals()['source_doc']
    print('source_doc deleted')    

# specify file input
source_filepath = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/phraseorom_prefabV2/new_sents_to_process/new_sents_to_process/new_sents_to_process-output_from_tidy_doc.conllu'
source_filepath = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/Romans_V3_archive/by_corpus/FYv3/FY01/FY01-6022v2tv8.conllu'
input_dict = run_threegram_hyphen_processing(source_filepath)
output_dict = run_twogram_hyphen_processing(input_dict)

output_conllfile_name = source_filepath.replace('6022','6022_test_with_corrections_')
output_conllfile_name = "/Users/Data/writetest5.conllu"
write_conll_from_outputdict(output_dict, output_conllfile_name)
## process 2 grams from dict and print :: success : now need to increment index

## now have tidy data all in dict, ready to print

###end    

#par-dessus en 3

test_doc = CoNLL.conll2doc('/private/var/folders/rh/w3v2n4rs3cz_8pf7g_c75_kh0000gn/T/fz3temp-2/GE68-7230v2v2tv8.conllu')      
      
