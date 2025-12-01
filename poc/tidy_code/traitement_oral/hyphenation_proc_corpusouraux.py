from tqdm import tqdm
from stanza.utils.conll import CoNLL
import itertools, glob, re, os, json, platform

## tidy functions
def make_token_conll_list(sent, t):
  '''
  split a string of conll annoations into a list of 10 items and return list
  Called by process_threegram_hyphens
  '''
  tok_list= sent.tokens[t].to_conll_text().split("\t")
  return  tok_list

def prepare_sentence(conll_sentence):
  '''
  called to make strings for sent splitter getting all comments and text
  Called by split_conll_sentence_with_suffix
  '''
  conll_sentence = [comment for comment in conll_sentence.comments] + [token.to_conll_text() for token in conll_sentence.tokens]
  return conll_sentence

def make_hyphen_pron_tokenstr_from3(sent, t):
  '''
  operating over three tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  Called by process_threegram_hyphens
  '''
  elements_A = sent.tokens[t+0].to_conll_text().split("\t")
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  new_element = [elements_A[0], str(elements_A[1] + elements_B[1] + elements_C[1])]+ elements_C[2:]
  hyphen_pron_token_list = [element for element in new_element]
  return hyphen_pron_token_list

def make_hyphen_pron_tokenstr_from3special(sent, t):
  '''
  operating over three tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  Called by process_threegram_hyphens
  Special variant to deal with cases where t has no preceding - :: this function avoids gluing the verb to the epenthetical form
  '''
  # element A gets ignored
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  token_for_B = elements_B[1]
  if len(token_for_B) ==1:
    token_for_B = "-t"
  new_element = [elements_B[0], str(token_for_B + elements_C[1]) ]+ elements_C[2:]
  hyphen_pron_token_list = [element for element in new_element]
  return hyphen_pron_token_list


def make_pardes_tokenstr_from3(sent, t):
  '''
  operating over three tokens, get the annotations for each and use index fromA, forms from A,B,C, remainder from C
  Called by process_threegram_hyphens
  '''
  elements_A = sent.tokens[t+0].to_conll_text().split("\t")
  elements_B = sent.tokens[t+1].to_conll_text().split("\t")
  elements_C = sent.tokens[t+2].to_conll_text().split("\t")
  ############### index from A, left from A, right from B, PRON from C
  new_form =  str(elements_A[1] + elements_B[1] + elements_C[1])
  new_element = [elements_A[0], new_form, new_form.lower()]+ elements_C[3:]
  new_element_list = [element for element in new_element]
  return new_element_list

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
  Called by run_threegram_hyphen_processing_for_oraux
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
        if data[t][1] == "PRON" and data[t+1][1] == "PUNCT" and data[t+2][1] == "PRON" and data[t][0] in epen_t_forms and data[t+2][0] in pronlist:
          hyphen_pron_token_list = make_hyphen_pron_tokenstr_from3(sent, t)
          sent_proc.append(hyphen_pron_token_list)
          #print(t, "SuccessB")
          t+=3 # t = verb, t+1
          jump=True
        # handle cases where epenthetical t has no leading t : POS is VERB or X, t+2 is PRON, form of t+1 is in epen forms and form of t+2 ends with a pron
        elif data[t][1] in ("VERB", "AUX")  and data[t+2][1] == "PRON" and data[t+1][0] in epen_t_forms and data[t+2][0][1:] in pronlist:
          tok_list = make_token_conll_list(sent, t)
          sent_proc.append(tok_list)
          hyphen_pron_token_list = make_hyphen_pron_tokenstr_from3special(sent, t)
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
      if jump and ((t == len(data)-1)  ):
          tok_list = make_token_conll_list(sent, t)
          sent_proc.append(tok_list)
          jump = False

      if jump and ((t >= len(data)))  :
          jump = False
         
    return sent_proc

def run_twogram_hyphen_processing_for_oraux(input_dict):
  output_dict = {}
  input_dict_size = len(input_dict)
  for key,value in tqdm(zip(input_dict.keys(), input_dict.values())):
      metas = value[0]
      new_key = key
      # metas_tidy = metas.replace('_zzqz_','\n')
      # _ = w.write(spacer)
      # _ = w.write(metas_tidy)
      # _ = w.write(spacer)
      sent_lines = value[1]
      data = [(el[1], el[3]) for el in sent_lines]
      # for el in sent_lines:
      t = 0
      current_sent = []
      if isinstance(len(data), int) and len(data) > 0:
        if len(data) ==1:
          if t ==0 :
            tok_list = make_tokstr_from_dict(sent_lines, t)
            current_sent.append(tok_list)
            result = metas, current_sent      
            output_dict[new_key] =   result

      jump =  False
      if len(data) > 1:

          while t < len(data)-2:
              # print(t)
              # if have verb or aux
              if data[t][1] in ("AUX",'VERB') and data[t+1][0] in hyphen_list and data[t+2][1] == "PRON":
                jump=False
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
                jump = True
              # if neither verb nor action
              else:
                # print(t, "Action if pos[t] is not verb/aux")        
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                # _ = w.write(tok_str)
                # print(tok_str)
                t+=1 
          if t  ==len(data) -2:
                jump=False
                tok_str = make_tokstr_from_dict(sent_lines, t)
                # _ = w.write(tok_str)
                # _ = w.write(spacer)
                current_sent.append(tok_str)
                t+=1
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                

          if jump and ((t == len(data)-1)  ):
                tok_str = make_tokstr_from_dict(sent_lines, t)
                current_sent.append(tok_str)
                jump = False            
          result = metas, current_sent      
          output_dict[new_key] =   result
          output_dict_size = len(output_dict)
  return output_dict, input_dict_size, output_dict_size

def run_threegram_hyphen_processing_for_oraux(source_doc):
  '''
  read conll from file, iterate over sentences processing threegram_hyphens, outputting to dictionary
  input_dict : dict of key = metadata lines from sentID and orig tokenisation
  '''
  input_dict, errorlist, spacer = {},[], "\n"
  sent_count_in = len(source_doc.sentences)
  for k, sent in tqdm(enumerate(source_doc.sentences)):
      data = [(token.to_conll_text().split('\t')[1].replace('#',''), token.to_conll_text().split('\t')[3])  for token in sent.tokens]
      metas = "\n".join([comment for comment in sent.comments[:-1]]) + '\n'
      meta_key = sent.comments[0]
      sent_proc = process_threegram_hyphens(data, sent)
      if meta_key in input_dict.keys():
        error_log = [k, meta_key]
        errorlist.append(error_log)
      result = metas, sent_proc
      input_dict[meta_key] = result
  if len(errorlist)>0:
    print(errorlist)
  return   input_dict, sent_count_in

def write_conll_from_outputdict_for_oraux(output_dict, output_conllfile_name):
  '''
  take output_dict, extract strings, make final meta lines, sequentialise index values and write to file
  '''
  with open(output_conllfile_name, 'w', encoding='UTF-8') as w:
    for key, value in tqdm(zip(output_dict.keys(), output_dict.values())):
        sent_lines = value[1]
        metas_tidy = f"\n{value[0]}" 
        # need to regenerate prefab_text from string
        _ = w.write(metas_tidy)
  
        for x in range(len(sent_lines)):
          raw_line = sent_lines[x]
          raw_line[0] = str(x+1)
          raw_line[6] = str(x+1)
          raw_line[2] =  re.sub(r"(.+?)'$",r"\1e", raw_line[2]) ## if lemma, for strange reason, ends wiht apostrophe, add E in its place
          print_line = "\t".join([item for item in raw_line])+"\n"
          # print(print_line)
          _ = w.write(print_line)

def load_hashtag_repl_dict():
  dict_file = f'{pathHead}/Code/code_for_gitlab/utilities/orfeo_hashtag_repl_dict.json'
  with open(dict_file, "r") as f:
      repl_dict = json.load(f)
  return repl_dict

def import_conll_with_selective_tidying(input_conll_file):
  '''
  take a path to a conll file and return source_doc, a ConLL document object
  this function determines if the file is from the ORFEO subcorpus, and corrects a specific error with the files : # are present in place of spaces in some multiword tokens. regexes are used to find and replace these based on an exhaustive dictionary. incorrect formes and lemmas are replaced with correct forms and tokens
  selection of the path to take to import is based on the presence of 'orfeo' in the file path. if this is true, the file is loaded as data, regex applied, and the data parsed as a conll object
  if the path does not contain orfeo, the source file is directly parsed into a source_doc
  args : input_conll_file : absolute path to conll file to process
  returns : source_doc : a parsed conll doc 
  '''
  if 'orfeo' in input_conll_file.lower():
    # print("Importing conllu and tidying any hashtag errors within polylexical tokens")
    with open(input_conll_file, 'r', encoding='UTF-8') as f:
      data = f.read()
    # do regex to replace with dict
    for patt, repl in tqdm(zip(repl_dict.keys(), repl_dict.values())):
      data = re.sub(patt, repl, data)
    
    source_doc = CoNLL.conll2doc(input_file=None, input_str = data)    
    
  else:
    # print("Importing conllu ; no hashtag tidying necessary")
    source_doc = CoNLL.conll2doc(input_file=input_conll_file)    

  return source_doc

### declarations


hyphen_list = ("–","-", "—","−")
pronlist = ("le","les","la", "moi","toi","soi", "lui","leur","en","y", "je", "tu", "il", "elle", "on", "ça", "ca", "nous", "vous", "ils", "elles","en","ce","ci","là", "j'", "t'", "c'")
epen_t_forms = ["t"] + [str(item[0]) + str(item[1]) for item in list(itertools.product(hyphen_list, ['t']))]

load_hashtag_repl_dict()

if platform.system() == "Darwin":
  pathHead = "/Users/Data/ANR_PREFAB/"
if platform.system()=="Windows":
  pathHead = "C:/Users/Data/ANR_PREFAB/"

## run section
# load dict for replacements


## special loader for oral : no sentence splitting, just need to reattach - and keep all commebts and tidy strange # error with regex
# specify file, load as data
# input_conll_file = 'C:/Users/Data/PREFAB/corpusV3prep/oral_as_on_turing/ORFEO_prefabV2/tcof/Incen_prov.conllu'
input_conll_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/CorpusOrauxV2FromTuring/ORFEO_prefabV2/cfpp/Youcef_Zerari_H_29_Abdel_Hachim_H_25_SO.conllu'

regenerated: ESLO, ORFEO, PronType=Dem
todo: , , TCOF
subcs = ['REPAS', 'CONF', 'DIA', 'DISC', 'ENT', 'ENTJEUN','ITI', 'CINE']
subcs= ["cfpb","cfpp","clapi","coralrom","crfp","fleuron","frenchoralnarrative","ofrom","reunions-de-travail","tcof","tufs","valibel"]
for subc in subcs:
these_files = glob.glob(f'/Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/CorpusOrauxV2FromTuring/TCOF2_prefabV2/*.conllu')
for input_conll_file in tqdm(these_files):
      output_conll_file = input_conll_file.replace("CorpusOrauxV2FromTuring","CorpusOrauxV3").replace('_prefabV2','_prefabV3')#.replace('.conllu','-d2.conllu').replace('prefabV2','prefabV3')
      source_doc = import_conll_with_selective_tidying(input_conll_file)
      input_dict, sent_count_in = run_threegram_hyphen_processing_for_oraux(source_doc)
      output_dict, input_dict_size, output_dict_size = run_twogram_hyphen_processing_for_oraux(input_dict)
      write_conll_from_outputdict_for_oraux(output_dict, output_conll_file)


all_tcof = CoNLL.conll2doc('/Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/CorpusOrauxFromTuring/ORFEO_prefabV3/tcoftest.conll')
len(all_tcof.sentences)
all_tcof.num_words


my_command = f'open -a /Applications/BBEdit.app {output_conll_file}'
os.system(my_command)


## end tidy declarations
k=239
sent = source_doc.sentences[k]
input_dict = {}

  Code/code_for_gitlab/traitement_oral/"




# parse from data



## functions to tidy



sent_proc = process_threegram_hyphens(data, sent)





input_dict['#sent_id = cefc-tcof-Incen_prov-1'][1]


# nto used
def split_conll_sentence_with_suffix(this_sentence):
    """
    Splits a CoNLL formatted sentence into two at the occurrence of '…' character.
    The sent_id in comment lines is suffixed with Greek letters (α, β, γ, etc.).
    
    Args:
        conll_sentence (list of str): The input CoNLL sentence, where each element is a line.
    
    Returns:
        list of list of str: A list containing one or more CoNLL sentences depending on whether the special character was found.
    """
    conll_sentence = prepare_sentence(this_sentence)
    split_sentences = []
    current_sentence = []
    found_split_char = False
    suffix_index = 0
    greek_letters_raw = ['α', 'β', 'γ', 'δ', 'ε', 'ζ', 'η', 'θ', 'ι', 'κ', 'λ', 'μ', 'ν', 'ξ', 'ο', 'π', 'ρ', 'σ', 'τ', 'υ', 'φ', 'χ', 'ψ', 'ω']

    greek_letters = []
    for i in greek_letters_raw:
      for j in greek_letters_raw:
       # Convert characters to lowercase letters (a-z)
        pair = str(i)+str(j)
        greek_letters.append(pair)
    
    EOS_list = ("...",'…',';',':') # list of forms to recognise as end of sentences

    for line in conll_sentence:
        # Handle comment lines
        if line.startswith("#"):
            if "sent_id" in line:
                if found_split_char:
                    base_id = line.split("=")[1].strip()
                    new_line = f"#sent_id = {base_id}{greek_letters[suffix_index]}"
                    suffix_index += 1
                else:
                    base_id = line.split("=")[1].strip()
                    new_line = f"#sent_id = {base_id}{greek_letters[suffix_index]}"
                    suffix_index += 1
                current_sentence.append(new_line)
            else:
                current_sentence.append(line)
        else:
            tokens = line.split("\t")
            if len(tokens) > 1 and tokens[1] in EOS_list:
                if len(tokens[1]) ==3:
                  special_step = 1
                else :
                  special_step =1
                found_split_char = True
                current_sentence.append(line)
                split_sentences.append(current_sentence)
                # Start a new sentence with the same comments, except the sID line
                current_sentence = [
                    l if not l.startswith("#sent_id") else f"# sent_id = {base_id}{greek_letters[suffix_index]}"
                    for l in conll_sentence if l.startswith("#")
                ]
                suffix_index += special_step
            else:
                current_sentence.append(line)
    
    if current_sentence:
        split_sentences.append(current_sentence)
    
    if not found_split_char:
        return [conll_sentence]
    
    return split_sentences
def get_tok_count(sent):
  count =0
  for line in sent:
    if line.startswith("#") is False:
      count +=1
  return int(count)
def run_line_reindexer(sent):
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
def write_conllu_file_for_InitialAproces(conllu_file_for_InitialAproces, all_splits):
    with open(conllu_file_for_InitialAproces, 'w', encoding='UTF-8') as p:
      for block in all_splits:
        # for block in orig_sentblock:
          print_line = "\n\n" + "\n".join([chunk for chunk in block]) + "\n\n"
          _ = p.write(print_line)






def check_counts(xml_tag_len,input_dict_size, output_dict_size,  output_sent_count):
    error_report = []
    if not all(isinstance(value, int) for value in [xml_tag_len,input_dict_size, output_dict_size,  output_sent_count]):
      error_message = [chunk, xml_tag_len,input_dict_size, output_dict_size,  output_sent_count, "NON_INT"]
      error_report.append(error_message)
    if not (xml_tag_len == input_dict_size == output_dict_size == output_sent_count):
      error_message = [chunk, xml_tag_len,input_dict_size, output_dict_size,  output_sent_count, "LEN_DIFF"]
      error_report.append(error_message)
    return error_report


error_log = []
error_log_file = re.sub('/$','log.csv', path)
for chunk in (chunks[4:10]):
    # make paths
    live_path = path + chunk + "/" 
    print(f'\nProcessing {chunk}\n')
    input_conll_file = glob.glob(live_path + "*6022v2tv8.conllu")[0]
    output_conll_file  = input_conll_file.replace('.conllu','hyphenprocessed.conllu')
    xml_with_dd = glob.glob(live_path + "*PREFABv2*.xml")[0]
    conllu_file_for_InitialAproces = output_conll_file.replace('6030','6130')
    if input_conll_file and xml_with_dd:
        # processing
        input_conll_file = 'C:/Users/Data/PREFAB/corpusV3prep/TCOF1_as_on_turing/Incen_prov.conllu'
        source_doc = load_and_tidy_conll(input_conll_file)
        input_dict, sent_count_in = run_threegram_hyphen_processing(source_doc)
        output_dict, input_dict_size, output_dict_size = run_twogram_hyphen_processing(input_dict)
        write_conll_from_outputdict(output_dict, output_conll_file)
        input_conll_doc = CoNLL.conll2doc(output_conll_file)
        updated_conll_doc = update_conll_with_dd(input_conll_doc, dd_info_list)
        all_splits, output_sent_count = run_sentence_splitt_pipe(updated_conll_doc)
        write_conllu_file_for_InitialAproces(conllu_file_for_InitialAproces, all_splits)

        error_report = check_counts(xml_tag_len,input_dict_size, output_dict_size,  output_sent_count)
        if error_report:
          error_log.append(error_report)
          with open(error_log_file, 'w', encoding='UTF-8') as log:
            for report in error_log:
              line = "\t".join([str(item) for item in report]) + "\n"
              log.write(line)
    else:
      error_report = [chunk, "_", "_", "_", "_", "_", "NO_XML"]
      error_log.append(error_report)
      with open(error_log_file, 'w', encoding='UTF-8') as log:
        for report in error_log:
          line = "\t".join([str(item) for item in report]) + "\n"
          log.write(line)
path = current_folder =  'C:/Users/Data/PREFAB/corpusV3prep/TR/'
subfolders = glob.glob(current_folder + "*/" )
chunks = [item.replace(current_folder[:-1], '').replace('\\', '').replace('/', '') for item in subfolders]
chunks
