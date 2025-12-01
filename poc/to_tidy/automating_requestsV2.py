# current, imperfect version of automatic converter of requetes from excel sheet with tokenisation and conditions
import pandas as pd
import numpy as np
import re
from tqdm import tqdm

# ppi_reqs = pd.read_excel('/Users/Data/Downloads/requetes_PPI_annotations_4_09_24.xlsx')
# ppi_reqs.columns = ['PPI', 'PPIcorr','TOK','wblocks','POS','contraintes','req_rom','waasasdfas','req_wik','oral','comment']


discordantiel_forms = ("<w=N'>", "<w=n'>", "<w=ne>", "<w=Ne>")

ppi_reqs_fem = pd.read_excel('/Users/Data/requestsv3.xlsx', sheet_name="Sheet2",  usecols=[1,2,3,4,5,6], header=None)
ppi_reqs_fem.columns
for col in ['req_rom','waasasdfas','req_wik','oral','comment']:
  ppi_reqs_fem[col] = ""
ppi_reqs_fem.columns = ppi_reqs.columns
all_ppi_reqs = pd.concat([ppi_reqs, ppi_reqs_fem], axis=0)
all_ppi_reqs.reset_index(inplace=True, drop=True)

# functions from dev stage not used in extracting frmo xlsx
# # def add_num_reqs(x, z):
#   # make requiremetn for right hand side : 
#   numRHSreq = f'num(#{x})<={z}'
#   # make requirement to add to w element
#   w_req = f',#{x}'  
#   return numRHSreq, w_req

# def insert_w_req(w_req, x, w_el_list):
  # target_el = w_el_list[x-1]
  # updated_el = re.sub(">", f'{w_req}>', target_el)
  # w_el_list[x-1] = updated_el
  # return w_el_list

# def optional_discordantiel(allow, w_el_list):
  # '''
  # function to go and get the token previous to an optional discordantiel, and elide the E if necessary
  # '''
  # special_string = '{1}+<>{0,1}'
  # if allow == "allow":
  # # get list of tuples from LC
  #   int_result =  [(c, special_string)  if (chunk in discordantiel_forms) else ('-1', chunk) for c, chunk in enumerate(w_el_list)]
  #   # get w_elsfrom tuple
  #   w_el_list = [chunk[1] for chunk in int_result]
  #   # get indx where optional inserted
  #   # get c values where there was a discordantiel present : then remove 1 from indx to get that of preceding form
  #   c_values = np.array([chunk[0] for chunk in int_result if int(chunk[0])>0])
  #   pronoun_values = c_values -1
  #   verb_values = c_values +1
  #   for verb_value in verb_values:
  #     # test if first letter of verb is in list of vowels
  #     if w_el_list[verb_value][3:4] in ('A','E','H','I','O','U','a','e','h','i','o','u','é','è'):
  #       # loop over the preceding forms, and if any end with an E, replace the e with an elision and send this to the list
  #       for pronoun_value in pronoun_values:
  #         if POS_el_list[pronoun_value] =="PRON" and w_el_list[pronoun_value].endswith("e>"):
  #           w_el_list[pronoun_value] = w_el_list[pronoun_value].replace("e>","'>")
  #   
  # else:
  #   w_el_list = w_el_list
  # return w_el_list
# def make_LHS(w_el_list, punct_flag):
#   if punct_flag == "punct":
#     LHS = w_el_list + ['<c=PUNCT>']
#   elif punct_flag == "nopunct":
#     LHS = w_el_list
#   return LHS

# def lemmatise_ADJ(w_el_list, POS_el_list):
#   ADJ_values = [  i for i in range(len(w_el_list)) if POS_el_list[i] == "ADJ"]
#   if ADJ_values:
#     for this_value in ADJ_values:
#       w_el_list[this_value] = w_el_list[this_value].replace('w=','l=')
# def make_requete(i, x, y, punct_flag, dd_flag):
#   # i=0# row num in df
#   # x =1 #word we want to add requirement on 
#   # y =3 # range we want to specify
#   # syntax = []
#   w_el_list = get_w_el_list(i)
#   POS_el_list = make_POS_el_list(i)
#   add_fs_form(w_el_list, POS_el_list)
#   # lemmatise_ADJ(w_el_list, POS_el_list)
#   # w_el_list = optional_discordantiel("allow", w_el_list)
#   numRHSreq, w_req = add_num_reqs(x, y)
#   w_el_list = insert_w_req(w_req, x, w_el_list)
#   LHS = make_LHS(w_el_list, punct_flag)
#   RHS = make_RHS(numRHSreq,  dd_flag)
#   full_string = join_sides(LHS, RHS)
#   return str(full_string)
# 
# 
# ### need to lemmatise PRON
#   
# results = []
# x=1
# y=3
# punct_flag = "punct"
# dd_flag = "dd"
# for i in range(0, 40):
#   full_string = make_requete(i, x, y,  punct_flag, dd_flag)
#   w_string = make_requete(i, x, y,  punct_flag, "no_dd")
#   result = full_string, w_string
#   results.append(result)
#   print(full_string)
# 
# outputtestdoc = "/Users/Adam/Desktop/requeststest.csv"
# with open(outputtestdoc, 'w', encoding='UTF-8') as w:
#   for result in results:
#     line = str(result[0]) + "\n" #str("\t") +  str(result[1]) +  "\n"
#     _ = w.write(line)
#     
#     
# with open(outputtestdoc, 'w', encoding='UTF-8') as w:
#   for i in range(len(ppi_reqs)):
#     plaintext = str(i+1) + "\t" + str(ppi_reqs.iloc[i,0]) + "\n"
#     _ = w.write(plaintext)
# 
# View(ppi_reqs.head(20))
# 
# for i in range(len(ppi_reqs)):
#   if isinstance(ppi_reqs.iloc[i, 1], str) :
#     print(i, ppi_reqs.iloc[i, 0], ppi_reqs.iloc[i, 1])
#   i=4607+2
# 

all_ppi_reqs = pd.read_excel('/Users/Data//ANR_PREFAB/requetes_lexicoscope/requestsv5.xlsm')
all_ppi_reqs = pd.read_excel('/Users/Data/ANR_PREFAB/requetes_lexicoscope/RequetesV9.xlsx', usecols=[1,2,3,4,5,6,7])

all_ppi_reqs.columns
ppi_reqs = all_ppi_reqs
## live functions
def get_w_el_list(i):
  '''
  get the list of w elements from the column in the excel df and tidy
  
  '''
  err_string = 'w=\\,'
  corr_string = 'w=,'
  # i=40
  w_elements_str = ppi_reqs.iloc[i, 3]
  
  #  if escaped comma string present in source, replace with unescaped
  if err_string in w_elements_str:
    w_elements_str = w_elements_str.replace(err_string,corr_string)

  # replace on end of <>, then on >?< to process optionals, then split on the added $ to yield list
  w_el_list = w_elements_str.replace("><",">$<").replace(">?<",">?$<").split("$")
  
  return w_el_list
def make_POS_el_list(i, w_el_list):
  '''
  get the list of POS elements form the POS column, then get w_el_list and insert PUNCT if optional , present
  ''' 
  POS_el_list = ppi_reqs.iloc[i, 4].split("// ")
  punct_locs = [i for i in range(len(w_el_list)) if w_el_list[i] == "<w=,>?"]
  if punct_locs:
    for punct_loc in punct_locs:
      POS_el_list.insert(punct_loc, "PUNCT")
  
  return POS_el_list

def make_RHS(RHS_extracted):
  '''
  return RHS in 3 variants, first with DD req and num, second with DD req only, third with num only
  '''
  RHS_ddw, RHS_dd, RHS_w = [],[], []
  get_ddstring = ' tag=s[type=dd]'
  # regex to isolate numreq
  

  # Search for the match in the complex string
  pattern = r"num\(#\d+\)\s*(<=|>=|<|>|=)\s*(\d+)"
  match = re.search(pattern, RHS_extracted)

  num_req = (match.group(0)) 
  
  syntax_req = re.findall(r'&& (\(.+\))', RHS_extracted)
  if len(syntax_req)>0:
    syntax_req = syntax_req[0] 
  else:
    syntax_req = "_blank_"
  
  RHS_dd_syn_num = f":: {num_req} && {syntax_req} &&{get_ddstring}".replace("&& _blank_ ","").replace(":: _blank_ ",":: ").replace(":: &&",":: ")
  RHS_dd_syn = f":: && {syntax_req} &&{get_ddstring}".replace("&& _blank_ ","").replace(":: _blank_ ",":: ").replace(":: &&",":: ")
  RHS_dd_num = f":: {num_req} &&{get_ddstring}".replace("&& _blank_ ","").replace(":: _blank_ ",":: ").replace(":: &&",":: ")

  RHS_syn_num = f":: {num_req} && {syntax_req}".replace("&& _blank_ ","").replace(":: _blank_ ",":: ").replace(":: &&",":: ")
  RHS_syn = f":: && {syntax_req} ".replace("&& _blank_ ","").replace(":: _blank_ ",":: ").replace(":: &&",":: ")
  RHS_num = f":: {num_req} ".replace("&& _blank_ ","").replace(":: _blank_ ",":: ").replace(":: &&",":: ")
  
  RHS_strings = [RHS_dd_syn_num, RHS_dd_syn, RHS_dd_num, RHS_syn_num, RHS_syn, RHS_num]
  return RHS_strings

def join_sides(LHS, RHS):
  '''
  LCs and joins to join LHS, RHS into tidy string
  '''
  LHS_string = "".join([element for element in LHS])
  RHS_string = "".join([element for element in RHS])
  full_string = f'{LHS_string} {RHS_string}'
  return full_string


#i=i=412
def extract_mot_def_extr(left_raw):
  '''
  get the x and y values : x == mot, y = # word in query
  '''
  mot_def_str = r"Mot (\d+) = #(\d+)"
  if len(left_raw) >0:
    mot_def_extr = re.findall(mot_def_str, left_raw)
    x, y = mot_def_extr[0]
    return x, y

def extract_LHS_end(left_raw):
  '''
  get the ending requirement, if any, from the xlsx
  '''
  mot_def_str = r"Mot (\d+) = #(\d+)"
  lhs_end_int = re.sub(mot_def_str, '', left_raw)
  lhs_end = lhs_end_int.replace('et finit par', '').replace('  ',' ')
  return lhs_end

## get requirements from XLSX input  
def extract_reqs(i):
  ''' 
  get all reqs with specific functions
  '''
  left_raw, RHS_extracted = extract_RHS(i)  
  # item x will have label y
  x, y = extract_mot_def_extr(left_raw)
  lhs_end = extract_LHS_end(left_raw)
  return x, y, lhs_end, RHS_extracted
  
def extract_RHS(i):
  '''
  get the RHS with a split on ::
  '''
  contrainte_raw= ppi_reqs.iloc[i, 5]
  if isinstance(contrainte_raw, str):
    if "::" in contrainte_raw:
      left_raw, RHS_extracted = contrainte_raw.split("::")
  else:
    left_raw, RHS_extracted = "", ""
  
  
  return left_raw, RHS_extracted 
  
def set_num_reqs(x, y, w_el_list):
  '''w1 is hash1 ==
  w1 = w in teh w_el_string == x
  hash1 is the hash number we want to attribute to the word in the requete hash == y
  '''
  # make requirement to add to w element
  w_el_list_num = w_el_list
  w_req = f',#{y}'  
  x = int(x)
  if x-1 in range(len(w_el_list_num)):
    #use this loop to add w to an existing word
    target_el = w_el_list_num[x-1]
    updated_el = re.sub(">", f'{w_req}>', target_el)
    w_el_list_num[x-1] = updated_el
  
  ## deal with monotokens
  if int(x) > len(w_el_list) and len(w_el_list) ==1:
    #use this loop to add w to last word : decrement x by 1
    x = x-1
    # copyof standard loop
    if x-1 in range(len(w_el_list_num)):
      #use this loop to add w to an existing word
      target_el = w_el_list_num[x-1]
      updated_el = re.sub(">", f'{w_req}>', target_el)
      w_el_list_num[x-1] = updated_el
  return w_el_list_num

def make_self_as_string(input_as_list):
  '''
  take a list and return it as a string with no sep value
  '''
  outputstring = "".join([chunk for chunk in input_as_list])
  return str(outputstring)


def make_blanks_lhs(i,x, RHS_extracted, lhs_end):
  '''
  make the whole LHS with PUNCT and optional blanks 
  '''
  # get the list of word elements
  blanks_w_el_list = get_w_el_list(i)
  # get list of PUNCT and the optional tokens to go to the left of the PPI
  lsh_blanks = make_lhs_blanks(RHS_extracted, x)
  # blanks_w_el_list.insert(int(x)-1, [chunk for chunk in lsh_blanks])
  # lefts = [blanks_w_el_list[c] for c in range(0, int(x)-1) ]
  # rights = [blanks_w_el_list[c] for c in range((int(x)-1), len(blanks_w_el_list)) ]  
  output_list = lsh_blanks + blanks_w_el_list
  if '<c=PUNCT>' in lhs_end:
    output_list = output_list + ['<c=PUNCT>']
  return output_list

def make_lhs_blanks(RHS_extracted, x):
  '''
  make a list of PUNCT followedby optional tokens to insert to the left of the PPI. THis function runs inside make_blanks lhs
  '''
  b_string = ['<>?']
  # num_req = re.findall(r'num\(#\d+\).+(\d+)', RHS_extracted)
  pattern = r"#\d+\)\s*(<=|>=|<|>|=)\s*(\d+)"

  # Search for the match in the complex string
  match = re.search(pattern, RHS_extracted)
  n_value = int(match.group(2)) -  int(x)
      
  
  blanks_needed = ['<c=PUNCT>']+[item for item in b_string for c in range(n_value)]
  lhs_blanks = blanks_needed
  return lhs_blanks

def empty_catcher(i, droplist):
  '''
  assume that we wnt to run, but check that we actually have strings to process. if not, set the run value to skip, which will force the next loop into the else block
  '''
  run_value = "run"
  if i in droplist:
    run_value = "skip"
  else:
    L_raw, r_raw = extract_RHS(i)  
    if '' == L_raw and '' == r_raw :
      run_value = "skip"
  return run_value
    
  
# make storage lists
#ppi_reqs_orig = ppi_reqs
droplist = [i for i in range(len(all_ppi_reqs)) if all_ppi_reqs.iloc[i,0] == "Exclu"]
ppi_reqs = all_ppi_reqs
missing_count =0
wikibras1_store, wikibras2_store,  romans_bras1_store, romans_bras2_store = [], [], [], []
oral3store, oral2store, oral1store = [],[],[]

len(wikibras1_store)
# loop
# i=233
# 4954= last row with ppi
for i in tqdm(range(len(ppi_reqs))):
  # run_value = empty_catcher(i, droplist)  
  if run_value == "run":
    ## get x, y, ending req for LHR and RHS as in original
    x, y, lhs_end, RHS_extracted = extract_reqs(i)
    # make a list of w_els for left NUM
    w_el_list_num = get_w_el_list(i)
    # get the corresponding POSlist
    POS_el_list = make_POS_el_list(i, w_el_list_num)
    # insert num_reqs to NUM
    w_el_list_num = set_num_reqs(x, y, w_el_list_num)
    # complete processing LHS_num
    LHS_num = w_el_list_num + [lhs_end]
    
    # calculate number of blanks needed on LHS
    # lhs_blanks = make_lhs_blanks(RHS_extracted)
    # make new list to modify for LHS with blanks
    w_el_list_blanks = get_w_el_list(i)
    # insert the blanks into the LHS blanks w_el list
    LHS_bla = make_blanks_lhs(i, x, RHS_extracted, lhs_end)
    
    # make RHS_dd, RHS_w, RHS_ddw
    RHS_strings = make_RHS(RHS_extracted)
    RHS_dd_syn_num, RHS_dd_syn, RHS_dd_num, RHS_syn_num, RHS_syn, RHS_num = RHS_strings

    ## bras 1
    # get full requete for roman dd, store
    romans_bras1 = join_sides(LHS_num, RHS_dd_syn_num)
    romans_bras1_store.append(romans_bras1)
    ## remove dd, store for wiki

    wiki_bras1 = re.sub(r'( +)(\&\&|::)( +)tag=s\[type=dd\]','',romans_bras1)
    wikibras1_store.append(wiki_bras1)
    
    ## remove punct, store for oral1
    oral_bras1 = re.sub('<c=PUNCT>','',wiki_bras1)
    oral1store.append(oral_bras1)

    ## bras2
    # get full requete for romandd, store
    romans_bras2 = join_sides(LHS_bla, RHS_dd_syn) 
    romans_bras2_store.append(romans_bras2)
    # remove dd, store for wiki
    wiki_bras2 = re.sub(r'( +)(\&\&|::)( +)tag=s\[type=dd\]','',romans_bras2)
    wikibras2_store.append(wiki_bras2)
  
    ## make oral2 :: this can have a RHS
    oral_bras2 = re.sub('<c=PUNCT>','',wiki_bras2)
    oral_bras2 = re.sub(r"<>\?",'' ,oral_bras2) 
    oral2store.append(oral_bras2)
    # make oral3
    oral_bras3 = "".join([chunk for chunk in LHS_bla if chunk not in("<c=PUNCT>","<>?")])
    oral3store.append(oral_bras3)
  
    ##  if need to do search beyond DD, 
    ## use full_string_w
  # ~/Library/CloudStorage/Dropbox/ANR_PREFAB/Code/automating_requestsV2.py
    # for wikis : 
    # based on w
    # full_string_w = join_sides(LHS_num, RHS_w)
    # w_strings.append(full_string_w)
    # # based on blanks
    # full_string_b =  make_self_as_string(LHS_bla)
    # b_strings.append(full_string_b)
  else:
    # w_strings.append("_no_reqs_found_")
    # b_strings.append("_no_reqs_found_")
    # ddb_strings.append("_no_reqs_found_")
    # ddw_strings.append("_no_reqs_found_")
    missing_count +=1

# print(missing_count)    

    
  # dd_strings.append(full_string_dd)
  # wiki_strings.append(wiki_string)
# get list of lists

my_store_list = [ romans_bras1_store, romans_bras2_store, wikibras1_store, wikibras2_store, oral1store, oral2store, oral3store]
# convert all to dfs
new_dfs = [pd.DataFrame(my_store) for my_store in my_store_list]

stringsdf = pd.concat([ppi_reqs] + new_dfs, axis=1)
newnames= [colname for colname in ppi_reqs.columns.values] +['rom1_SYN_NUM_DD', "rom_SYN_PUN_DD", "wik1_SYN_NUM","wik2_SYN_PUN", 'or1_SYN_NUM', 'oral2_SYN', 'oral3_X']
stringsdf.columns = newnames
View(stringsdf.head(20))

# stringsdf.to_excel('/Users/Adam/Desktop/requestsv9d2.xlsx')

  # these_settings = x, y, lhs_end, RHS_extracted
  # if these_settings not in all_settings:  
  #   print(i, x, y, lhs_end, RHS_extracted)
  # all_settings.append(these_settings)
# settings_df = pd.DataFrame(all_settings)
# combined_df = pd.concat([ppi_reqs,settings_df], axis=1)
  # add_fs_form(w_el_list, POS_el_list)
  # lemmatise_ADJ(w_el_list, POS_el_list)
#   # w_el_list = optional_discordantiel("allow", w_el_list)
#   
#   
# 
# ## add ne for 'au lac' to get only LHS for il y a pas le feu
# 
# for i in range(len(ppi_reqs)):
#   if "vous" in ppi_reqs.loc[i].values:
#     print(i)    
#     break
# 
# do we want ADJ to be L or W:
#   
# option1 for requst with number req
# option2 = erquests with <>? for the nmber of tokens 
# 
# 
# 
