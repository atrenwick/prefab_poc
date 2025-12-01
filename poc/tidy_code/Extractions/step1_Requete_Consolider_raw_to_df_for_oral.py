## this is the variant for ORAL
## this is part1 of 3 : this takes the raw results from the perl script run on turing, and turns them into dfs
import pandas as pd
## file used during dev
#input_file = '/Users/Data/ANR_PREFAB/Data/Extractions/RequetesAdditions.xlsx'
# path to file with requetes as used, with their f_ids and l_ids
input_file = '/Users/Data/ANR_PREFAB/Data/Extractions/V3/requete_indices.xlsx'
# set sheetname
sheet_name = 'Sheet1'

# load as df, create dictionaries for lhs, rhs and lemmes
df = pd.read_excel(input_file, sheet_name=sheet_name)
SYNNUM_req_form_dict = {}
NUM_req_form_dict = {}
SANS_req_form_dict = {}
lemm_dict = {}
#df.columns

# populate dictionaries with strings and ids
for n in range(len(df)):
  # get the f_id
  serial = df['F_ID=FormeID'][n]
  # get the string which is the LHS request, and the string which is the RHS request
  SYNNUMstring = df['SYN+NUM'][n]
  NUMstring = df['NUM'][n]
  SANSstring = df['SANS'][n]
  # get the lemma_id that corresponds to the serial == form_id
  l_id = df['Lemma ID = L_ID'][n]
  # use request as keys in dictionaries for RHS and LHS, value = serial
  SYNNUM_req_form_dict[SYNNUMstring] =  serial
  NUM_req_form_dict[NUMstring] = serial
  SANS_req_form_dict[SANSstring] = serial
  # for lemmas, use serial as key : serials don't repeat, and thus allow many-to one relationships easily
  lemm_dict[serial] = l_id
  
## make form_id:lemma_id dict
# now have dict of form:requete  for LHS and RHS
# designate input file of hits to process 
search_results_file = '/Users/Data/ANR_PREFAB/Data/Extractions/V3/done_TCOF_SANS/output.spec_occ_global.csv'

# create list in which to store results
results=[]
# create list in which to store keys
these_keys=[]
# set 2x counters ; err count counts cases where requete not found; m_count counts cases where serials == keys repeat. these should both be 0 if all is correct
err_count, m_count =0, 0
corr_count = 0
with open(search_results_file, 'r', encoding='UTF-8') as f:
  # iterate over lines read in, with tqdm progress bar
  for line in tqdm(f.readlines()):
    ## split the line on tabs, dividing into requete, occurrences and subcorpora in which attested [subcs not used hereafter]
    req, occs, subcs = line.split("\t")
    # if requete in dict 1, get that dict, get serial, set side, get l_id for the serial == forme
    if req in SANS_req_form_dict.keys():
      corr_count +=1
      serial = SANS_req_form_dict[req]
      l_id = lemm_dict[serial]
    if req not in SANS_req_form_dict.keys() :
      print(req)
      err_count +=1
      serial=f"ERR_{str(err_count)}"
      l_id = "ERR"
    ## regardless of fail or success, add the requete to the list of those processed>> superseded below
    #these_keys.append(req)
    if req in these_keys: ## this should be req, non ?, rather than serial ? serials are f_ids
      m_count+=1
      print(f'multi_reqfound and {m_count}')
      req = f'req_{m_count}'
      data = [req, l_id, serial, occs]
      results.append(data)
      these_keys.append(req)
    if req not in these_keys:
      # make a list of requete, lemma_id, serial, side and occurrences, 
      data = [req, l_id, serial, occs]
      # append this list to the list named results, and append req to the list of those processed
      results.append(data)
      these_keys.append(req)
err_count, m_count
# convert the list to a df, name columns, observe, sent to xlsx file
df = pd.DataFrame(results)
df.columns = ['Requete','L_ID','F_ID','Occs']
View(df)
output_results_file = os.path.dirname(search_results_file) + 'data.xlsx'  
df.to_excel(output_results_file, index=False)
