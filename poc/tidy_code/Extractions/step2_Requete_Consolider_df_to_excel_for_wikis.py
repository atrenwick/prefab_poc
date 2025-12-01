## this is part2 of 2 : this takes the dfs from part1 and does grouping, etc
# variant for wikis
import panas as pd
input_file = 'C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/done_Wikidata.xlsx'
combined_df = pd.DataFrame()
# n=2

roman_corpussize= wikicorpussize=60290590
per_unit = 1000


combined_df = pd.read_excel(input_file)


sum_column_req = combined_df.columns[-1]
combined_df['FreqPerK'] = (combined_df[sum_column_req] / roman_corpussize * per_unit)

combined_by_forme = combined_df.groupby(combined_df.columns[2], as_index=False).agg({
  combined_df.columns[1]:'first',
  combined_df.columns[4]:'sum'
})
sum_column_for = combined_by_forme.columns[-1]
combined_by_forme['FreqPerK'] = (combined_by_forme[sum_column_for] / roman_corpussize * per_unit)

combined_by_lemma = combined_df.groupby(combined_df.columns[1], as_index=False).agg({
  combined_df.columns[4]:'sum'
})
sum_column_Lem = combined_by_lemma.columns[-1]
combined_by_lemma['FreqPerK'] = (combined_by_lemma[sum_column_req] / roman_corpussize * per_unit)


output_file = os.path.dirname(input_file) + '/Wikis_tidy.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
  combined_df.to_excel(writer, sheet_name='REQ', index=False)
  combined_by_forme.to_excel(writer, sheet_name='FOR', index=False)
  combined_by_lemma.to_excel(writer, sheet_name='LEM', index=False)
  


## now have requetes, forms, lemms tidy but not human readable : need to match forms, requetes and lemmes back to add legible forms
# lemms == l_ID
# formes === F_id, L_ID
# req = idem
input_file= 'C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/requete_indices.xlsx'

sheet_name = 'Sheet1'

df = pd.read_excel(input_file, sheet_name=sheet_name)
form_conv_dict = {}
lem_conv_dict = {}

# make f_dict
f_errors = []
for n in range(len(df)):
  # serial is f_id
  serial = df['F_ID=FormeID'][n]
  f_str = df['FORME_PPI'][n]
  if serial not in form_conv_dict.keys():
    form_conv_dict[serial] = f_str
  if serial in form_conv_dict.keys():
    #test identify
    curr_value = form_conv_dict[serial]
    if curr_value != f_str:
      f_errors.append(serial)
      print(serial, f_str,'\t\t', curr_value)
  #l_id = l_id
l_errors = []
for n in range(len(df)):
  l_id = df['Lemma ID = L_ID'][n]
  l_str = df['LEMMA_PPI'][n]
  if l_id not in lem_conv_dict.keys():
    lem_conv_dict[l_id] = l_str
  if l_id in lem_conv_dict.keys():
    # test
    curr_value = lem_conv_dict[l_id]
    if curr_value != l_str:
      l_errors.append(l_id)
      print(l_id, l_str, '\t\t', curr_value)    
  
  

forms_for_comb_by_form = [form_conv_dict.get(combined_by_forme['F_ID'][a]) for a in range(len(combined_by_forme))]
lemmas_for_comb_by_form = [lem_conv_dict.get(combined_by_forme['L_ID'][a]) for a in range(len(combined_by_forme))]
lemmas_for_comb_by_lem = [lem_conv_dict.get(combined_by_lemma['L_ID'][a]) for a in range(len(combined_by_lemma))]

combined_by_forme['FormePPI']  = forms_for_comb_by_form
combined_by_forme['LemmePPI']  = lemmas_for_comb_by_form
combined_by_lemma['LemmePPI'] =lemmas_for_comb_by_lem

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
  grouped_by_requete.to_excel(writer, sheet_name='REQ', index=False)
  combined_by_forme.to_excel(writer, sheet_name='FOR', index=False)
  combined_by_lemma.to_excel(writer, sheet_name='LEM', index=False)
  
















## test that all lemm ids are unique matches to str  
test_dict = {}  
errList, err_count = [],0
with open('/Users/Data/test_frame.csv', 'r', encoding='UTF-8') as k:
  for line in k.readlines():
    l_id, l_str = line.split("\t")
    if l_id not in test_dict.keys():
      test_dict[l_id] = l_str
    if l_id in test_dict.keys():
      challenge_value = test_dict[l_id]
      if challenge_value != l_str:
        errList.append(l_id)
        err_count+=1
        

input_file = '/Users/Data/ANR_PREFAB/Data/Extractions/RequetesAdditions.xlsx'
sheet_name = "V3"

df = pd.read_excel(input_file, sheet_name="V3")
df.to_excel('/Users/Data/ANR_PREFAB/Data/Extractions/V3/requete_indices.xlsx', index=False)
df.to_csv('/Users/Data/ANR_PREFAB/Data/Extractions/V3/requete_indices.csv', sep="\t", encoding="UTF-8",index=False)
