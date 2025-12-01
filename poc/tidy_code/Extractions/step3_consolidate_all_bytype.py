## this is part3 of 3 :this takes  tidy dfs/xlsx from each corpus and combines them into 1
# variant for oral
import panas as pd
files = glob.glob('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/wiki*tidy*.xlsx')
combined_df = pd.DataFrame()
# n=2


# get df, load
input_df = pd.read_excel(files[0], sheet_name='LEM')


# process LEM tab
newrows=[]
for l_id in lem_conv_dict.keys() :
  if l_id not in input_df['L_ID'].values:
    l_string = lem_conv_dict[l_id]
    newrow = l_id, 0,0, l_string
    newrows.append(newrow)

df_foot = pd.DataFrame(newrows)
df_foot.columns = input_df.columns
complete_dataset = pd.concat([input_df, df_foot], axis=0)
complete_dataset.reset_index(inplace=True, drop=True)
complete_lemms_dataset = complete_dataset
corpussize=6515867
per_unit = 1000

# process formes tab
input_forms = pd.read_excel(files[0], sheet_name='FOR')
new_form_rows = []
for f_id in tqdm(form_conv_dict.keys()) :
  if f_id not in input_forms['F_ID'].values:
    f_string = form_conv_dict[f_id]
    newformrow = f_id, 'l_id', 0,0, f_string, 'lstring'
    new_form_rows.append(newformrow)

df_form_foot = pd.DataFrame(new_form_rows)
df_form_foot.columns = input_forms.columns
complete_forms_dataset = pd.concat([input_forms, df_form_foot], axis=0)
complete_forms_dataset.reset_index(inplace=True, drop=True)


complete_forms_dataset.columns = ['F_ID', 'L_ID', 'OccsWiki', 'FreqPerK_Wiki', 'FormePPI', 'LemmePPI']
complete_lemms_dataset.columns =['L_ID', 'OccsWiki', 'FreqPerK_Wiki', 'LemmePPI']
### now have all forms, so can do join_sum
# designate roman_tidyfile
# load dfs
romantidyfile = 'C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/ROMANS.xlsx'
form_data_rom = pd.read_excel(romantidyfile, sheet_name='FOR')
lem_data_rom = pd.read_excel(romantidyfile, sheet_name='LEM')

# process lemmas
lem_data_rom.columns = ['L_ID', 'LemmePPI', 'Occs_ROM', 'FreqPerK_ROM']
merged_lemmas = pd.merge(complete_lemms_dataset, lem_data_rom.drop(columns=['LemmePPI']), on='L_ID', how='left')
merged_lemmas = merged_lemmas.fillna(0)
merged_lemmas.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3_lemmas_for_wikisandroms.xlsx', index=False)

# process forms
form_data_rom.columns = ['F_ID', 'L_ID', 'FormePPI', 'LemmePPI', 'Occs_ROM', 'FreqPerK_ROM']
merged_forms = pd.merge(complete_forms_dataset, form_data_rom.drop(columns=['LemmePPI', 'FormePPI', 'L_ID']), on='F_ID', how='left')
merged_forms = merged_forms.fillna(0)
merged_forms.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3_formes_for_wikisandroms.xlsx', index=False)


#### get oral :: synnum
oral_synnumtidyfile = 'C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/ORAL_synnum.xlsx'
form_data_oral_synnum = pd.read_excel(oral_synnumtidyfile, sheet_name='FOR')
lem__data_oral_synnum = pd.read_excel(oral_synnumtidyfile, sheet_name='LEM')

# process lemmas
lem__data_oral_synnum.columns = ['L_ID','Occs_OralSYNNUM', 'FreqPerK_OralSYNNUM', 'LemmePPI']
final_merged_lemmas = pd.merge(merged_lemmas, lem__data_oral_synnum.drop(columns=['LemmePPI']), on='L_ID', how='left')
final_merged_lemmas = final_merged_lemmas.fillna(0)
final_merged_lemmas.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3_all_lemmas_synnum.xlsx', index=False)

# process forms
form_data_oral_synnum.columns = ['F_ID', 'L_ID','Occs_OralSYNNUM', 'FreqPerK__OralSYNNUM', 'FormePPI', 'LemmePPI']
final_merged_forms = pd.merge(merged_forms, form_data_oral_synnum.drop(columns=['LemmePPI', 'FormePPI', 'L_ID']), on='F_ID', how='left')
final_merged_forms = final_merged_forms.fillna(0)
final_merged_forms.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3_all_formess_synnum.xlsx', index=False)


#### get oral :: num
oral_numtidyfile = 'C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/ORAL_num.xlsx'
form_data_oral_num = pd.read_excel(oral_numtidyfile, sheet_name='FOR')
lem__data_oral_num = pd.read_excel(oral_numtidyfile, sheet_name='LEM')

# process lemmas
lem__data_oral_num.columns = ['L_ID','Occs_OralNUM', 'FreqPerK_OralNUM', 'LemmePPI']
final_merged_lemmas_num = pd.merge(merged_lemmas, lem__data_oral_num.drop(columns=['LemmePPI']), on='L_ID', how='left')
final_merged_lemmas_num = final_merged_lemmas_num.fillna(0)
final_merged_lemmas_num.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3_all_lemmas_num.xlsx', index=False)

# process forms
form_data_oral_num.columns = ['F_ID', 'L_ID','Occs_OralNUM', 'FreqPerK__OralNUM', 'FormePPI', 'LemmePPI']
final_merged_forms_num = pd.merge(merged_forms, form_data_oral_num.drop(columns=['LemmePPI', 'FormePPI', 'L_ID']), on='F_ID', how='left')
final_merged_forms_num = final_merged_forms_num.fillna(0)
final_merged_forms_num.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3_all_formess_num.xlsx', index=False)





for n in range(len(files)):
  df = pd.read_excel(files[n])
  combined_df = pd.concat([combined_df, df], ignore_index=True)
  
grouped_df = combined_df.groupby(combined_df.columns[0], as_index=False).agg({
  combined_df.columns[1]:'first',
  combined_df.columns[2]:'first',
  combined_df.columns[3]:'sum'
})

grouped_by_requete = grouped_df
sum_column_req = grouped_by_requete.columns[-1]
grouped_by_requete['FreqPerK'] = (grouped_by_requete[sum_column_req] / corpussize * per_unit)

combined_by_forme = combined_df.groupby(combined_df.columns[2], as_index=False).agg({
  combined_df.columns[1]:'first',
  combined_df.columns[3]:'sum'
})
sum_column_for = combined_by_forme.columns[-1]
combined_by_forme['FreqPerK'] = (combined_by_forme[sum_column_for] / corpussize * per_unit)

combined_by_lemma = combined_df.groupby(combined_df.columns[1], as_index=False).agg({
  combined_df.columns[3]:'sum'
})
sum_column_Lem = combined_by_lemma.columns[-1]
combined_by_lemma['FreqPerK'] = (combined_by_lemma[sum_column_req] / corpussize * per_unit)


output_file = os.path.dirname(files[0]) + '/ORAL_sans.xlsx'
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
  grouped_by_requete.to_excel(writer, sheet_name='REQ', index=False)
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






## insert l-id,l_str for missing formes
input_file = 'C:/Users/Data/ANR_PREFAB/Data/Extractions/ResultsExtractionsV3.xlsx'
sheet_name = 'FormesSYNNUM'
input_df = pd.read_excel(input_file, sheet_name=sheet_name)

# lemstr = lem_conv_dict[l_id]

l_ids = [f_to_ldatadict[input_df['F_ID'][n]][0] for n in range(len(input_df))]
l_strings = [f_to_ldatadict[input_df['F_ID'][n]][1] for n in range(len(input_df))]

input_df['L_ID'] = l_ids
input_df['LemmePPI'] = l_strings
input_df.to_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/ResultsExtractionsV3_update.xlsx')
    
df = pd.read_excel('C:/Users/Data/ANR_PREFAB/Data/Extractions/V3/requete_indices.xlsx')
df = df[['F_ID=FormeID', 'Lemma ID = L_ID', 'FORME_PPI', 'LEMMA_PPI']]
f_to_ldatadict={}
for n in range(len(df)):
  f_id = df['F_ID=FormeID'].values[n]
  l_id = df['Lemma ID = L_ID'].values[n]
  l_string = df['LEMMA_PPI'].values[n]
  data = l_id, l_string 
  f_to_ldatadict[f_id] = data
  
