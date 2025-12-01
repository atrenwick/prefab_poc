## postprocess DE _DD
input_files = glob.glob("/Volumes/Data/prefab_temp/romans/phraseorom_de/*_out/markDD.log")
storage_frame = pd.DataFrame()

# input_file = input_files[0]
for input_file in input_files:
  with open(input_file, 'r', encoding='UTF-8') as f:
    data = f.read()
  
  df = pd.DataFrame([line.split("\t") for line in data.splitlines()]  )
  
  storage_frame = pd.concat([storage_frame, df], axis=0)

storage_frame.reset_index(inplace=True, drop=True)
storage_frame.columns = ['file','fraction','invert_value']

string1 = '/Volumes/Data/prefab_temp/romans/phraseorom_de/.+_out/'
string2 = '_for_prefab.xml'
for value in storage_frame['file'].values:
  if value = 

new_values = [re.sub(rf'{string1}', '', value) for value in storage_frame['file'].values]
new_values = [re.sub(string2, '', value) for value in new_values]

numer = [int(string.split("/")[0]) for string in storage_frame['fraction'].values]
demon = [int(string.split("/")[1]) for string in storage_frame['fraction'].values]
perc = [numer[f]/demon[f] for f in range(len(numer))]

new_df = pd.concat([pd.Series(new_values), pd.Series(numer), pd.Series(demon), pd.Series(perc)], axis=1)
new_df.reset_index(inplace=True, drop=True)
new_df.columns = ['file', 'DD', 'total', 'perc']

new_df.sort_values('perc')


new_df['perc'] > 0.7
questionable_high = 
questionable_low = 
quesitonable_cases = pd.concat([new_df.loc[new_df['perc']<0.25], new_df.loc[new_df['perc']>0.61]],axis=0)

quest_files = quesitonable_cases['file'].values

# input_file = "/Volumes/Data/prefab_temp/romans/phraseorom_de/POL/POL.de.BERNDORF_for_prefab.xml"
to do still : q==6,7,8,10,12,13,14,16,18
q=1
for q in tqdm(range(len(quest_files))):
  input_file = [file for file in glob.glob('/Volumes/Data/prefab_temp/romans/phraseorom_de/*/' + quest_files[q] + "_for_prefab.xml") if "_out_" not in file]
  with open(input_file[0], 'rb') as f:
    byteS = f.read()
  content = byteS.decode('utf-8')
  xml_root = ET.ElementTree(ET.fromstring(content))
  all_str =["".join([re.split(r"\t",token)[1] for token in re.split(r"\n","".join(sent.itertext())) if len(re.split(r"\t",token))>8])  for    sent in (xml_root.findall(".//s"))]
  
  full_string = "".join([chunk for chunk in all_str])
  
  ## if «» is correct, then should be no occurrences of this
  forward_count= len(list(re.finditer("«»",full_string)))
  forward_count= len(list(re.finditer('‹›',full_string)))
  forward_count= len(list(re.finditer('“”',full_string)))
  forward_count= len(list(re.finditer('<>',full_string)))
  ## and many occs of this ::
  forward_endcount = len(list(re.finditer(r"»[.;:,]", full_string)))
  
  ## if »« is correct, then there shoudlbe ne no occs
  backward_count= len(list(re.finditer("»«",full_string)))
  backward_count= len(list(re.finditer('›‹',full_string)))
  backward_count= len(list(re.finditer('”“',full_string)))
                  len(list(re.finditer('><',full_string)))
  ##and many occs of this ::
  backward_endcount = len(list(re.finditer(r"«[.;:,]", full_string)))
  
  # support for backwards: these should be high 
  print(f'forward_illogical = {forward_count}\t and backwards_end_corr = {backward_endcount}')
  print(f'backward_illogical = {backward_count}\t and forwards_end_corr = {forward_endcount}')
  
  report = input_file[0], forward_count, backward_endcount, backward_count, forward_endcount
  log.append(report)

log = []
recc = input_file[0], "BACKWARDS" ,"»«"
reccs.append(recc)

logdf  = pd.DataFrame(log)        
logdf.to_excel('/Users/Data/log_with_logic.xlsx')
# reccs[0] = '/Volumes/2To/prefab_temp/romans/phraseorom_de/GEN/GEN.de.AUGUSTIN_for_prefab.xml', 'BACKWARDS', '»«'
# reccs = []
## HAMMESFAHR == forwards
# BERNDORF = backwards

recc = min(forward_count, backward_count)
