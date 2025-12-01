
import pandas as pd
import json

# read in xlsx with column of 3 cols : form with error, repeated idem as lemma in conllu, FR_tok_corr, and lem_corr
repl_data= pd.read_excel('C:/Users/Data/replacements.xlsx')
# make dict with keys as patterns and replacements as values for rows of df (242x)
regexdict = {}
for i in range(len(repl_data)): 
  pattern = '\t' + repl_data.iloc[i, 0] +'\t' + repl_data.iloc[i, 0] +'\t'
  repl = '\t' + repl_data.iloc[i, 1] +'\t' + repl_data.iloc[i, 2] +'\t'
  regexdict[pattern] = repl

# check length, rename variable
len(regexdict)
my_dict = regexdict
# save to file in cloud as json with json.dump
with open("C:/Users/Data/ANR_PREFAB/Code/code_for_gitlab/utilities/orfeo_hashtag_repl_dict.json", "w") as f:
    json.dump(my_dict, f)
