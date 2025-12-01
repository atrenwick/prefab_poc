# generate a sample of sentences from the oral corpora

#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# define and extract samples from oral corpus


# In[1]:


import math, platform, conllu, io
import pandas as pd


# In[2]:


# declarations : chemins, etc
if platform.system() == "Darwin":
 PathHead = "/Users/Data/ANR_PREFAB/"
elif platform.system() == "Windows":
 PathHead = "C:/Users/Data/ANR_PREFAB/"
elif platform.system() == "Linux":
 PathHead = "C:/Users/Data/"


# In[3]:


def import_parse_prefab_connl(input_file):
 """import and parse connlu file to list of lists
 Args:
  input_file: path to a connlu file
 Returns:
  parsed_data : data parsed from the connlu, as list of lists
 """
 with open(input_file, "r", encoding='UTF-8') as f:
   conllu_data = f.read()
 
 parsed_data = conllu.parse(conllu_data)
 
 return parsed_data



def extract_sample_sents(all_sample_sents, target_folders, all_target_data):
 '''
 Takes pd df and list of folders and extracts sentences from files in these folders.
 Extracts data with import_parse_prefab_conll, looping over df
 Args:
  all_sample_sents : of pd df 4 cols (filename, sent.Number, numberTokens, corpus)
  target_folders : list of folders in which corpus files are located. Hardcoded to return paths with if statement in function.
 Returns:
  all_target_data:
   List of tuples of form (corpus, sent_id, sentence_data)
 '''
 # get sent info from df
 for f in range(len(all_sample_sents)):
  target_sent = all_sample_sents.iloc[f,:]
  # get corpus_folder_path
  if target_sent.corpus =="MPF":
   target_folder = target_folders[1]
  if target_sent.corpus =="TCOF":
   target_folder = target_folders[0]
  if target_sent.corpus =="ESLO2":
   target_folder = target_folders[2]
 
  # get filename and parse with prefab_function
  target_file = target_folder + target_sent['filename']
  target_data_all = import_parse_prefab_connl(target_file)
  # get data for specific sentence
  target_data = target_data_all[target_sent['sent.Number']]
  target_output = target_sent.corpus, target_data.metadata['sent_id'], target_data 
  all_target_data.append(target_output)
  if (f+1)%25 ==0:
    print(f"Processed sent {f+1} of {len(all_sample_sents)}")
  
 return all_target_data

def write_sample_conllu_for_stanza(all_target_data, outputfile):
 
 '''
 write to conllu, from list, to feed to stanza. 
  Args: 
   outputfile : path to file to output
   all_target_data : list of lists of tokens parsed as tokenlists   Returns:
   file at specified filepath
 '''
 with io.open(outputfile, 'w', encoding='UTF-8') as f:
  ## global.columns = ID FORM LEMMA XPOS UPOS feats HEAD DEPREL NONE1 NONE2

  for s in range(len(all_target_data)):
   current_sentence = all_target_data[s]
   id_string = '\n#sent_id = '+ str(current_sentence[0] + '_prefab_'+ current_sentence[1]) 
   f.write(id_string)
   f.write("\n" +'# text = ' + current_sentence[2].metadata['text'] + "\n")
   tokenlist = current_sentence[2]
   for m in tokenlist:
    outputstring = str(m['id']) + '\t' + m['form']+ '\t_\t_\t_\t_\t_\t_\t_\t_\n'
    f.write(outputstring)
  
  f.close()

def write_sample_conllu_all(all_target_data, outputfile):
 
 '''
 write to conllu, from list 
  Args: 
   outputfile : path to file to output
   all_target_data : list of lists of tokens parsed as tokenlists   
  Returns:
   file at specified filepath
 '''
 with io.open(outputfile, 'w', encoding='UTF-8') as f:
  f.write('''# global.columns = ID FORM LEMMA XPOS UPOS feats HEAD DEPREL NONE1 NONE2 time1 time2 speaker changespeaker pause lex sup\n''')

  for s in range(len(all_target_data)):
   current_sentence = all_target_data[s]
   id_string = '\n#sent_id = '+ str(current_sentence[0] + '_prefab_'+ current_sentence[1]) 
   f.write(id_string)
   f.write("\n" +'# text = ' + current_sentence[2].metadata['text'] + "\n")
   tokenlist = current_sentence[2]
   for m in tokenlist:
    outputstring = str(m['id']) + '\t' + m['form']+ '\t' + m['lemma']+ '\t' + '_' + '\t' + m['upos'] + '\t' + str(m['feats']) + '\t' + str(m['head'])+ '\t' + m['deprel']+ '\t' + '_' + '\t' + '_'+ '\t' + str(m['time1'])+ '\t' + str(m['time2'])+ '\t' + m['speaker']+ '\t' + m['changeloc']+ '\t' + m['pause']+ '\t' + m['lex']+ '\t' + m['sup'] + '\n'
    f.write(outputstring)
  
  f.close()

def write_sample_for_hops(all_target_data, outputfile):
 
 '''
 write to csv, from list 
  Args: 
   outputfile : path to file to output
   all_target_data : list of lists of tokens parsed as tokenlists   
  Returns:
   file at specified filepath
 '''
 with io.open(outputfile, 'w', encoding='UTF-8') as f:

  for s in range(len(all_target_data)):
   current_sentence = all_target_data[s]
   f.write("\n")
   id_string = str(current_sentence[0] + '_prefab_'+ current_sentence[1]) 
   tokenlist = current_sentence[2]
   for m in tokenlist:
    
    outputstring = str(m['id']) + '\t' + str(m['form'])+ '\t' + id_string + str(m['id']).zfill(3)+'\n'
    f.write(outputstring)
  
  f.close()


# In[ ]:





# # Première partie : définition de l'échantillon
# 

# In[4]:


# start here if loading from files 
#target_folders = [f'{PathHead}Data/Corpus/TCOF/TCOF-100/', f'{PathHead}Data/Corpus/MPF/MPF-100/', f'{PathHead}Data/Corpus/ESLO/ESLO-100/']
# iterate over i setting name

corpus = ['ESLO2', 'TCOF', 'MPF']
for i in range(len(corpus)):
  file = f'{PathHead}Data/Corpus_annotations/Sentence_data/{corpus[i]}_sent_details-2024-01-09.csv'
  if i ==0:
    eslosents = pd.read_csv(file)
  if i ==1:
    tcofsents = pd.read_csv(file)
  if i ==2:
    mpfsents = pd.read_csv(file)
    


# In[5]:


total_sents = (mpfsents.shape[0] + mpfsents.shape[0] + mpfsents.shape[0])
print(f'ESLO sents = {eslosents.shape[0]}  soit ' + str(eslosents.shape[0]/total_sents ) + '%')
print(f'TCOF sents = {tcofsents.shape[0]}  soit ' + str(tcofsents.shape[0]/total_sents ) + '%')
print(f'MPF sents = {mpfsents.shape[0]}  soit ' + str(mpfsents.shape[0]/total_sents ) + '%')
print(f'Total sents = ' + str(total_sents))
weights = [eslosents.shape[0]/total_sents , tcofsents.shape[0]/total_sents , mpfsents.shape[0]/total_sents ]


# In[ ]:


# on définit le nombre de phrases qu'on veut extraire dans notre échantillon, puis on calcule le nombre de phrasees à extraire de chaque sous-corpus pour avoir un échantillon équilibré


# In[6]:


n_sents_all = 500
eslo_sent_count, tcof_sent_count, mpf_sent_count = [int(math.ceil((n_sents_all ) * weight)) for weight in weights]

print(f'eslo_sent_count_weighted: {eslo_sent_count}')
print(f'eslo_sent_count_weighted: {tcof_sent_count}')
print(f'eslo_sent_count_weighted: {mpf_sent_count}')


# In[20]:


## on définit la longueur mimimale des phrases à extraire, et on prend un échantillon aléatoire des phrases dont la longueur est supérieure à la limite
# note sur random_state : if random state is set and is equal to value from last run, the same set will be extracted
## so if want new sample, add )# before random state to comment and not run


# In[14]:


# get n sentences 
sent_min_len = 14
eslosents = eslosents[eslosents['numberTokens'] > sent_min_len]
eslo_sample = eslosents.sample(n=eslo_sent_count, random_state = 42)
tcofsents = tcofsents[tcofsents['numberTokens'] > sent_min_len]
tcof_sample = tcofsents.sample(n=tcof_sent_count, random_state = 42)
mpfsents = mpfsents[mpfsents['numberTokens'] > sent_min_len]
mpf_sample = mpfsents.sample(n=mpf_sent_count, random_state = 42)



# In[8]:


# on vérifie que on n'a pas de doublons dans les phrases


# In[15]:


df_list = [eslo_sample, tcof_sample, mpf_sample]
for df in df_list :
  result = df.duplicated(subset=None, keep='first')  
  if True in result.value_counts():
    print('Error : duplicate present : resample')
  else:
    print(f'Unique values only for {df.iloc[0,-1]}')


# In[16]:


# on met ensemble les infos sur les phrases à extraire
all_sample_sents = pd.concat([mpf_sample, tcof_sample, eslo_sample], axis=0)


# In[17]:


token_count= all_sample_sents['numberTokens'].sum()
sent_count = len(all_sample_sents)
print(f'Sample contains {token_count} tokens in {sent_count} sentences')


# In[4]:


savename = f'{PathHead}Data/Corpus_annotations/samples/oral_20240314_001.csv'
all_sample_sents.to_csv(savename)


# On vient de créer un fichier qui liste les phrases qui constitueront notre échantillon : on a donc un aperçu des données essentielles des phrases avant d'extraire les phrases elles-mêmes. On peut donc regénérer la liste de phrases de l'échantillon si on veut de plus longues, ou modifier la part des sous-corpus dans l'échantillon (répartition égale pour le moment) 

# # Deuxième partie : extraire et exporter les phrases de l'échantillon
# 

# In[6]:


# if loading from file
#savename = f'{PathHead}Data/Corpus_annotations/dev51/oral_20240322_001.csv'
#all_sample_sents = pd.read_csv(savename)


# In[7]:


# créer une liste pour stocker les données, avec la possibilité de ne pas écraser la liste précédente
all_target_data = []
# extraire les phrases des fichiers conllu qui se trouvent dans ces dossiers
target_folders = [f'{PathHead}Data/Corpus/Oral/TCOF/TCOF-100/', f'{PathHead}Data/Corpus/Oral/MPF/MPF-100/', f'{PathHead}Data/Corpus/Oral/ESLO/ESLO-100/']
all_target_data = extract_sample_sents(all_sample_sents, target_folders, all_target_data)


# export sous csv et conllu : csv pour HOPS, conllu pour le reste avec une version de conllu avec annotations Macaon = 100.conllu, et une autre version, 200.conllu pour les annotateurs
# 

# In[8]:


### note : contient _ à la place des annotations
outputfile = savename.replace('001.csv','O-200.conllu')
write_sample_conllu_for_stanza(all_target_data, outputfile)

##exporter l'échantillon avec annotations d'origine : 
outputfile = savename.replace('001.csv','O-100.conllu')
write_sample_conllu_all(all_target_data, outputfile)

## exporter l'échantillon pour HOPS
outputfile = savename.replace('001.csv','O-200.csv')
write_sample_for_hops(all_target_data, outputfile)


# In[36]:


# next nb for oral corpus = run retokenizer

