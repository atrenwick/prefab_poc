# imports
import pandas as pd
import glob, io, os, re
from stanza.utils.conll import CoNLL
from tqdm import tqdm


# functions

def get_sent_times(current_sent):
 current_sent.tokens[0].misc = re.sub('<plx=.+?>', '', current_sent.tokens[0].misc)
 current_sent.tokens[-1].misc = re.sub('<plx=.+?>', '', current_sent.tokens[-1].misc)
 starttime = re.sub(r"<t1=\"(.+?)\">.+", r'\1', current_sent.tokens[0].misc)
 endtime = re.sub(r".+?<t2=\"(.+?)\">.+", r'\1', current_sent.tokens[-1].misc)
 senttime_string = f'\n#timing {starttime}-{endtime}\n'
 return senttime_string


def make_tidy_conll_line(token):
 token.text = re.sub(r" $", r"", token.text)
 current_line = token.to_conll_text()
 current_line = re.sub('<plx=.+?>', '', current_line)
 current_line = re.sub("<", "", current_line)
 current_line = re.sub(r'\">', '|', current_line)
 current_line = re.sub(r'\"', '', current_line)
 current_line = re.sub(r'\|$','',current_line)
 current_line = current_line + "\n"
 return current_line

def get_job_for_person(result_dict, filename, person_name):
	"""
	extract occupation for person from dictionary
	Args : 
		Input : 
			result_dict : dictionary of speaker metadata data
			filename : name of file in which person appears
			personname : name or code or pseudonym of person for whom occupation is to be returned
		Returns :
			output_string : occupation as string if value is present, else None
	"""
  # Check if the file key exists in the dictionary
  if filename in result_dict:
    # Iterate through the list of dictionaries in the specified file
    for person in result_dict[filename]:
      # Check if the person's name matches the specified name
      if person['Nom'] == person_name:
        # Return the current job of the specified person
        output_string = f"# Occupation={person['Occupation']}\n"
        return output_string
   return None
  
def get_age_for_person(result_dict, filename, person_name):
	"""
	extract age for person from dictionary
	Args : 
		Input : 
			result_dict : dictionary of speaker metadata data
			filename : name of file in which person appears
			personname : name or code or pseudonym of person for whom age is to be returned
		Returns :
			output_string : age as string if value is present, else None
	"""

  # Check if the file key exists in the dictionary
  if filename in result_dict:
    # Iterate through the list of dictionaries in the specified file
    for person in result_dict[filename]:
      # Check if the person's name matches the specified name
      if person['Nom'] == person_name:
        # Return the current job of the specified person
        output_string = f"# Age={person['Age']}\n"
        return output_string
   return None
  
def get_placeBorn_for_person(result_dict, filename, person_name):
	"""
	extract place of birth for person from dictionary
	Args : 
		Input : 
			result_dict : dictionary of speaker metadata data
			filename : name of file in which person appears
			personname : name or code or pseudonym of person for whom place of birth is to be returned
		Returns :
			output_string : place of birth as string if value is present, else None
	"""
  # Check if the file key exists in the dictionary
  if filename in result_dict:
    # Iterate through the list of dictionaries in the specified file
    for person in result_dict[filename]:
      # Check if the person's name matches the specified name
      if person['Nom'] == person_name:
        # Return the current job of the specified person
        output_string = f"# LieuNaissance={person['LieuNaissance']}\n"
        return output_string
  return None
 
def get_etudes_for_person(result_dict, filename, person_name):
	"""
	extract education level for person from dictionary
	Args : 
		Input : 
			result_dict : dictionary of speaker metadata data
			filename : name of file in which person appears
			personname : name or code or pseudonym of person for whom education level is to be returned
		Returns :
			output_string : education level as string if value is present, else None
	"""
  # Check if the file key exists in the dictionary
  if filename in result_dict:
    # Iterate through the list of dictionaries in the specified file
    for person in result_dict[filename]:
      # Check if the person's name matches the specified name
      if person['Nom'] == person_name:
        # Return the current job of the specified person
        output_string = f"# NiveauEtudes={person['Études']}\n"
        return output_string
  return None
 
def get_genre_for_person(result_dict, filename, person_name):
	"""
	extract gender for person from dictionary
	Args : 
		Input : 
			result_dict : dictionary of speaker metadata data
			filename : name of file in which person appears
			personname : name or code or pseudonym of person for whomgenderh is to be returned
		Returns :
			output_string : gender as string if value is present, else None
	"""
  # Check if the file key exists in the dictionary
  if filename in result_dict:
    # Iterate through the list of dictionaries in the specified file
    for person in result_dict[filename]:
      # Check if the person's name matches the specified name
      if person['Nom'] == person_name:
        # Return the current job of the specified person
        output_string = f"# Sexe={person['Sexe']}\n"
        return output_string
  return None
 
def get_FLM_for_person(result_dict, filename, person_name):
	"""
	extract Français langue maternelle status for person from dictionary
	Args : 
		Input : 
			result_dict : dictionary of speaker metadata data
			filename : name of file in which person appears
			personname : name or code or pseudonym of person for whom Français langue maternelle status is to be returned
		Returns :
			output_string : Français langue maternelle status as string if value is present, else None
	"""
  # Check if the file key exists in the dictionary
  if filename in result_dict:
    # Iterate through the list of dictionaries in the specified file
    for person in result_dict[filename]:
      # Check if the person's name matches the specified name
      if person['Nom'] == person_name:
        # Return the current job of the specified person
        output_string = f"# FrançaisLangMat={person['FR_lang_mat']}\n"
        return output_string
  return None

#speaker = "L1"
def get_speaker_metastring(speaker, filename, result_dict):
 """
 Create single string of metadata for speaker based on returned values
 Args : 
 	Input : speaker : name/pseudonym/code for speaker
 			filename : name of file in which person appears
 			result_dict : dictionary of speaker metadata
	Returns : single string of concatenated metadata strings with NR for unattributed values
 """

 namestring = f"# Loc={speaker}\n"
 
 jobstring = get_job_for_person(result_dict, filename, speaker)
 if jobstring is None:
  jobstring = f'# Occupation=NR\n'
 agestring = get_age_for_person(result_dict, filename, speaker)
 if agestring is None:
  agestring = f'# Age=NR\n'
 placebornstring = get_placeBorn_for_person(result_dict, filename, speaker)
 if placebornstring is None:
  placebornstring = f'# LieuNaissance=NR\n'
 etudes_string= get_etudes_for_person(result_dict, filename, speaker)
 if etudes_string is None:
  etudes_string = f'# NiveauEtudes=NR\n'
 genrestring =get_genre_for_person(result_dict, filename, speaker)
 if genrestring is None:
  genrestring = f'# Sexe=NR\n'
 FLM_string = get_FLM_for_person(result_dict, filename, speaker)
 if FLM_string is None:
  FLM_string = f'# FrançaisLangMat=NR\n'

 speaker_metas = "".join([string for string in [namestring, jobstring, agestring, placebornstring, etudes_string, genrestring, FLM_string]])
 return speaker_metas

def convert_speaker_metas_toDict(speaker_csv_file):
 """
 take a csv file of speaker metadata and convert it to a dictionary
 Args : 
 	Inputs : speaker_csv_file : absolute path to a csv file with speaker metadata
 	Return : result_dict : a dictionary of metadata for each speaker
 """
 speaker_csv_data = pd.read_csv(speaker_csv_file, sep=";", usecols=[1,2,3,4,5,6,7,8])

 result_dict = {}
 
 for _, row in speaker_csv_data.iterrows():
  file_key = row['fichier']
  row_dict = {
   "Nom":row["Nom"],
   "LieuNaissance":row['LieuNaissance'],
   "Occupation":row['Occupation'],
   "Études":row["Études"],
   "Age":row["Age"],
   'Sexe':row["Sexe"],
   'FR_lang_mat':row['FR_lang_mat']
  }
  if file_key not in result_dict:
    result_dict[file_key] = []
  
  # Append the row dictionary to the list of people for this file key
  result_dict[file_key].append(row_dict)
 return  result_dict

def get_speaker_set(current_sent):
 """
 get list of all unique speakers in current sentence based on contents of conllu column 10
 Args : 	
 	Input : current_sent : conllu annotations of current sent
 	Returns : speakers_set : list of the values in the set of speakers appearing in the sentence
 """
 speakerslist = []
 for token in current_sent.tokens:
  speaker = re.findall(r"<spk=\"(.+?)\"", token.misc)
  speakerslist.append(speaker[0])
 
 # get list of unique speakers
 speakers_set = list(set(speakerslist)) 
 return speakers_set

def import_prepare_for_metaaddition(input_file):
   '''
   take a conllu file and prepare for annotation with stanza, export
   Args : 
     input_file : path to file to tag
   Returns:
     source_doc : stanza document
     all_sentsout : list of sentences to pass to stanza
   
   not used in this version :outputfile : filename for stanza export
   Notes : uses regex to tidy speaker, PLX_tag, timing info
   '''
   with open(input_file, 'r',encoding='UTF-8') as w:
    input_data = w.read()

   pattern = r'<plx="..."><t1=".+?"><t2=".+?"><spk="<t1="(.+?)"><t2="(.+?)"><spk="(.+?)">">'
   repl = r'<t1="\1"><t2="\2"><spk="\3">'
   test = re.sub(pattern, repl, input_data)
   with open(input_file, 'w',encoding='UTF-8') as w:
    w.write(test)
   source_doc = CoNLL.conll2doc(input_file)
 
   return source_doc

def run_orfeo_metainserter(corpus, speaker_csv_file):
 """
 Run metadata inserter for all oral corpora, even though orfeo is present in functionname
 Args : 
 	Input : 
 		Corpus : corpus being processed
 		speaker_csv_file : csv file containing metadata on speakers
 	Returns : no return object ; writes a file to location `output_name` with metadata strings in conllu metalines
 """
 spacer = "\n"
 #speaker_csv_file =f'/Users/Data/ANR_PREFAB/CorpusPREFAB/oral_done/{corpus}/2024-06-20-{corpus}-speakers.csv'
 result_dict = convert_speaker_metas_toDict(speaker_csv_file) 

 # conll_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/ORFEO/CFPB-7000/CFPB-1000-5-7000tv8.conllu'
 # conll_files = sorted(glob.glob(f'/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/ORFEO/{corpus}-7000/*-7000tv8.conllu'))
 conll_files = sorted(glob.glob(f'/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/ESLO2-7000/*-7000tv8.conllu'))
 for conll_file in tqdm(conll_files):
  conll_data = import_prepare_for_metaaddition(conll_file)
  filename = os.path.basename(conll_file).replace("-7000tv8.conllu",'')
  if filename.startswith("TCOF-"):
   filename = filename[5:]
  #output_name = conll_file.replace('7000tv8','PREFAB_V2').replace("MPF-","")
  # output_name = f'/Users/Data/ANR_PREFAB/CorpusPREFAB/oral_done/{corpus}/{filename}.conllu'
  output_name = f'/Users/Data/ANR_PREFAB/CorpusPREFAB/oral_done/{corpus}/{filename}.conllu'
  with open(output_name,'w',encoding="UTF-8") as w:
  # reset linestore, add sentid, before get next sent, need to add spacer
   for current_sent in conll_data.sentences:
    linestore, senttime_string =[], ""
    # current_sent = conll_data.sentences[s]
    # linestore.append(current_sent.comments[0])
    _ = w.write(current_sent.comments[0])
    senttime_string = get_sent_times(current_sent)
    # print(senttime_string)
    # linestore.append(senttime_string)
    _ = w.write(senttime_string)
    # get list of unique speakers
    speakers_set = get_speaker_set(current_sent)
    if len(speakers_set) == 1:
      speaker_metas = get_speaker_metastring(speakers_set[0], filename, result_dict)
    if len(speakers_set) > 1:
      speaker_metas = "\n".join([get_speaker_metastring(speakers_set[n], filename, result_dict) for n in range(len(speakers_set))])
    # linestore.append(speaker_metas)
    _ = w.write(speaker_metas)
    for token in current_sent.tokens:
     line = make_tidy_conll_line(token)
     # linestore.append(line)
     _ = w.write(line)
    _ = w.write(spacer)
    # linestore.append(spacer)
    # for item in linestore:
     # _ = w.write(item)
 
###### run section    

# example path to file with annotations
#input_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/ORFEO/CFPP-7000/Christophe_Andre_H_62_Marie_Anne_Andre_F_63_5e-7000tv8.conllu'

## getting speaker info for metalines
# speaker_csv_file = '/Users/Data/ANR_PREFAB/Data/Métadonnées corpus oraux/Metadata Compilation/2024-06-18-MPF_speakers.csv'
# indicate corpus being processed and file with speaker metadata for this corpus, then run the metainserter.
corpus = 'ESLO2'
speaker_csv_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/oral_done/ESLO2/2024-06-21-ESLO2-speakers.csv'
run_orfeo_metainserter(corpus, speaker_csv_file)

