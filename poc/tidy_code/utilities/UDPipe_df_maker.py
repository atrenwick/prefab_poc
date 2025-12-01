## script to make a df, saved as csv, of commands that can be sent to UDPipe with udpipe_runner.py
# imports
import os, pandas, re, io, glob


def formulate_udpipe_APIcall(fichier_conllu, model_code, reglage, local_path, remote_path):
    '''
    Combine input paths, model codes, settings, local and remote paths to make a df of commands to send to the UDPipe REST API, to allow processing in a specific order.
    Args :  fichier conllu : conllu file  to analyse
            model_code : three digit code for model code as detailed below ; unauthorised values will give errors
            reglage : sets the value of reglagesG as either the string necessary for :
                parsing only (`parse`)
                tag and parse `both`
                tag : `tag`
    '''
    if model_code in [301,311] :
        model_name = 'french-gsd'
    if model_code in [302,312] :
            model_name = 'french-parisstories'
    if model_code in [303,313] :
            model_name = 'french-partut'
    if model_code in [304, 314] :
            model_name = 'french-rhapsodie'
    if model_code in [305,315] :
            model_name = 'french-sequoia'
    
    if reglage == "parse":
        processors = ' -F parser= '
    if reglage == "both":
        processors = ' -F tagger= -F parser= '
    if reglage == "tag":
        processors = ' -F tagger= '
    
    fichier_conllu = fichier_conllu.replace(local_path, remote_path).replace('\\','/')##need to deal with escaping here
    tete_commande = 'curl -F data=@/'
    fichier_source = fichier_conllu
    reglagesG = ' -F model='
    input_spec  = ' -F input=conllu'
    reglages_add = f'http://lindat.mff.cuni.cz/services/udpipe/api/process > /'
    fichier_sortie = fichier_conllu.replace('.conllu', f'-{model_code}.json').replace('004/',f'{model_code}/')
    fichier_sortie = fichier_sortie.replace(local_path, remote_path)
    #commande = f'{tete_commande}{fichier_source}{reglagesG}{model_name}input{processors}{reglagesD}{fichier_sortie}'
    commande = f'{tete_commande}{fichier_source}{reglagesG}{model_name}{input_spec}{processors}{reglages_add}{fichier_sortie}'

    return commande


def make_udpipe_commands(path, return_format):
  """
  take a path to a folder and find the conllu files and create make a df of commands for the UDPipe REST API to process these, then write this df to a csv file.
  Args :
  	input : path : absolute path to a folder containing conllu files with the *104*.conllu ending.
  			return_format : string value used to specify output. 
  	Returns : return is conditional based upon string specified in return_format
  		df	: the pd df will be returned. 
  		list: the list of commands will be printed in the console
  		Any other value :  no return object
  
  """
  #input_files = ['/Users/Data/ANR_PREFAB/Data/Corpus_annotations/dev51/oral_20240322_R-200.conllu']
  #path = f'{PathHead}Data/Corpus/Romans/romans_tranche5/004/'
  #path = '/Users/Data/ANR_PREFAB/processing_romans_tranches/romans_tranche4/004/004_uncomp/to_reslice/'
  #input_files = glob.glob(path + "*.conllu")
  input_files =    glob.glob(path + '*104*.conllu')
  input_files = sorted(input_files)
  model_codes = [301, 302, 303, 304, 305]
  settings = ['tag']#, 'parse', 'both']
  commandes = []
  #local_path = f'{PathHead}Data/Corpus/Romans/romans_tranche5/'
  #remote_path =  "//Users/Data/PREFAB/Romans/romans_tranche5/"
  #remote_path =  f"{PathHead}Data/Corpus/Romans/romans_tranche4/"
  local_path = path
  remote_path  = local_path
  for fichier_conllu in input_files:
      for model_code in model_codes:
          for reglage in settings:
              commande = formulate_udpipe_APIcall(fichier_conllu, model_code, reglage, local_path, remote_path)
              commandes.append(commande)
  print(len(commandes))
  
  frame_name = re.sub('/$','_udpipe_commands.csv', path)
  with open(frame_name, "w", encoding = "UTF-8") as k:
      for command in commandes:
          string = command + "\n"
          k.write(string)
  if return_format == "list":
    return_object = commandes
  if return_format == "df":
    return_object = pd.DataFrame(commandes)
  if return_format not in ("list", "df"):
    return_object = None
  return return_object
        
#for letter in ['B','C','D','E','F','G','H','I','J','K','L','M','N','O','P']:    
#  path = f'/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/{letter}/'
#for letter in tqdm(['RT']):
#  input_files = glob.glob(f"/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/Processing/{letter}/" + f"*-104.conllu")
#path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/phraseorom_prefabV2/'
path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/Processing/RT/'
returnobject = make_udpipe_commands(path, "list")

# for i in range(2, len(returnobject)):
#   os.system(returnobject[i])

        
        
