## about : script to facilitate processing jsons from UDPipe 
## 
# json simple processing
# imports
import json, send2trash, pathlib, glob, os, zipfile, re

# functions
def zip_files(source_files, destination_zip, folder):
 """
 Zips a list of source files into a destination zip archive with compression level 9.

 Args:
   source_files: A list of paths to the files to be zipped.
   destination_zip: The path to the destination zip archive.
   folder : path to folder in which the zipfile will be created 
 """
 with zipfile.ZipFile(destination_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
  for source_file in source_files:
   source_path= pathlib.Path(source_file)
   zip_file.write(source_file, arcname=source_path.relative_to(folder), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)

def json_to_conllu(subfolder):
  '''
  From a path to a folder containing 5 jsons, convert jsons from UDPipe to conllu, incrementing file suffixes as necessary. Simplex processor.
  Args :
  	subfolder : absolute path to a folder containing json files from UDPipe.
  	If 5 files are present, the simplex processor runs, extracting conllu annotations from jsons and returning conllu files. 
  	If more than 5 files are present, the function will not run.
  	All jsons are copied, compressed, and the originals sent to system trash.
  Returns :
  	no return object : 1x conllu file created for each model, with suffix incremented by 10. 1x zip file made containing all json files.

	Notes : 
  # if json has filesize 30 = json contains error message that input is oversize (>4,4Mo)
  # if json has filesize 120 conll contains a sentence of over 1000 words, which is usually either a list or a wikitable
  '''
  
  json_files = glob.glob(subfolder + "*.json")

	## determine number of files and whether errors present  
  if len(json_files) ==5 and os.path.getsize(json_files[0])!= 30 and os.path.getsize(json_files[0])!= 120 :
   #iterate over files extracting code from name, then reading file, parsing json, extracting result, writing to output
   for json_file in json_files:
    model_codein = json_file[-8:-5]
    model_codeout = str(int(model_codein) + 10)
    output_path = json_file.replace(model_codein, model_codeout).replace('.json', '.conllu').replace("-104-30","-30") 
    if "-104-" in output_path:
      output_path = output_path.replace('-104-','-')
    with open(json_file, "r", encoding='UTF-8') as j:
        json_data = json.load(j)
        conll_in = json_data["result"]
    with open(output_path, "w", encoding = "UTF-8") as k:
        k.write(conll_in)
   source_files = json_files
   destination_zip = json_files[0].replace('-301.json','-json_raws.zip').replace('-104-301.json','-json_raws.zip')
   if "-104-" in destination_zip:
    destination_zip = destination_zip.replace('-104-','-')
   folder = path= subfolder
   zip_files(source_files, destination_zip, folder)
   print(f'{destination_zip} créé') 
   for file in json_files:
    send2trash.send2trash(file)
    
def json_to_conllu_consol( subfolder):
 '''
 From a path to a folder containing 5 jsons, convert jsons from UDPipe to conllu, incrementing file suffixes as necessary. Complex processor.
   
 Args :
  subfolder : absolute path to a folder containing json files from UDPipe.
  If more than files are present, the complex processor runs, extracting conllu annotations from jsons, concatenating conllu_data and returning 1 conllu file per parser.
  If 5 files are present, the function will not run.
  All jsons are copied, compressed, and the originals sent to system trash.
  In concatenation, statements of generator, model and licence are removed from all except the aa slice.
 Returns :
  no return object : 1x conllu file created for each model, with suffix incremented by 10. 1x zip file made containing all json files.

  Notes : 
  # if json has filesize 30 = json contains error message that input is oversize (>4,4Mo)
  # if json has filesize 120 conll contains a sentence of over 1000 words, which is usually either a list or a wikitable

 '''
 
 ## determine number of files and whether errors present  
 json_files = sorted(glob.glob(subfolder+ "*.json"))
 if len(json_files) > 5:
   json_files = [file for file in json_files if os.path.getsize(file) >124 ] # check for fail on 1 file as whole
   connl_inputs = [file.replace("-301.json", ".conllu") for file in json_files] # 
   connl_inputs = [file for file in connl_inputs if ".conllu" in file] # get conllu inputs
   codes = [item.replace(".conllu", "").replace(subfolder,'') for item in connl_inputs]
   for model_code in ['301', '302', '303', '304', '305']:
     output_file = subfolder + codes[0].replace("aa",'-') + f'{str(int(model_code) + int(10))}' + ".conllu"
     if '-104-31' in output_file:
      output_file = output_file.replace('-104-31','-31')
     input_files = [subfolder + code + f"-{model_code}.json" for code in codes]
     for f, file in enumerate(input_files):
      with open(file, "r", encoding='UTF-8') as j:
        json_data = json.load(j)
        conll_in = json_data["result"]
        regex = r'# generator = UDPipe 2, https://lindat.mff.cuni.cz/services/udpipe\n# udpipe_model = .+?\n# udpipe_model_licence = CC BY-NC-SA'
        reg_repl = ""
 
        if f ==0:
          output = conll_in
        else :
          tidy_conll = re.sub(regex, reg_repl, conll_in)
          output = str(output) + str(tidy_conll)   
 
     with open(output_file, "w", encoding = "UTF-8") as k:
       k.write(output)
     print(output_file)

   source_files = json_files
   destination_zip = json_files[0].replace('-301.json','-json_raws.zip').replace('-104-301.json','-json_raws.zip').replace('aa','')
 if "-104-" in destination_zip:
  destination_zip = destination_zip.replace('-104-','-')
 folder = path = subfolder
 zip_files(source_files, destination_zip, folder)
 print(f'{destination_zip} créé') 
 ## TODO : add cli arg to activate this
 for file in json_files:
  send2trash.send2trash(file)
 

def run_json_preprocessors(folder):
 '''
 Take a folder, with tidied subfolders of parses, and preprocess jsons to convert to conllu, concatenating any splits, zipping jsons and sending originals to trash
 Args : folder : absolute path to an organised folder of subfolders of parses
 Returns : no return object, creates conllu files with necessary suffixes and extensions, moves jsons to trash after extraction of conllu and zipping
 '''
 empty_chunk_error_message = '''
 Are you running this script on a LETTER level folder or a numbered subfolder ?\n
 '''
 ## generate a list of chunks by removing folder paths to get list of folder names to be operated with
 chunks = [item.replace(folder,'').replace('/','') for item in glob.glob(folder + "*/" )]
 if len(chunks) ==0:
   print(empty_chunk_error_message)
 #chunks = ['HS62']
 # for each chunk, construct the path to the folder to be processed
 # although the path is passed to both versions of the json_to_conllu function, only 1 version will run as they both use a checker to determine whether to run, only running if their requirements are met
 for chunk in chunks:
   path = folder + chunk + "/"
   json_to_conllu(path)
   json_to_conllu_consol(path)
  #print(path)

# del(path, subfolder)
# ## running
# RTUGChar
# specify an input letter or letters :: TODO : render this for argparser and CLI
letters = ["Processing"]

# iterate over the letters to run the processors for the letter folder
for letter in letters: 
 folder =  f'/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/{letter}/'
 #folder = f'/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/{letter}/'
 run_json_preprocessors(folder)

# path = folder + "2_00/"
