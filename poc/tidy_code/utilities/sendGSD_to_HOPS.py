
# processing HOPS_error segments : send to stanza, get back
# this script takes the conllu from stanza and turns it into a csv as if HOPS had returned it
import platform, io, os, stanza, re,  glob
from stanza.utils.conll import CoNLL
from tqdm import tqdm

def build_fullrows_from_conll(conll_data):
  """
  take conllu_data and build to look like HOPS output 
  Args : input : 
  	conll_data : StanzaGSD parse of text
  Returns : 
  	all_sents : a list of all the sentences built to look like HOPS input
  """
  all_sents = []
  for s in range(len(conll_data.sentences)):
      sent_rows = ["\n"]
      this_sent = conll_data.sentences[s]
      for t, token in enumerate(this_sent.tokens):
          chunk = token.to_conll_text().split("\t")
          UUID = this_sent.comments[0] +"-"+ str(t+1).zfill(3)
          UUID =UUID.replace("# sent_id = ","")
          row = "\t".join([str(chunk[0]), str(chunk[1])  , str(UUID) , str(chunk[3]), str(chunk[4]), str(chunk[5]), str(chunk[6]), str(chunk[7]), str(chunk[8]), str(chunk[9])]) + "\n"
          sent_rows.append(row)
      all_sents.append(sent_rows)        
  return all_sents

def send_GSD_to_HOPS(input_file, csv_output_file):
  """
  load a conllu_file as a stanza conllu document and export it as an imitation of a HOPS parse
  Args : 
  	Input :
  		input_file : absolute path to a Stanza GSD parse of a text of which an imitation is to be made
  		csv_output_file : absolute path to a csv file which will hold the imitation of the HOPS annotations
	Returns : no return object, csv_output_file exported
  """
  conll_data = CoNLL.conll2doc(input_file)
  
  all_sents = build_fullrows_from_conll(conll_data)
  
  with open(csv_output_file, 'w', encoding = "UTF-8") as h:
      for sent in all_sents:
          for row in sent:
              _ = h.write(row)
  
# import, define output name
input_file = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/copies_for_stanzapass/A_01-104be_out.conllu'
csv_output_file = "/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/copies_for_stanzapass/A_01-104be_out_sent_tocsv.csv'"

## provide list of files over which to iterate
target_files = ['/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/copies_for_stanzapass/A_01-104ay_out.conllu','/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/copies_for_stanzapass/A_01-104be_out.conllu']

# path to folder in which to look for files to process
path = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/to_prepare_asHOPS/'
target_files = glob.glob(path + "*.conllu")
for target_file in tqdm(target_files):
  input_file = target_file
  output_file = target_file.replace('.conllu', '.csv')
  send_GSD_to_HOPS(input_file, output_file)
  
## join HOPS and GSD slices as CSV
# loop to take specific folders of input and for each folder, iterate over model_codes of imitation HOPS data and concatenate true HOPS annotations with imitation HOPS annotations, and export a single file per parser
chunk_code = "B_01", "C_04", "F_00", "", "M_03", "P_01", 
for chunk_code in ["L_04"]:
  print(chunk_code)
  for model_code in ['5262', '5272', '5282', '5287', '5292', '5297']:
    csv_data = ""
  
    output_file = f'/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/{chunk_code}_done/{chunk_code}-{model_code}.csv'
    csv_files = sorted(glob.glob(f'/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/{chunk_code}_done/*{model_code}.csv'))
    for f, file in tqdm(enumerate(csv_files)):
      with open(file, 'r', encoding = "UTF-8") as c:
        csv_in = c.read()
    
        csv_data = str(csv_data) + "\n" + csv_in
    with open(output_file, 'w', encoding = "UTF-8") as o:
      _ = o.write(csv_data)
    
