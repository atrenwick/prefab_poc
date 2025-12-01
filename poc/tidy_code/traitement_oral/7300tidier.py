# oral 7300 comment tidier
# read in conll files and make 2 tweaks to metalines:
## sent.comments[0] contains a repetition of the sentID. a regex is used to remove this repetition
## sent.comments[-1] contains a renumbering from 0 of sentIDs. this is removed
import re
from tqdm import tqdm
from stanza.utils.conll import CoNLL
# specify paths for input and output
inpath = '/7300'
outpath = '/7300/7300tidy'
# specify input files
input_files = glob.glob('C:/Users/Data/CorpusOraux/CorpusOralPrefabV3/ESLO2_prefabV3/*/7300/*.conllu')
len(input_files)
for input_file in tqdm(input_files):
  # input_file = input_files[23]
  outputfile = input_file.replace('\\','/').replace(inpath, outpath)
  outputfolder = os.path.dirname(input_file).replace('\\','/') + '/7300tidy/'
  if os.path.exists(outputfolder) is False:
    os.makedirs(outputfolder)
  conll_input = CoNLL.conll2doc(input_file)

  with open(outputfile, 'w', encoding='UTF-8') as w:
  
    # remove duplicate of sentID in 0th comment
    for this_sent in (conll_input.sentences):
      sent_id_bad = this_sent.comments[0]
      sent_id_good = re.sub('(#.+?)#.+',r'\1',sent_id_bad)
      this_sent.comments[0] = sent_id_good
  
      comment_chunk = "\n" + "\n".join([comment for comment in this_sent.comments[:-1]]) + "\n"
      _ = w.write(comment_chunk)
      for token in this_sent.tokens:
        line = token.to_conll_text() + '\n'
        _ = w.write(line)
        
