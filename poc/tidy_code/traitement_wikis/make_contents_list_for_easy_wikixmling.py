# helper loop to make contents file for letter
folder = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/_depparsed/7200/'
suffix = "*.conllu"
conll_files = glob.glob(folder + suffix)
for file in tqdm(conll_files):
 fileshort = os.path.basename(file)
 outputfile = folder +fileshort.replace('-7200v3tv8.conllu','_contents.csv')
 source_doc = import_prepare_for_stanza(file)
 results = []
 for sentence in source_doc.sentences:
  # sentence = source_doc.sentences[23]
  sentID= re.sub("#sent_id = ", "", sentence.comments[0])
  doublet = fileshort, sentID
  results.append(doublet)
   
  
 with open(outputfile, 'w', encoding="UTF-8") as f:
  for result in results:
   string = result[0] + "\t" + result[1] + "\n"
   _ = f.write(string)
   
