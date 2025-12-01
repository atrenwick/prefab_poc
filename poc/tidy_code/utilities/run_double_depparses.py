#imports
import io, os, stanza, glob, argparse
from stanza.utils.conll import CoNLL
from tqdm import tqdm

#functions
def import_prepare_for_stanza(input_file):
    '''
    import a conllu file as a stanza doc object
    Args : 
        input_file : path to file to open
    Returns:
        source_doc : stanza document with annotations
    '''
    source_doc = CoNLL.conll2doc(input_file)

    return source_doc

def write_depparses_preferentially(output_file, dep_annotations, feat_annotations): 
  """
  preferentially write features from depparse and re-parse of same document
  Args :
  	input : output_file: conllu document to be written with dep_annotations and certain feat_annotations
  			dep_annotations: conllu document with all annotations from depparse function
  			feat_annotations : conllu document with feature annotations based on re-POS-MWT-LEM analysis of final tokenisation
	Returns : no return object, output_file is exported 

  This function takes as input a document on which depparse has already been run. An initial pass is made re-analysing the document based on the final tokenisation, predicting POS, LEM and morphological features.
  These annotations are returned as feat_annotations, and are not exported. 
  Once this has been completed, the document with the dep_annotations is loaded and prepared for export back to file. For each token, if the token text is identical in the dep_annotations and the feat_annotations AND the POS is identical AND the lemma is identical AND the dep_annotations feats are _, then and only then are the dep_annotations feat_values set to those of feat_annotations.
  Strings are then prepared for export to file as conllu.
  """  
  with open(output_file, 'w', encoding="UTF-8") as f:
    for sentA, sentB in zip(dep_annotations.sentences, feat_annotations.sentences):
      id_string = '\n' + str(sentA.comments[0])   + '\n'
      #metadata = '\n# text = ' + str(sentence.comments[1]) + '\n' + sentence.comments[2]+ "\n"
      metadata = "\n".join([sentA.comments[i] for i in range(1, len(sentA.comments)-1)]) +"\n"
      # print(id_string)
      _ =  f.write(id_string)
      # print(metadata)
      _ =  f.write(metadata)
      for Atoken, Btoken in zip(sentA.tokens, sentB.tokens):
        if (Atoken.text != Btoken.text):
          conllA=  Atoken.to_conll_text().split("\t")
        else:  
          conllA, conllB = Atoken.to_conll_text().split("\t"), Btoken.to_conll_text().split("\t")
          # check lemmas @ 2 and POS at 3  and B has feats and A has no feats
          if (conllA[2] == conllB[2]) and (conllA[3] == conllB[3]) and (conllB[5] != "_") and (conllA[5] == "_"):
            conllA[5] = conllB[5]
    
        line_to_print = "\t".join([item for item in conllA ] ) +"\n"
        #print(line_to_print)  
        _ = f.write(line_to_print)  

# run
# savepath = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/depparse_done/'
# conll_files = glob.glob(f'//Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/to_depparse/*.conllu')
savepath = "//Users/Data/PREFAB/wiki/"
conll_files = glob.glob('//Users/Data/PREFAB/wiki/*7000*.conllu')
#conll_files = ['//Users/Data/PREFAB/depparseMe/done_with_commas/7000/318-7000tv8.conllu', '//Users/Data/PREFAB/depparseMe/done_with_commas/7000/322-7000tv8.conllu']

if os.path.exists(savepath) is False:
    os.makedirs(savepath)
if len(conll_files) <=1:
	print("No files found")  

if len(conll_files)>0:
	print(f"Processing {len(conll_files)} files")
	feat_pipe = stanza.Pipeline(lang="fr", processors = 'tokenize,pos,mwt', tokenize_pretokenized=True, tokenize_no_ssplit=True)
	# conll_current = '/Users/Data/ANR_PREFAB/CorpusPREFAB/oral_done/CFPB_with-il_prob/CFPB-1083-3B.conllu'
	for conll_current in tqdm(sorted(conll_files)):
		feat_doc_in = import_prepare_for_stanza(conll_current)  
		feat_annotations = feat_pipe(feat_doc_in)
  
		### current version :: comment the lines between here and until, but not including, dep_annotations importing from conll_current  
		#### TODO : for next version; use argument to activate or deactivate these lines with IF ==
		# dep_pipe = 	stanza.Pipeline(lang='fr', processors='depparse', depparse_pretagged=True, logging_level = "Info")
		# dep_doc_in = import_prepare_for_stanza(conll_current)
		# dep_annotations = dep_pipe(dep_doc_in)
		dep_annotations = import_prepare_for_stanza(conll_current)
	output_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/error/GE68-7400v2v2tv8.conllu'
		savename = os.path.basename(conll_current)
		output_file = savepath + savename.replace("7230v","7300")
		write_depparses_preferentially(output_file, dep_annotations, feat_annotations)    
CoNLL.write_doc2conll(feat_annotations, output_file)
	print(f"Wahoo, subcorpus done\n")        
