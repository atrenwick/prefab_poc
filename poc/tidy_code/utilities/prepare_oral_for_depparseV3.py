## prepare oral for depparse v3
## take output from lemmatisation == 6000 file, with PLX annotations reinserted, then sent to 6001, then6022 state. then ready for depparse
import string    
import re
input_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/Oral_prefab_def/TCOF2/uncompressed/TCOF-1crossfit_mao_15/TCOF-1crossfit_mao_15-6000tv8.conllu'

# send any remaining qu'est-ce que, to correct number of tokens with right tagg
def peaufiner_questques(input_file, output_file):
  """
  function to apply specific regex replacements to oral sentences
  inputs : 
  			input_file : absolute path to a file with conllu data in 10 columns
  			output_file : absolute path to a file to write
  output : no output object, output_file is written
  """
  with open(input_file, 'r', encoding="UTF-8") as f:
    input_data = f.read()
    patternZ = r'(\d+)\t(Qui|qui)\tqui\tPRON\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce\test-ce\t.+?\t_\t_\t_\t_\t_\t<t1=".+?">.+?\n\d+\tque\tque\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replZ = r'\1\t\2 est-ce que\tqui est-ce que\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\5">\6\n'
    
    #Y qui est-ce qu' en 3
    patternY = r'(\d+)\t(Qui|qui)\tqui\tPRON\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce\test-ce\t.+?\t_\t_\t_\t_\t_\t<t1=".+?">.+?\n\d+\tqu\'\tque\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replY = r'\1\t\2 est-ce qu\'\tqui est-ce que\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\5">\6\n'
    
    #Y qui est-ce quI en 3
    patternX = r'(\d+)\t(Qui|qui)\tqui\tPRON\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce\test-ce\t.+?\t_\t_\t_\t_\t_\t<t1=".+?">.+?\n\d+\tqui\tqui\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replX = r'\1\t\2 est-ce qui\tqui est-ce qui\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\5">\6\n'
    
    # W qu'est-ce que/qu' en 3
    patternW = r'(\d+)\t(Qu\'|qu\')\tque\t.+?\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce\test-ce\t.+?\t_\t_\t_\t_\t_\t<t1=".+?">.+?\n\d+\t(qu\'|que)\tque\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replW = r'\1\t\2 est-ce \5\tqu\'est-ce \5\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\6">\7\n'
    
    #V qu'est-ce qui en 3
    patternV = r'(\d+)\t(Qu\'|qu\')\tque\t.+?\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce\test-ce\t.+?\t_\t_\t_\t_\t_\t<t1=".+?">.+?\n\d+\tqui\tqui\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replV = r'\1\t\2est-ce qui\tqu\'est-ce qui\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\5">\6\n'
    
    
    #U qu'est-ce qu'/que en 2
    patternU = r'(\d+)\t(Qu\'|qu\')\tque\t.+?\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce (qu\'|que)\test-ce (qu\'|que)\t.+?\t_\t_\t.\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replU = r'\1\t\2 est-ce \5\tqu\'est-ce que\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\7">\8\n'
    
    #Tqu'est-ce qui en 2
    patternT = r'(\d+)\t(Qu\'|qu\')\tque\t.+?\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce (qui)\test-ce (qui)\t.+?\t_\t_\t.\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replT = r'\1\t\2 est-ce \5\tqu\'est-ce qui\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\7">\8\n'
    
    #S qui est-ce qu'/que en 2
    patternS = r'(\d+)\t(Qui|qui)\tqui\tPRON\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce (qu\'|que)\test-ce (que)\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replS = r'\1\t\2 est-ce \5\tqui est-ce que\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\7">\8\n'
    
    #R qui est-ce qui en 2
    patternR = r'(\d+)\t(Qui|qui)\tqui\tPRON\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\test-ce (qui)\test-ce (qui)\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replR = r'\1\t\2 est-ce \5\tqui est-ce qui\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\7">\8\n'

    #Q est-ce que en 2
    patternQ = r'(\d+)\t(Est|est)-ce\test-ce\tPRON\t_\t_\t(.+?|.)\t_\t_\t<t1="(.+?)">.+?\n\d+\t(que|qu\')\t(que)\t.+?\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">(.+?)\n'
    replQ = r'\1\t\2-ce \5\test-ce que\tPRON\tPronType=Int\t_\t_\t\3\t_\t_<t1="\4"><t2="\7">\8\n'
    
    patterns = [patternZ, patternY, patternX, patternW, patternV, patternU, patternT, patternS, patternR, patternQ]
    repls = [replZ, replY, replX, replW, replV, replU, replT, replS, replR, replQ]
    
    this_data = input_data 
    for pattern, replacement in zip(patterns, repls):
      updated = re.sub(pattern, replacement, this_data)
      this_data = updated 
    
    with open(output_file, 'w', encoding="UTF-8") as f:
      for line in updated:
        line_out = line.replace('\\','')
        _ = f.write(line_out)


#punctuation_set = set(string.punctuation)
#punctuation_set.add(elements[1][0])

exceptionalCases = ['-là', '-ci', "."]

def process_leading_punct(current_sent, exceptionalCases):
	"""
	process sentences detatching leading punct when necessary
	args : input :
		current_sent : sentence on which to operate
		exceptionalCases : list of special cases where - are not to be detatched, and .
	returns : results : a listof strings, each representing a conllu sentence
	"""
    results = []
    metachunk = "\n"+ current_sent.comments[0] + "\n" + current_sent.comments[1] + "\n" + current_sent.comments[2] + "\n"
    results.append(metachunk)
    ndx=0 
    for token in current_sent.tokens:
        #elements = current_sent.tokens[15].to_conll_text().split('\t')
        elements = token.to_conll_text().split('\t')
        if elements[1].lower() == "putain" and elements[3] in ("PROPN","ADJ","ADV"):
          elements[2] = "putain"
          elements[3] = "INTJ"
        if elements[1].lower() == "hum" and elements[3] in ("NOUN","PROPN"):
          elements[2] = elements[2].lower()
          elements[3] = "INTJ"
        if elements[1].lower() == "d\'accord" and elements[3] != "ADV":
          elements[2] = elements[2].lower()
          elements[3] = "ADV"
        if elements[1].lower() == "faut" and elements[2] in ("faillir","PROPN"):
          elements[2] = "falloir"
          elements[3] = "VERB"

        # Check if elements[1] is in exceptionalCases
        if  len(elements[1]) >1  and elements[1] in exceptionalCases:
            results.append('\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:]))
            continue
        if len(current_sent.tokens) ==1  and elements[1] in exceptionalCases:
            results.append('\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:]))
            continue

        special_outputs = []
        
        # Check if the first element starts with consecutive punctuation
        if elements[1] in ("-y", "-en", "—y", "—en", "-en", "-y"):
          this_char = elements[1][0]
          elements[1] = elements[1][1:]
          spacer_line = "\t".join([f'{this_char}',f'{this_char}', 'PUNCT', '_','_',str(ndx),'_','_',elements[-1]])
          results.append(spacer_line)
          ndx +=1
          mod_line = ('\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:]))
          results.append(mod_line)
          continue

        else : 
          if len(elements[1]) >1:
            if  elements[1][0] in punctuation_set is True:
                punc_chars = []
                i = 0
                # elements[1][0][i]
                # Collect consecutive punctuation characters
                while i < len(elements[1]) and elements[1][i].isalpha() is False:
                    punc_chars.append(elements[1][i])
                    i += 1
                
                final_column = elements[-1]
                
                # Create special output strings for each punctuation character
                for punc_char in punc_chars:
                    special_output = f"{punc_char}\t{punc_char}\tPUNCT\t_\t_\t{str(ndx)}\t_\t_\t{final_column}"
                    special_outputs.append(special_output)
                    ndx+=1
            
                # Remove the collected punctuation characters from the first element
                elements[1] = elements[1][i:]
          
# ndx=4        
        modified_string = '\t'.join(elements[1:6]) + "\t" + str(ndx) +"\t"  +'\t'.join(elements[7:])
        if len(special_outputs) >0:
          for output in special_outputs:
            results.append(output)
        results.append(modified_string)
        ndx+=1
    return results


def prepare_oral_for_depparse(corpus, pathB, input_files):
  """
  run the preparer functions defined above for each file within a corpus
  """
  ## make filenames and run peaufiner
  for input_file in tqdm(input_files):
    output_file =input_file.replace('6000','6001')
    pathA = os.path.dirname(output_file) + "/"
    done_file = output_file.replace('6001', '6022').replace(pathA, pathB)
    peaufiner_questques(input_file, output_file)
    
    ## once changes madewith regex, import and render as stanza doc
    source_doc = import_prepare_for_stanza(output_file)
	## process leading punctuaiton and create output, then write    
    outputstrings = []
    for current_sent in source_doc.sentences:
      results = process_leading_punct(current_sent, exceptionalCases)
      q=0
      for result in results:
          result = re.sub('_unknownpostag_','_', result)
        
          if result.startswith("\n#") is False:
            this_string = (str(q+1) + "\t" + str(result) +"\n")
            q+=1
            outputstrings.append(this_string)
          else:
            outputstrings.append(result)
    
    with open(done_file,'w',encoding='UTF-8') as k:
      for string in outputstrings:
       _= k.write(string)
        


#### run section
#/Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/Oral_prefab_def/MPF/uncompressed
# specify inputcorpus, file, path, then invoke the function to run the script over the corpus
corpus = "MPF"
pathB = f'/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse/{corpus}/'
input_files  = glob.glob(f'/Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/Oral_prefab_def/{corpus}/uncompressed/*/*-6000tv8.conllu')
prepare_oral_for_depparse(corpus, pathB, input_files)




   

files = glob.glob("/Users/Data/ANR_PREFAB/CorpusPREFAB/*/*/*/*6000tv8.conllu")
