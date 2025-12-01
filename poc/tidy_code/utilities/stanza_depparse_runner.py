#imports
import platform, io, os, stanza, conllu, re, glob
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from tqdm import tqdm
  
# functions for stanza processing
def import_prepare_for_stanza(input_file):
    '''
    take a conllu file and load as stanza document object
    Args : 
        input_file : path to file to tag
    Returns:
        source_doc : stanza document
    '''
    source_doc = CoNLL.conll2doc(input_file)

    return source_doc

    
def write_stanza_annotations(annotated_document, output_file):
    '''
    write stanza document as conllu file
    Args:
    document_out : Stanza document object containing annotations produced by stanza pipeline
    output_file : conllu file to be created in which annotations will be exported
    Returns: no return object
    Notes : get sentence.comments[i] through -1 to avoid getting final item == ? prefab_text??
    
    '''

    with io.open(output_file, 'w', encoding='UTF-8') as f:
### TODO : change this commenting to a CLI-modifiable  option
#        for sentence in annotated_document.sentences:
#            id_string = '\n' + str(sentence.comments[0])   + '\n'
#            #metadata = '\n# text = ' + str(sentence.comments[1]) + '\n' + sentence.comments[2]+ "\n"
#            metadata = "\n".join([chunk for chunk in range(1, len(sentence.comnents))])
#            # print(id_string)
#            f.write(id_string)
#            f.write(metadata)
#      
#            for token in sentence.tokens:
#                f.write(token.to_conll_text() + '\n')
############## variant to print all metas for post-processing reparse
        for sentence in annotated_document.sentences:
            id_string = '\n' + str(sentence.comments[0]) 
            metadata = "\n".join([sentence.comments[i] for i in range( len(sentence.comments)-1)]) + "\n"
            #print(id_string)
            f.write(id_string)
            #print(metadata)
            f.write(metadata)
            for token in sentence.tokens:
                f.write(token.to_conll_text() + '\n')




def run_stanza_depparse_120(target_file, nlp):
    # 3.1 Annotate with GSD pipeline
    source_doc = import_prepare_for_stanza(target_file)
    annotated_document =nlp(source_doc)
    
    # export GSD annotations to file
    output_file = target_file.replace('6022','7000')
    #output_file = target_file.replace(".conllu", "-updated.conllu")
    write_stanza_annotations(annotated_document, output_file)
    #return annotated_document


## depparse_done = Z, X
## depparse_en_cours:Y
folder = "//Users/Data/PREFAB/wiki/"
target_files = sorted(glob.glob(folder + "*6022*.conllu"))
#target_filesG1 = sorted(glob.glob(folder + "temp/" + "*.conllu"))
#target_filesG2 = sorted(glob.glob(folder + 'wiki/' + "*.conllu"))
#target_files = target_filesG1 + target_filesG2

## runner with error_reporting
## TO DO : promote thisto a funciton, called on CLI invoke
error_report = []
nlp = stanza.Pipeline(lang='fr', processors='depparse', depparse_pretagged=True, logging_level = "Info",  download_method=None))
for target_file in tqdm(target_files):
    try:
        run_stanza_depparse_120(target_file, nlp)
        print(f"Depparsing folder >> {os.path.basename(target_file)} <<")
    except ValueError as e:
        # Extract the sentence number and error message if there is a ValueError :value errors can appear if processing has yielded empty tokens or non-sequential token_ids ; this us usually due to multiple spaces in strings, especially imported data from Excel
        error_message = str(e)
        match = re.search(r'sentence (\d+): (.+)', error_message)
        if match:
            sentence_number = match.group(1)
            specific_message = match.group(2)
            error_report.append((target_file, sentence_number, specific_message))
            print(str(target_file) + "::" + str(sentence_number) + "::" + str(specific_message) + "\n")
        else:
            # If the specific pattern is not found, capture the whole error message
            error_report.append((target_file, 'Unknown', error_message))
    
    except Exception as e:
        # Capture any other type of exception
        error_type = type(e).__name__
        error_message = str(e)
        error_report.append((target_file, 'Unknown', f"{error_type}: {error_message}"))
    continue
if len(error_report) >0:
    print(error_report)	