import platform, io, os, stanza, conllu, re, glob
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from tqdm import tqdm


def import_prepare_for_stanza(input_file):
    '''
    take a conllu file and prepare for annotation with stanza, export
    Args : 
        input_file : path to file to tag
    Returns:
        source_doc : stanza document
        all_sentsout : list of sentences to pass to stanza
    outputfile : filename for stanza export
    '''
    source_doc = CoNLL.conll2doc(input_file)

    return source_doc

def write_stanza_annotations_from_retokenisation(annotated_document, output_file):
    '''
    write stanza document as conllu file for PREFAB sent_tests
    Args:
    document_out : Stanza document object containing annotations produced by stanza pipeline
    output_file : conllu file to be created in which annotations will be saved
    Returns: no return object 
    '''
    #these_tokens = 0
    with io.open(output_file, 'w', encoding='UTF-8') as f:
        for sentence in annotated_document.sentences:
            meta_id = sentence.comments[0]
            meta_text_raw = sentence.comments[1]
            meta_text_prefab = sentence.comments[2]
            metadata_chunk = "\n" + "\n".join([chunk for chunk in [meta_id, meta_text_raw, meta_text_prefab]]) +"\n"
            f.write(metadata_chunk)

            for token in sentence.tokens:
                my_string = str(token.to_conll_text() + "\n")
                f.write(my_string)
                #these_tokens+=1
	#   return these_tokens
#



my_path = "/home/username/PREFAB/wiki_final/"


###############################################################################################################################
########################################## setup for gsd  ############################################################
###############################################################################################################################
##
##nlp = stanza.Pipeline(lang='fr', package = 'gsd', processors='tokenize,pos', tokenize_pretokenized=True, tokenize_no_ssplit=True, logging_level = "Info", download_method=None)
##
##input_files = sorted(glob.glob(my_path + "*104.conllu"))
##
##for f, input_file  in tqdm(enumerate (input_files)):
##    source_doc = import_prepare_for_stanza(input_file)
##
##    annotated_document =nlp(source_doc)
##
##    #for sent in annotated_document.sentences:
##    #    for token in sent.tokens:
##    #        print(token.to_conll_text())
##    # export GSD annotations to file
##
##    output_file = input_file.replace('104.conllu','120.conllu')#.replace('tranche4/004','tranche4/120')
##    write_stanza_annotations_from_retokenisation(annotated_document, output_file)
##   # print(f'Processed file {f} of {len(input_files)}')
##
##del(nlp)
###
#############################################################################################################################
######################################## setup for parisstories  ############################################################
#############################################################################################################################
config = {
        # Comma-separated list of processors to use
	'processors': 'tokenize,mwt,pos',
        # Language code for the language to build the Pipeline in
        'lang': 'fr',
        # Processor-specific arguments are set with keys "{processor_name}_{argument_name}"
        # You only need model paths if you have a specific model outside of stanza_resources
	'tokenize_model_path': '/home/username/stanza_resources/fr/tokenize/parisstories.pt',
	'pos_model_path': '/home/username/stanza_resources/fr/pos/parisstories_charlm.pt',
	'pos_pretrain_path': '/home/username/stanza_resources/fr/pretrain/conll17.pt',
    'pos_forward_charlm_path': '/home/username/stanza_resources/fr/forward_charlm/newswiki.pt', 
    'pos_backward_charlm_path': '/home/username/stanza_resources/fr/backward_charlm/newswiki.pt',
        # Use pretokenized text as input and disable tokenization
  'tokenize_pretokenized': True, 'tokenize_no_ssplit': True}

nlp = stanza.Pipeline(**config) # Initialize the pipeline using a configuration dict


input_files = sorted(glob.glob(my_path + "*104.conllu"))

for f, input_file  in tqdm(enumerate (input_files)):
    source_doc = import_prepare_for_stanza(input_file)

    annotated_document =nlp(source_doc)

    output_file = input_file.replace('104.conllu','121.conllu')#.replace('tranche4/004','tranche4/121')
    write_stanza_annotations_from_retokenisation(annotated_document, output_file)

del(nlp, config)
#############################################################################################################################
######################################## setup for partut  ############################################################
#############################################################################################################################

nlp = stanza.Pipeline(lang='fr', package = 'partut', processors='tokenize,pos', tokenize_pretokenized=True, tokenize_no_ssplit=True, logging_level = "Info", download_method=None)

input_files = sorted(glob.glob(my_path + "*104.conllu"))

for f, input_file  in tqdm(enumerate (input_files)):
    source_doc = import_prepare_for_stanza(input_file)

    annotated_document =nlp(source_doc)

    output_file = input_file.replace('104.conllu','122.conllu')
    write_stanza_annotations_from_retokenisation(annotated_document, output_file)


del(nlp)
#############################################################################################################################
######################################## setup for sequoia  ############################################################
#############################################################################################################################


nlp = stanza.Pipeline(lang='fr', package = 'sequoia', processors='tokenize,pos', tokenize_pretokenized=True, tokenize_no_ssplit=True, logging_level = "Info", download_method=None)
input_files = sorted(glob.glob(my_path + "*104.conllu"))

for f, input_file  in tqdm(enumerate (input_files)):
    source_doc = import_prepare_for_stanza(input_file)
    annotated_document =nlp(source_doc)
    output_file = input_file.replace('104.conllu','123.conllu')
    write_stanza_annotations_from_retokenisation(annotated_document, output_file)



