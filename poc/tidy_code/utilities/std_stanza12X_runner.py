## script to run several stanza pipelines for a single input, to get multiple analyses using configurations with different treebanks
import platform, io, os, stanza, conllu, re, glob
from stanza.utils.conll import CoNLL
from conllu import parse, parse_tree
from tqdm import tqdm


def import_prepare_for_stanza(input_file):
    '''
    take a conllu file and return a stanza doc object
    Args : 
        input_file : path to conllu file to convert to doc object for processing
    Returns:
        source_doc : stanza document
    
    '''
    source_doc = CoNLL.conll2doc(input_file)

    return source_doc

def write_stanza_annotations_from_retokenisation(annotated_document, output_file):
    '''
    write stanza document as conllu file
    Args:
    document_out : Stanza document object containing annotations produced by stanza pipeline
    output_file : conllu file to be created in which annotations will be written
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


# specify a path in which to work
my_path = "//Users/Data/PREFAB/wiki/"


#############################################################################################################################
######################################## setup for gsd  ############################################################
#############################################################################################################################
# setup for stanza specifying language, package, processors, pretokenized as True to avoid retokenisation, tokenize_no_ssplit as true to avoid resegmentation of sentences, logging info and download method
nlp = stanza.Pipeline(lang='fr', package = 'gsd', processors='tokenize,pos', tokenize_pretokenized=True, tokenize_no_ssplit=True, logging_level = "Info", download_method=None)

# take specified path and generate list of files
input_files = sorted(glob.glob(my_path + "*104.conllu"))

for f, input_file  in tqdm(enumerate (input_files)):
    source_doc = import_prepare_for_stanza(input_file)

    annotated_document =nlp(source_doc)

    #for sent in annotated_document.sentences:
    #    for token in sent.tokens:
    #        print(token.to_conll_text())
    # export GSD annotations to file

    output_file = input_file.replace('104.conllu','120.conllu')#.replace('tranche4/004','tranche4/120')
    write_stanza_annotations_from_retokenisation(annotated_document, output_file)
   # print(f'Processed file {f} of {len(input_files)}')

del(nlp)
#
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
	'tokenize_model_path': '//Users/Data/stanza_resources/fr/tokenize/parisstories.pt',
	'pos_model_path': '//Users/Data/stanza_resources/fr/pos/parisstories_charlm.pt',
	'pos_pretrain_path': '//Users/Data/stanza_resources/fr/pretrain/conll17.pt',
    'pos_forward_charlm_path': '//Users/Data/stanza_resources/fr/forward_charlm/newswiki.pt', 
    'pos_backward_charlm_path': '//Users/Data/stanza_resources/fr/backward_charlm/newswiki.pt',
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




