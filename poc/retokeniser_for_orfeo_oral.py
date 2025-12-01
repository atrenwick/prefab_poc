def import_parse_prefab_connl_custom_global(input_file):
  """import and parse connlu file to list of lists
  Args:
    input_file: path to a connlu file
  Returns:
    parsed_data : data parsed from the connlu, as list of lists
  """
  global_string = '''# global.columns = ID FORM LEMMA XPOS UPOS FEATS HEAD DEPREL NONE1 NONE2 TIME1 TIME2 SPEAKER\n'''
  with open(input_file, "r", encoding='UTF-8') as f:
      conllu_data = f.read()
  conllu_data = global_string + conllu_data  
  parsed_data = conllu.parse(conllu_data)
  
  return parsed_data

def tidy_forme_apost(polylex):
    '''
    change different types of apostrophe into single straight apostrophe
    '''
    forme_array = np.array(polylex['Forme'].values)
    tidy_array = []
    for string in forme_array:
        new_string = string.replace("’","'")
        new_string = string.replace("'","'")
        new_string = string.replace("’","'")
        tidy_array.append(new_string)
    return tidy_array

def load_polylex_latest(plx_filepath):
  '''
  load specified version of PLX data, PLX sheet to prepare PLX retokenisation
  args:
  input: plx_filepath :
      full path to PLX file to take as input, with first sheet as PLX info
  returns :
      polylex: array of polylexForm column before duplicates dropped
      listplx : array of polylex Form column, duplicates dropped
      plx_df : pd df of LEM, Forme, Ambig, POS, traits, gram_type
  Notes on errors : if a FORME cell is empty, numpy will throw an error float does't have a .replace() method, so look for empty Forme entries in excel.
      
  '''
  # importer le fichier, et prendre les colonnes LEM, Forme, ambig, POS, traits 
  # polylex = pd.read_excel(f'{PathHead}/Dropbox/ANR_PREFAB/Data/Corpus_annotations/Polylexicaux/Liste_polylexicaux/2024-02-07-polylexicaux.xlsx')
  polylex = pd.read_excel(plx_filepath)
  polylex = polylex[['LEM', 'Forme', 'AMBIGtidy', 'POS_UD_def', 'traits_UD_def']]
  polylex = polylex.loc[polylex["AMBIGtidy"] !="O"]
  #polylex = polylex.loc[polylex["POS_UD_def"] !="PROPN"]
  polylex = polylex.reset_index(drop=True)
  # créer la liste des PLE sans doublon, puis rétablir l'index
  tidy_array = tidy_forme_apost(polylex)
  polylex['Forme'] = tidy_array.copy()
  listplx = polylex['Forme'].drop_duplicates()
  listplx = listplx.reset_index(drop=True)
  

  # ajouter le nombre de mots (en ajoutant 1 au nombre d'espaces)
  gram_type = [str(listplx[i]).count(" ")+1 for i in range(len(listplx))]
  
  # faire un dataframe de ces listes 
  plx_df = pd.concat([pd.DataFrame(listplx), pd.DataFrame(gram_type)], axis=1)
  plx_df.columns = ['plx', 'gram_type']
  
  return (polylex, listplx, plx_df)

def make_ngram_lists(plx_df):
    '''
    make named lists of ngrams for values of n of 1 to 7, using list comprehensions of arrays created from input df
    inputs :
        plx_df : df of polylexicals, with 2 columns, forme and gram_type
            note:/help: if the input file or df has rows with blanks, these can be rendered as floats, and cause errors/crash with re.sub(), so check for blanks
    returns :
        named lists for all values of 1,2,3,4,5,6,7
    
    '''
    heptagrams = plx_df.loc[plx_df['gram_type']==7]
    hexagrams = plx_df.loc[plx_df['gram_type']==6]
    quingrams = plx_df.loc[plx_df['gram_type']==5]
    quadgrams = plx_df.loc[plx_df['gram_type']==4]
    trigrams = plx_df.loc[plx_df['gram_type']==3]
    bigrams = plx_df.loc[plx_df['gram_type']==2]
    unigrams = plx_df.loc[plx_df['gram_type']==1]
    gramsets = [heptagrams, hexagrams, quingrams, quadgrams, trigrams, bigrams, unigrams]
    gramset_names = ['heptagrams', 'hexagrams', 'quingrams', 'quadgrams', 'trigrams', 'bigrams', 'unigrams']
    
    return gramsets, gramset_names

# uptodatest
def prepare_sent(s):
    '''
    Prepare sentences for retokenisation. Specific processing for each text type based on 'wiki', 'oral', or "roman_" in input_file_path.
    Note that for romans, underscore is required to avoid issues with files and folders containing 'roman'
    Specific processing is limited to collapsing multiple spaces to single space, removng spaces between integers over 1000 and the word "aujourd'hui"
    Processing is much more detailed for roman_ corpus, as OCR error are also corrected here, as well as tokenisation workarounds for Lexicoscope ("d_autres", ,etc)
    Args : 
        input:
        conll_file : full path to conllu file to be processed.
    Returns :
        sent : sentence to be processed
        id_string : unique sentence ID from metadata
        meta_text_raw : original tokenisation of sentence
        
    '''
    # 7.1 get sentence from df
    if 'wiki' in conll_file:
        if 'text_raw' in all_target_data[s].metadata.keys():
            sent = all_target_data[s].metadata['text_raw']
        else:
            sent = '_'
        meta_text_raw = "\n#text_raw = " + sent
        # sent_id  for conllu meta line
        id_string = all_target_data[s].metadata['sent_id']

        # consolidate numbers : remove spaces from integers
        int_regex = r" ([0-9]+) ([0-9]+) "
        int_regex_repl = r" \1\2 "

        sent = re.sub(int_regex, int_regex_repl,sent)
        sent = re.sub("(  )+"," ", sent)
        sent = re.sub("aujourd' hui", "aujourd'hui",sent)
        sent = re.sub("aujourd ' hui", "aujourd'hui",sent)
        sent = re.sub("\t", " ",sent)

    if 'cefc' in conll_file or 'CEFC'in conll_file:
        if 'text' in all_target_data[s].metadata.keys():
            sent = all_target_data[s].metadata['text']
        else:
            sent = '_'
        meta_text_raw = "\n#text_raw = " + sent
        id_string = all_target_data[s].metadata['sent_id']

        # consolidate numbers : remove spaces from integers
        int_regex = r" ([0-9]+) ([0-9]+) "
        int_regex_repl = r" \1\2 "

        sent = re.sub(int_regex, int_regex_repl,sent)
        sent = re.sub("aujourd' hui", "aujourd'hui",sent)
        sent = re.sub("aujourd ' hui", "aujourd'hui",sent)
        sent = re.sub("\t", " ",sent)
        sent = re.sub("  ", " ",sent)

        regexC1 = r"([dsjltmncDSJLTMNC]) '$"
        replC1  = r"\1'"
        regexC2 = r"Qu '"
        replC2  = r"Qu'"
        regexC3 = r"qu '"
        replC3  = r"qu'"
        sent = re.sub(regexC1, replC1, sent)
        sent = re.sub(regexC2, replC2, sent)
        sent = re.sub(regexC3, replC3, sent)

        
    if 'oral' in conll_file or 'Oral'in conll_file:
        if 'text' in all_target_data[s].metadata.keys():
            sent = all_target_data[s].metadata['text']
        else:
            sent = '_'
        sent = all_target_data[s].metadata['text']
        meta_text_raw = "\n#text_raw = " + sent
        id_string = all_target_data[s].metadata['sent_id']

        # consolidate numbers : remove spaces from integers
        int_regex = r" ([0-9]+) ([0-9]+) "
        int_regex_repl = r" \1\2 "

        sent = re.sub(int_regex, int_regex_repl,sent)
        sent = re.sub("aujourd' hui", "aujourd'hui",sent)
        sent = re.sub("aujourd ' hui", "aujourd'hui",sent)
        sent = re.sub("\t", " ",sent)
        sent = re.sub("  ", " ",sent)
        
    if 'roman_' in conll_file:
        # use s to get current_sent == everything related to sentence
        current_sentence = all_target_data[s]
        # get teh raw text, send to sent
        #sent = all_target_data[s][1].metadata['sent_text_raw']
        if 'text_raw' in all_target_data[s].metadata.keys():
            sent = all_target_data[s].metadata['text_raw']
        else:
            sent = '_'
        # prepare raw text for conllu meta line
        meta_text_raw = "\n#text_raw = " + sent
        # sent_id  for conllu meta line
        id_string = "\n#sent_id = " + str(all_target_data[s].metadata['sent_id'])

        # print raw tokenisation as text
        #print(f"Sent = {sent}")

        # consolidate numbers : remove spaces from integers
        int_regex = r" ([0-9]+) ([0-9]+) "
        int_regex_repl = r" \1\2 "

        sent = re.sub(int_regex, int_regex_repl,sent)
        #7.1 Do not tweak elisions. can correct OCR errors here as well as scories
        
        sent = re.sub(" d u ", " du ", sent)
        sent = re.sub(" a u ", " au ", sent)
        sent = re.sub("^D u ", "Du ", sent)
        sent = re.sub("^A u ", "Au ", sent)
        sent = re.sub(" D u ", " Du ", sent)
        sent = re.sub(" A u ", " Au ", sent)
        sent = re.sub("^d u ", "du ", sent)
        sent = re.sub("^a u ", "au ", sent)
        
        sent = re.sub("aujourd'_hui", "aujourd'hui", sent)
        sent = re.sub("Aujourd'_hui", "Aujourd'hui", sent)
        sent = re.sub("Jusqu'_à", "Jusqu'à", sent)
        sent = re.sub("jusqu'_à", "jusqu'à", sent)
        sent = re.sub(" a ux ", " aux ", sent)
        sent = re.sub(" d es ", " des ", sent)
        sent = re.sub(" A ux ", " Aux ", sent)
        sent = re.sub(" D es ", " Des ", sent)
        sent = re.sub("d'_autres", "d'autres", sent)
        sent = re.sub("D'_autres", "D'autres", sent)
        sent = re.sub("d'_autre", "d'autre", sent)
        sent = re.sub("D'_autre", "D'autre", sent)

        sent = re.sub("d'_ailleurs", "d'ailleurs", sent)
        sent = re.sub("D'_ailleurs", "D'ailleurs", sent)
        sent = re.sub("d'_où", "d'où", sent)
        sent = re.sub("D'_où", "D'où", sent)
        sent = re.sub("Jusqu'_au", "Jusqu'au", sent)
        sent = re.sub("jusqu'_au", "jusqu'au", sent)
        sent = re.sub("Jusqu'_aux", "Jusqu'aux", sent)
        sent = re.sub("jusqu'_aux", "jusqu'aux", sent)

        sent = re.sub("quelqu'_un", "quelqu'un", sent)
        sent = re.sub("Quelqu'_un", "Quelqu'un", sent)
        sent = re.sub("quelqu'_une", "quelqu'une", sent)
        sent = re.sub("Quelqu'_une", "Quelqu'une", sent)
        sent = re.sub("d'_abord", "d'abord", sent)
        sent = re.sub("D'_abord", "D'abord", sent)

    return(sent, id_string, meta_text_raw)

# uptodatest
def find_matches(sent, nlp):
    # 7.2 create document, run matcher, initialize variables
    ## note on sentences :: sentences from lexicoscope in XML have the sequence LETTER ELISION_APOSTROPHE SPACE LETTER
    ## if sent into pipeline this way, ELISION_APOSTROPHE is rendered as its own token. this is the case with both nlp=French() AND nlp= spacy.blank("fr")
    ## correct results are achieved when teh SPACE immediately after ELISION_APOSTROPHE is removed : the tokeniser seems to expect these apostrophes to have LETTER on either side, and renders the elided form, with its APOSTROPHE, as a token of form {LETTER + ELISION_APOSTROPHE}
    # this however breaks when the matcher becomes involved : therefore no tweaking here, as can post-process to send apostrophes back to their elided word
    '''
    Find matches in a sent. If matches are present, processes in exact mode only, searching for matches with diacritics. If no matches are present, fuzzy mode is activated and sentences are searched for matches based on u_matcher, ie unaccented matcher, which replaced all accented/diacritic bearing characters with unacented versions.  
    Args :
        input : 
            sent : sentence to be processed
            nlp : nlp object to be used when calling the matcher
        Returns :
        matches : list of matches (match id, start, end)
        doc : document object containing sent
        match_condition : whether exact or fuzzy matching was used
        
    '''
    doc = nlp(sent)
    matches = matcher(doc)
    match_condition = "exact"
    if len(matches) ==0:
        matches = u_matcher(doc)
        match_condition = "fuzzy"
    return matches, doc, match_condition

# uptodatest
def get_spans_for_matchesv1(matches, doc):
    '''
    take the doc and matches and extract the spans ie chars in target ranges
    args : 
        matches : list of matches created by matcher
        doc : doc object containing sentence
    Returns : spans :
        list of spans corresponding to the matches
        Note : these aren't strings, only spans : they're still of spacy token class
    '''
    spans = []
    for match_id, start, end in matches:
      this_span = doc[start:end]
     # print(this_span.__str__())
      spans.append(this_span)
    return spans

# test to get start end as well
#def get_spans_for_matches(matches, doc):
#    spans, starts, ends = [], [], []
#    for match_id, start, end in matches:
#        this_span = doc[start:end]
#        spans.append(this_span)
#        starts.append(start)
#        ends.append(end)
#    return (spans, starts, ends)


# uptodatest
def get_annotations_for_filtered_spans(filtered_spans, match_condition):
    '''
    take a list of filtered spans (filtered to select longest match) and their match conditions to extract annotations
    
    This function calls  get_plx_POS, and at this stage extracts the actual string, severing links to spacy object, env, with np.where, annotations are extracted by get_plx_POS, PLX value is set depending on match_condition
    
    Args :
        filtered_spans : list of spans filtered by the filter_spans funciton
        match_condition : whether fuzzy or exact matching gave the current set of spans
    Returns :
        tidy_data : list of tuples of annotations  

    Help ::if this function throws an error that no item at 0,0, it means that the PLX is not in the list

    '''
    tidy_data = []
    for filtered_span in filtered_spans:
        POS, LEM, TRAITS = get_plx_POS(filtered_span.__str__(), match_condition)
        if TRAITS is np.nan:
            TRAITS = "_"
        if match_condition == "exact":
            PLX = "PLX"
        if match_condition == "fuzzy":
            PLX = "PLZ"
        data_row = filtered_span, filtered_span.start, filtered_span.end, POS, LEM, TRAITS, PLX
        tidy_data.append(data_row)
    return tidy_data

# uptodatest
def run_retokeniser(tidy_data, doc):
    '''
    run the retokeniser to merge tokens and annotations to specific values
        Args :
            tidy_data : list of tuples of annotations and spans to operate over
            doc : document object containing sentence
        Returns :
            doc object with new tokenisation
    '''
    
    with doc.retokenize() as retokenizer:
        for t in range(len(tidy_data)):
          span = tidy_data[t][0]
          POS = tidy_data[t][1]
          LEM = tidy_data[t][2]
          TRAITS = tidy_data[t][3]
          PLX = tidy_data[t][4]
          retokenizer.merge(span, attrs={"LEMMA": LEM, "POS":POS, "MORPH":TRAITS, "TAG":PLX})
    return (doc)

# uptodatest
def make_metastrings(doc, meta_text_raw, id_string):
    '''
    Make metadata strings for sentence making metastring with new tokenisation, with double / and space as word boundary, and concatenates this with existing metastrings 
    Args :
        doc : doc containing retokenised sentence
        meta_text_raw : text with original tokenisation, spaces indicating word boundaries, extracted from input conllu
        id_string : string_id extracted from input conllu
    Returns :
        meta_strings:
            String with linebreaks separating metadata strings for ID, original tokenisation and prefab retokenisation, ready for writing to conllu
    '''
    #7.6 combine meta strings made previously with new meta line of retokenised text post-polylexicals but before post-correction
    #meta_text_prefab = "\n#text_prefab = " + "".join([((str('NomPropre')) + "// ") if str(token.__str__()) in anon_list else ((str(token)) + "// ")  for token in doc]) + "\n"
    meta_text_prefab = "\n#text_prefab = " + "".join([  ((str(token)) + "// ")  for token in doc]) + "\n"
    meta_strings = id_string  + meta_text_raw + meta_text_prefab
    return meta_strings

# uptodatest
def create_conllu_strings(doc):
    '''
    Make conllu strings for each token in a document

    Args : 
        doc : document object containing retokenised sentence
        Note : this operates with the remaps list imported from Polylex_latest	to allow further customisation
    Returns:
        sent_data : list of strings, each string representing a token and its annotations in conllu format, the list representing a whole sentence
    '''
# 7.7 enumerate over tokens to make list of conllu output
    sent_data = []
    for i, token in enumerate(doc):
        string = make_conll_string_from_token(i, token, doc, remaps) 
        #print(string)
        sent_data.append(string)
    return sent_data

def get_plx_POS(token, match_condition):
    '''
    function to get POS, LEM, traits for PLEs found, using np where in polylex_df
    '''
    if match_condition == "exact":
    
        if token in polylex.Forme.values:
            target_row_idx = np.where(polylex.Forme == token)[0][0]

        if token not in polylex.Forme.values:
            token_sub = token.replace(" - ","-").replace('- ', '-').replace(' -', '-')
            # notes here for cell top
            if token_sub in polylex.Forme.values:
                target_row_idx = np.where(polylex.Forme == token_sub)[0][0]
      
            if token_sub not in polylex.Forme.values:
                token_sub_dot = token.replace(" .",".").replace('. ', '.').replace(' . ', '.')
                if token_sub_dot in polylex.Forme.values:
                    target_row_idx = np.where(polylex.Forme == token_sub_dot)[0][0]

                if token_sub_dot not in polylex.Forme.values:
                  token_sub_dot1 = token_sub_dot.replace("' ","'").replace('. ', '.').replace(' . ', '.')
                  if token_sub_dot1 in polylex.Forme.values:
                      target_row_idx = np.where(polylex.Forme == token_sub_dot1)[0][0]
                  if token_sub_dot1 not in polylex.Forme.values:
                      token_sub_dot1b = token_sub_dot.replace("' ","'").replace('. ', '.').replace(' . ', '.').replace(" qu '"," qu'")
                      if token_sub_dot1b in polylex.Forme.values:
                          target_row_idx = np.where(polylex.Forme == token_sub_dot1b)[0][0]
                      if token_sub_dot1 not in polylex.Forme.values:
                          print(f"Error at sent == {conll_file}: {s} : {token} has no match")

        target_row = polylex.loc[target_row_idx]
        POS = target_row[3]
        LEM = target_row[0]
        TRAITS = target_row[4]
        
    if match_condition == "fuzzy":
    
        if token in u_forms:
            target_row_idx = np.where(u_forms ==token)[0][0]

        if token not in u_forms:
            token_sub = token.replace(" - ","-").replace('- ', '-').replace(' -', '-')
            # notes here for cell top
            if token_sub in u_forms:
                target_row_idx = np.where(u_forms == token_sub)[0][0]
      
            if token_sub not in u_forms:
                token_sub_dot = token.replace(" .",".").replace('. ', '.').replace(' . ', '.')
                if token_sub_dot in u_forms:
                    target_row_idx = np.where(u_forms == token_sub_dot)[0][0]
                if token_sub_dot not in u_forms.values:
                    token_sub_dot1 = token_sub_dot.replace("' ","'").replace('. ', '.').replace(' . ', '.')
                    if token_sub_dot1 in polylex.Forme.values:
                        target_row_idx = np.where(polylex.Forme == token_sub_dot1)[0][0]
                    if token_sub_dot1 not in polylex.Forme.values:
                    	print(f"Error at sent == {conll_file}: {s} : {token} has no match")



#                    print(f"Error at sent == {conll_file}: {s} : {token} has no match")

        target_row = polylex.loc[target_row_idx]
        POS = target_row[3]
        LEM = target_row[0]
        TRAITS = target_row[4]
    
    return POS, LEM, TRAITS

def get_forme(token):
    '''
    get a token and, get string rather than hash, tidy, then check if in anon_list, the list of codes for anonymised values. 
    If yes, set tthe token to NomPropre. Lemma and POS have been set to this value with retokrniser, but they don't change the token itself other than tidying it
    This changes the token in the conllu lines. metalines function needs to change the metaline for prefab
    '''
    form_raw = token.__str__()
    form_raw = form_raw.replace("\' ","\'") # idem
    form_raw = form_raw.replace("- ","-") # does this break things for oral when fixin them for wiki
    form = form_raw.replace(" -","-") # idem
#    if form in anon_list:
#        form = 'NomPropre'
    return form

def make_conll_string_from_token(i, token, doc, remaps):
    '''
    make conllu string from token based on i, token and doc
    '''

    idx = str(i+1)
    form = get_forme(token)
    lemma = get_lemma(token)
    POS = get_POS(token)
    traits = get_traits(token)
    PLX = getPLXvalue(token)
    # chunk to force POS, LEM, traits for specfic lexical items
    if form in remaps.Forme.values:
        remap_index = np.where(remaps.Forme.values == form)[0][0]
        remap_data = remaps.loc[remap_index]
        POS = remap_data[2]
        lemma = remap_data[1]
        traits = remap_data[3]
        PLX = '<plx=\"PLR\">' + token.tag_
        #print(POS, lemma, traits)
    
    string = str(idx) + "\t" + str(form) + "\t" + str(lemma) + "\t_\t" + str(POS) + "\t" + str(traits) +  "\t_\t_\t_\t" + str(PLX) + "\n"
    return string

def get_lemma(token):
    '''
    Get lemma either by setting to _ if none, or from token.lemma as string
    Args : 
        token : token of sentence currently being analysed
    Returns :
        lemma : lemma as string
    '''
    if token.lemma_ == "":
        lemma = "_"
    else :
        lemma = token.lemma_
    return lemma

def getPLXvalue(token):
    '''
    Get PLXvalue either by setting to _ if none, or from token.tag_, where the retokeniser stored it as string
    Args : 
        token : token of sentence currently being analysed
    Returns :
        PLX : PLXvalue as string
    '''
    if token.tag_ == "":
        PLX = "_"
    else :
        PLX = token.tag_
    return PLX

def get_POS(token):
    '''
    Get POS either by setting to _ if none, or from token.pos_ as string
    Args : 
        token : token of sentence currently being analysed
    Returns :
        POS : POS as string
    '''
    if token.pos_ == "":
        POS = "_"
    else :
        POS = token.pos_
    return POS

def get_traits(token):
    '''
    Get traits either by setting to _ if none, or from token.morph as string
    Args : 
        token : token of sentence currently being analysed
    Returns :
        traits : traits as string
    '''
    if len(token.morph) ==0:
        traits = "_"
    else: 
        traits = str(token.morph)
    return traits

def get_sentence_details(mode, data_in):
    '''
    non-recursive version of get_sentence_details, looking -
        - only in the target_folder specified
        - only for items with conllu as extension within this folder
       
    Arguments :
        mode : specify, either 'list' or 'folder'
            if list, data_in must be a list of full_paths to files to process, list goes straight to files, substituting .fr.xml to conllu
            if folder, data_in must be path to a folder, and this full path will be passed to glob and all conllu files processed
        data_in : one of  
            list of full paths of files to process (mode == list) 
            folder : path to folder
            
    '''
    if mode == "list":
        files = [re.sub('.xml$','-xml.conllu', file) for file in data_in]
    if mode == "folder":
        target_folder = data_in
        files = glob.glob( target_folder + "*conllu")
    file_overviews = []
    sent_details = []
    print(f'Found {len(files)} files to process…')
    for text_file_id, file_fullpath in enumerate(files):
        file_data = import_parse_prefab_connl(file_fullpath)
        target_folder = os.path.dirname(file_fullpath)
        file_short = re.sub(target_folder, "", file_fullpath)
        #file_short = re.sub(" ", "_", file_short)
        overview = file_short, len(file_data) # get number of sents
        file_overviews.append(overview)  # send to list
        if (text_file_id ) %10 ==0 and text_file_id != 0:
            print(f"Processed {text_file_id} files")
        for i, sentence in enumerate(file_data):
            sent_info = file_short, i, len(sentence)
            sent_details.append(sent_info)
            if len(sent_details) % 1e4 ==0:
                print(f"Processed  {len(sent_details)} sents " )

    file_overview = pd.DataFrame(file_overviews, columns=['filename','sent.Count'])
    sent_details_df = pd.DataFrame(sent_details, columns=['filename','sent.Number', 'numberTokens'])
    print(f"Done : processed {len(sent_details_df)} sents in {len(files)} files !")                                                          
    return file_overview, sent_details_df

def make_temp_data(meta_strings, sent_data, id_string): # this needs debugging with oral cproa
    # 7.8 specify savelocation, ,export
    current_sent_temp = []
    if id_string.startswith("\n#sent_id") == False:
        sent_id_special = str("\n#sent_id = " + meta_strings )
        current_sent_temp.append(sent_id_special)

    if id_string.startswith("\n#sent_id") == True:
        current_sent_temp.append(meta_strings)

    for string in sent_data:
        current_sent_temp.append(string)
    
    current_sent_temp = "".join(current_sent_temp)
    return current_sent_temp

def retokenisation_postproc_on_string(current_sent_temp):
    '''
    This function takes in conllu output based on the retokenisation of polylexical units based on the Prefab Polylexical list.
    The input contains correctly  tokenised polylexical units, but does not recognise apostrophes marking elision as belonging to the word containing the elision. Rather, these apostrophes are analysed as being their own independent tokens.
    This function uses a series of regular expressions to correct this, and reattach each elision to the correct word, and remove newly-redundant lines in the conll, correct the indexationof conll lines, as well as correct the modified tokenisation in the meta line text_prefab.
    This is done in two parts. The first part, operating on `data`, modifies the data in the numbered lines of the conllu output.
    This `data` is then split into lines, which are iterated over, applying regex1 and 2 to apply the same changes to the metadata line #text_prefab.
    Finally, iterating over lines, each line is printed to output_file. Regex0 then finds lines that contain data, splits the line index number from the remainder of the line, and inserts a new sequence of continuous numbers to take account of deleted lines.
    NOTE : on error, the text "moo" will be printed to the console, to alert that something went awry as the line met none of the 4 conditions. Line number and the text of the line are printed
    Args :  current_sent_temp
                : current sent as string
    Returns : processed_sentence : string of all sentence and annotations in complete state
    
    takes in conllu file and iterates line by line to attribute consecutive indices to tokens in INDEX column
    '''

    # regex0 finds lines that start with 1 or more integers followed by a tab, to find non-meta, non-blank lines
    # regex1 searches #text_prefab looking for a single token in the list dsjltmn, followed by two / followed by a space then an apostrophe. this finds cases where 1-token words have been split from elisions marking apostrophes, which form their own subsequent token
    # repl1 replaces the first group found in regex1, i.e. the letter found between brackets = dsjltmnc with both this character and an apostrophe
    # regex2 and repl2 are variants of regex1 and repl1, but search for the specific sequence of qu, rather than any single element of the list, replacing this with qu'
    # regex3 operates on the numbered lines of conll output. At this stage, no annotations are present for elided tokens, so any 1 character from the list in [] if found when followed by the sequence of tabs, underscores, linebreak integer tab, elision, tabs, underscores and linebreak that represent an elided form without apostrophe, the remainder of the line, as well as the totality of the next line if this contains only an apostrophe after the line index number
    # repl3 combines the elided form with the apostrophe, and completes the line with the appropriate sequence of tabs and underscores. This results in the complete destruction of line n+1, leaving non-consecutive line index numbers.
    # regex4 and repl4 do the same as regex3 and repl3, but with the qu sequence rather than a single character, and replace this again with a single line, completely destroying the subsequent line.
    regex0 = r"^\d+\t"
    # operate on sent as string
    regex1 = r" ([dsjltmncDSJLTMNC])// '//"
    repl_1 = r" \1'//"
    regex2 = r" (qu|Qu)// '//"
    repl_2 = r" \1'//"
    regex5 = r" (.+?qu)// '//"
    repl_5 = r" \1'//"

    regex7 = r"// '// "
    repl_7 = r"'// "
    regexY = r"// -// y//"
    replY = r"// -y//"
    regexB = r" (.+?)-(moi|toi|soi|lui|leur|je|tu|il|elle|elles|on|ca|ça|nous|vous|ils|elles|en|y|le|la|les|là)//"
    replB = r" \1 // -\2//"
        
    
    # operate on sent as conllu
    regex3 = r"\t([dsjltmncDSJLTMNC])\t_\t_\t_\t_\t_\t_\t_\t(_.+?)\n(\d+)\t'\t_\t_\t_\t_\t_\t_\t_\t_\n"
    repl3  = r"\t\1'\t_\t_\t_\t_\t_\t_\t_\t_\2\n"
    regex4 = r"\t(qu|Qu)\t_\t_\t_\t_\t_\t_\t_\t(_.+?)\n(\d+)\t'\t_\t_\t_\t_\t_\t_\t_\t_\n"
    repl4  = r"\t\1'\t_\t_\t_\t_\t_\t_\t_\t_\2\n"
    
    regex6 = r"\t(.+?qu)\t_\t_\t_\t_\t_\t_\t_\t(_.+?)\n(\d+)\t'\t_\t_\t_\t_\t_\t_\t_\t_\n"
    repl6  = r"\t\1'\t_\t_\t_\t_\t_\t_\t_\t_\2\n"

    regex8 = r"\t(.+?)\t_\t_\t_\t_\t_\t_\t_\t(_.+?)\n(\d+)\t'\t_\t_\t_\t_\t_\t_\t_\t_\n"
    repl8  = r"\t\1'\t_\t_\t_\t_\t_\t_\t_\t_\2\n"
    
    regex9 = r"\t-\t_\t_\t_\t_\t_\t_\t_\t(_.+?)\n(\d+)\ty\t_\t_\t_\t_\t_\t_\t_\t_\n"
    repl9 =  r"\t-y\t_\t_\t_\t_\t_\t_\t_\t_\2\n"


    regexA = r"(.+?)-(moi|toi|soi|lui|leur|je|tu|il|elle|elles|on|ca|ça|nous|vous|ils|elles|en|y|le|la|les|là)\t_\t_\t_\t_\t_\t_\t_\t(.+?)\n"
    replA =  r"\1\t_\t_\t_\t_\t_\t_\t_\t\3\n2\t-\2\t_\t_\t_\t_\t_\t_\t_\t\3\n"

    data = current_sent_temp
    data = re.sub(regex3, repl3, data)
    data = re.sub(regex4, repl4, data)
    data = re.sub(regex6, repl6, data)
    data = re.sub(regex8, repl8, data)
    data = re.sub(regex9, repl9, data)
    data = re.sub(regexA, replA, data)
    
    lines = data.split("\n")
    lines = [(line + "\n") for line in lines ]
    # Process each line in the list
    processed_lines = []
    for z, line in enumerate(lines): # add l, enumerate, and keep track of moo lines
        # if starts with  # is metadata, so don't change ; if blank, keep blank
        if line == "\n" : 
            processed_lines.append(line)
            token_index =0

        if line.startswith("#sent_id"): 
            processed_lines.append(line)
            token_index =0

        if line.startswith("#text_raw") : 
            processed_lines.append(line)
            token_index =0
            
        if line.startswith("#text_prefab"):
            prefab_meta = line 
            prefab_meta = re.sub(regex1, repl_1, prefab_meta)   
            prefab_meta = re.sub(regex2, repl_2, prefab_meta)   
            prefab_meta = re.sub(regex5, repl_5, prefab_meta)   
            prefab_meta = re.sub(regex7, repl_7, prefab_meta)   
            prefab_meta = re.sub(regexY, replY, prefab_meta)   
            prefab_meta = re.sub(regexB, replB, prefab_meta)   
            processed_lines.append(prefab_meta)
            token_index =0
        
        # if the line starts with an integer followed by a tab, we've got a number, so split this from the line
        # then add a new index from 1, and add that to head of line, then send to processed_lines
        if  re.match(regex0, line):
            token_line = re.sub(regex0, "", line)
            token_index +=1
            tidy_line = str(token_index) + "\t" + token_line
            processed_lines.append(tidy_line)
            
    
    processed_sentence = processed_lines 
    return processed_sentence

# take in doc made from decl of tokens, lemmas as consolidation of extras and noise to keep depparse analyser quiet.
def find_doc_matches(doc):
    # 7.2 take in doc document, run matcher, initialize variables
    ## note on sentences :: sentences from lexicoscope in XML have the sequence LETTER ELISION_APOSTROPHE SPACE LETTER
    ## if sent into pipeline this way, ELISION_APOSTROPHE is rendered as its own token. this is the case with both nlp=French() AND nlp= spacy.blank("fr")
    ## correct results are achieved when teh SPACE immediately after ELISION_APOSTROPHE is removed : the tokeniser seems to expect these apostrophes to have LETTER on either side, and renders the elided form, with its APOSTROPHE, as a token of form {LETTER + ELISION_APOSTROPHE}
    # this however breaks when the matcher becomes involved : therefore no tweaking here, as can post-process to send apostrophes back to their elided word
    '''
    Find matches in a sent. If matches are present, processes in exact mode only, searching for matches with diacritics. If no matches are present, fuzzy mode is activated and sentences are searched for matches based on u_matcher, ie unaccented matcher, which replaced all accented/diacritic bearing characters with unacented versions.  
    Args :
        input : 
            doc_in : doc to be processed
            nlp : nlp object to be used when calling the matcher
        Returns :
        matches : list of matches (match id, start, end)
        doc : document object containing sent
        match_condition : whether exact or fuzzy matching was used
        
    '''
    matches = matcher(doc)
    match_condition = "exact"
    if len(matches) ==0:
        matches = u_matcher(doc)
        match_condition = "fuzzy"
    return matches, doc, match_condition

def run_retokeniser_on_oral(tidy_data, doc):
    '''
    run the retokeniser to merge tokens and annotations to specific values
        Args :
            tidy_data : list of tuples of annotations and spans to operate over
            doc : document object containing sentence
        Returns :
            doc object with new tokenisation
    '''
    
    with doc.retokenize() as retokenizer:
        for t in range(len(tidy_data)):
          span = tidy_data[t][0]
          span_spart = tidy_data[t][1] 
          span_end = tidy_data[t][2]
          new_tag_full = compact_extra_annots(doc, span_spart, span_end)
          POS = tidy_data[t][3]
          LEM = tidy_data[t][4]
          TRAITS = tidy_data[t][5]
          PLX = tidy_data[t][6]
          tag_full = '<plx=\"' + PLX + '\">'+ new_tag_full
          retokenizer.merge(span, attrs={"LEMMA": LEM, "POS":POS, "MORPH":TRAITS, "TAG":tag_full})
    return (doc)

def compact_extra_annots(made_doc, span_start, span_end):

    target = made_doc[span_start:span_end]
    extra_data = [token.tag_ for token in target]

    # t1 is t1 in 0th line, t2 is -1th line
    t1out = re.sub(r'<t1=\"(.+?)\">.+$', r'\1', extra_data[0])
    t2out = re.sub(r'.+?<t2=\"(.+?)\">.+$', r'\1', extra_data[-1])
    spks = list(set([re.sub(r'.+?<spk=\"(.+?)\">.+$', r'\1', extra_data[a]) for a in range(len(extra_data))]))[0]

    extralist ='<t1=\"' + str(t1out) + '\"><t2=\"' + str(t2out) + '\"><spk=\"' + str(spks) + '\">' 

    return extralist

# Part 6 : Preparing to find matches :: special imports
import spacy, conllu, re, platform, unidecode, glob
import pandas as pd
import numpy as np
from spacy.util import filter_spans
from spacy.tokenizer import Tokenizer
from spacy.matcher import PhraseMatcher
from spacy.lang.fr import French
from spacy.tokens import Doc
from tqdm import tqdm
# declarations : chemins, etc
if platform.system() == "Darwin":
  PathHead = "/Users/Adam/Library/CloudStorage/Dropbox/ANR_PREFAB/"
elif platform.system() == "Windows":
  PathHead = "C:/Users/renwicka/Dropbox/ANR_PREFAB/"
elif platform.system() == "Linux":
  PathHead = "C:/Users/nom_utilisateur/"

plx_folderpath= 'Data/Corpus_annotations/Polylexicaux/Liste_polylexicaux/'

plx_filepath = f"{PathHead}{plx_folderpath}2024-04-08-polylexicaux.xlsx"

# 6.1 instantiate nlp object and matchers
#nlp=French()
nlp_m = spacy.blank("fr")
matcher = PhraseMatcher(nlp_m.vocab)
u_matcher = PhraseMatcher(nlp_m.vocab)


# 6.2 load plx file, create lists of plx, and add patterns to matcher in desc. order of length
plx_filepath = f"{PathHead}{plx_folderpath}2024-04-08-polylexicaux.xlsx"
polylex, listplx, plx_df = load_polylex_latest(plx_filepath)
gramsets, gramset_names = make_ngram_lists(plx_df)
u_forms = np.array([unidecode.unidecode(polylex.Forme[i]) for i in range(len(polylex.Forme))])


for gramset, gramset_name in zip(gramsets, gramset_names):
    # make for strict matcher = matching with diacritics
    patterns = [nlp_m.make_doc(polylex) for polylex in gramset.plx.values]
    matcher.add(gramset_name, patterns)
    # make for fuzzy matcher = matching without any diacritics
    u_patterns = [nlp_m.make_doc(unidecode.unidecode(polylex)) for polylex in gramset.plx.values]
    u_matcher.add(gramset_name, u_patterns)    



remaps = pd.read_excel(plx_filepath, "remap")
#anon_data = pd.read_excel(plx_filepath, "anon_list")  
#anon_list = anon_data['Value'].values

#current_sentence = all_target_data[23]

# consolidate numbers : remove spaces from integers
#int_regex = r" ([0-9]+) ([0-9]+) "
#int_regex_repl = r" \1\2 "#

#sent = re.sub(int_regex, int_regex_repl,sent)
#sent = re.sub("aujourd' hui", "aujourd'hui",sent)
#sent = re.sub("aujourd ' hui", "aujourd'hui",sent)
#sent = re.sub("\t", " ",sent)
#sent = re.sub("  ", " ",sent)
#print(sent)


#def make_t_string(token):
#    t_id = str(token['id'])
#    t_form = str(token['form']) 
#    t_lem = str(token['lemma'])
#    t_pos = str('X')
#    t_xpos = str('X')
#    t_feats = str('_')
#    t_head = str(token['head'])
#    t_deprel = str(token['deprel'])
#    t_extras = str('_')
#    t_none = str('_')
#    items = t_id, t_form, t_lem, t_pos, t_xpos, t_feats, t_head, t_deprel, t_extras, 
#    t_string = "\t".join([item for item in items]) + '\n'
#    return t_string

#def redist_custom_attrs(made_doc):
#    
#    # redistribute custom attributes
#    for token in made_doc:
#        extras_raw = token.lemma_
#
#        extras_tidy = extras_raw.replace('><','>/<').split('/')
#        t1_data = str(extras_tidy[0].replace('<t1=\"','').replace('\">',''))
#        t2_data = str(extras_tidy[1].replace('<t2=\"','').replace('\">',''))
#        spk_data = str(extras_tidy[2].replace('<spk=\"','').replace('\">',''))
#        cloc_data = str(extras_tidy[3].replace('<cloc=\"','').replace('\">',''))
#        lex_data = str(extras_tidy[4].replace('<lex=\"','').replace('\">',''))
#        sup_data = str(extras_tidy[5].replace('<sup=\"','').replace('\">',''))
#
#        token._.PF_time1 = t1_data
#        token._.PF_time2 = t2_data
#        token._.PF_speaker = spk_data
#        token._.PF_changeloc = cloc_data
#        token._.PF_lex =lex_data
#        token._.PF_sup =sup_data
#    
#    return made_doc


#conll_file= f'/Users/Adam/Library/CloudStorage/Dropbox/ANR_PREFAB/Data/Corpus/Oral/TCOF/TCOF-100/TCOF-reunionelus_del_10-100.tsv'
#conll_file = '/Volumes/AlphaFour/ANR_PREFAB/processing_romans_tranches/trancheW/004/lemmatised/HS62/HS62-003.conllu'
#conll_file = '/Users/Adam/Library/CloudStorage/Dropbox/ANR_PREFAB/Data/Corpus/Oral/TCOF/TCOF-100/TCOF-1crossfit_mao_15-100.tsv'
#conll_file = 'C:/Users/renwicka/Dropbox/ANR_PREFAB/Data/toy/testfile_oral-test.conllu'
#conll_file = 'C:/Users/renwicka/Dropbox/ANR_PREFAB/Data/toy/testfile_oral-test.conllu'
#testname = 'C:/Users/renwicka/Dropbox/ANR_PREFAB/Data/toy/testfile_oral-testOUTPUT.conllu'

global_line ="# global.columns = ID FORM LEMMA XPOS UPOS FEATS HEAD DEPREL EXTRAS NONE2"
desired_keys = ['id','form','lemma','upos','xpos','feats', 'head','deprel','none1','none2']  # Specific keys to retrieve
#						 /Volumes/AlphaFour/ANR_PREFAB/CorpusPREFAB/Oral/TCOF-100/001/TCOF-1crossfit_mao_15-100.tsv
corpus_chunks = ['FLEURON','FON','OFROM']
for corpus_chunk in corpus_chunks:
	print(f"Working on {corpus_chunk}")
	input_files = glob.glob(f"/Volumes/AlphaFour/ANR_PREFAB/CorpusPREFAB/Oral_def/ORFEO/{corpus_chunk}-100/001/*.orfeo")
	for input_file in tqdm(input_files):
		conll_file = input_file
		outpfile = conll_file.replace('/001/','/004/').replace('.orfeo','104.conllu')
		# load 100 state file
		#conll_file = '/Users/Adam/Library/CloudStorage/Dropbox/ANR_PREFAB/Data/Corpus/Oral/ESLO/ESLO-100/ESLO2_ENT_1016-100.tsv'
		#outpfile = '/Users/Adam/Desktop/test_outputtest4.conllu'
		
		#testname = "/Users/Adam/Desktop/test_output.conllu"
		parsed_data = import_parse_prefab_connl_custom_global(conll_file)
		all_target_data = parsed_data
		
		
		# compress extra cols into col8
		# sent extras to list, then insert list into colNone1
		completed_sentences = []
		outputstrings = []
		
		
		
		
		# special function to take extra cols, XMLwrap and send to LEMM column where spacy is expecting them
		for s in range(len(all_target_data)):
		    if s ==0:
		        outputstrings.append(global_line)
		        #print(global_line)
		
		#    metastrings = "".join([(f'\n# {key} = '+  all_target_data[s].metadata[key]) for key in ["sent_id","text"] ] + ['\n'])
		#    outputstrings.append(metastrings)
		    #print(metastrings)
		
		    extralist = ['<t1=\"' + str(token['time1']) + '\"><t2=\"' + str(token['time2']) + '\"><spk=\"' + str(token['speaker']) + '\">' for token in all_target_data[s]]
		    #print(extralist)
		    for t, token in enumerate(all_target_data[s]):
		        #token['lemma'] = extralist[t]
		        token['none1'] = extralist[t]
		
		
		
		# make special doc from current sent
		for s in range(len(all_target_data)):
		    current_sent = all_target_data[s]
		    words = [token['form'] for token in current_sent]
		    spaces = [True for x in range(len (words)-1)] + ['False']
		    #lemmas = ["_" for token in current_sent]
		    misc = [ token['none1'] for token in current_sent]
		    sent_starts = [False for word in words ]
		    sent_starts[0] = True
		    made_doc = Doc(nlp_m.vocab, words=words, spaces=spaces, sent_starts = sent_starts, tags=misc)
		    meta_text_raw = "#text_raw = " + " ".join([word for word in words])
		    id_string = all_target_data[s].metadata['sent_id'] + "\n"
		
		    matches, made_doc, match_condition = find_doc_matches(made_doc)
		    spans = get_spans_for_matchesv1(matches, made_doc)
		    filtered_spans = filter_spans(spans)
		    tidy_data = get_annotations_for_filtered_spans(filtered_spans, match_condition)
		    made_doc_retok = run_retokeniser_on_oral(tidy_data, made_doc)
		    doc = made_doc_retok
		    meta_strings = make_metastrings(doc, meta_text_raw, id_string)
		    sent_data = create_conllu_strings(doc)
		
		    current_sent_temp = make_temp_data(meta_strings, sent_data, id_string)
		    processed_sentence = retokenisation_postproc_on_string(current_sent_temp)
		    completed_sentences.append(processed_sentence)
		    
		
		
		
		
		with open (outpfile, 'w', encoding="UTF-8") as u:
		        for proc_sentence in completed_sentences:
		            for line in proc_sentence:
		                u.write(line)
		
		
