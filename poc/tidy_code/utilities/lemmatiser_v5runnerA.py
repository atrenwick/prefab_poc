##lemmatiserV3
# note that v6 is in early beta ; A suffix here is non-meaningful
# imports
import pandas as pd
import numpy as np
from stanza.utils.conll import CoNLL
import json, time, platform, glob, os, re, string
from tqdm import tqdm

if platform.system() == "Darwin":
  PathHead = "/Users/Data/ANR_PREFAB/"
elif platform.system() == "Windows":
  PathHead = "C:/Users/Data/ANR_PREFAB/"
elif platform.system() == "Linux":
  PathHead = "C:/Users/nom_utilisateur/"

# loaders
## loaders :: a 'preloader' for individual files for each analyser, which is called by the loader for each analyser, running over the model codes
def hops_preload(i, hops_file, hops_model):
    ''' 
    take int, hops_file and hops_model and remove redundant columns, empty columns and add model_code to colnames
    int is used to determine whether TokenID and Forme cols are to be retained (yes if int ==0, else no, to avoid redundant repetitions)
    Args:
        i: integer from enumeration over list of hops_models.
        hops_file: file of annotations from hops
          hops_file is loaded with tab as separator, and no header. quoting is set to 3, to ensure that various quotation marks opened in 1 token are recognised properly
        hops_model : code of hopsmodel
    Returns:
        hops_preloaded : pd df of hops annotations without redundant/empty cols
    '''
    hops_names_temp = ["TokenID", "Forme", "UUID", "POS"]#, "blank1", "blank2", "Head", "DEPrel", "blank3", "blank4"]
    hops_preloaded = pd.read_csv(hops_file, sep="\t", header=None, quoting = 3, usecols = [0,1,2,3])
    hops_preloaded.columns = hops_names_temp
    hops_model = str(hops_model)
    if i ==0:
        #hops_preloaded = hops_preloaded.drop(["blank1", "blank2", "blank3", "blank4", "Head", "DEPrel"], axis=1)
        names_real = [name + hops_model for name in hops_preloaded.columns]
        names_real[0], names_real[1], names_real[2] = "TokenID","Forme","UUID"
        hops_preloaded.columns = names_real
    else:
        # hops_preloaded = hops_preloaded.drop(["TokenID", "Forme", "blank1", "blank2", "blank3", "blank4","Head", "DEPrel"], axis=1)
        hops_preloaded = hops_preloaded.drop(["TokenID", "Forme"], axis=1)
        names_real = [name + hops_model for name in hops_preloaded.columns]
        names_real[0] = "UUID"
        hops_preloaded.columns = names_real
        
    return hops_preloaded
def load_all_HOPS(path, corpus, ref_file):
    '''
    take path, corpus, and ref file and enumerate over list of codes of HOPSmodels.
    the input corpus, and whether it is ROM or not is a remnant of older version, used to discriminate the structure of filenames ; it is always set as ROM by the lemmatizer
    loop over files and use the preloader to load each file, then merge on UUID to yield a df of all HOPS parses
    '''
    hops_models = [5262, 5272, 5282, 5287, 5292, 5297]
    for i, hops_model in enumerate(hops_models):
        if corpus != 'ROM':
            hops_file = f'{path}{corpus}-{hops_model}/{ref_file}-h6310-{str(hops_model)}.csv'
        if corpus == 'ROM':
            hops_file = f'{path}{ref_file}-{str(hops_model)}.csv'

        hops_preloaded = hops_preload(i, hops_file, hops_model)
        if i ==0:
            hops_all = hops_preloaded
        else:
            hops_all = pd.merge(hops_all, hops_preloaded, how="left", on="UUID")
    
    hops_all = change_dtypes(hops_all)
    return hops_all

def load_udpipe(i, udpipe_file, udpipe_model):
    '''
    take int, udpipe_file and udpipe_model and remove redundant columns, empty columns and add model_code to colnames
    Args:
        i: integer from enumeration over list of udpipe_models.
        hops_file: file of annotations from udpipe
        hops_model : code of udpipe_model
    Returns:
        udpipe_df : pd df of udpipe annotations without redundant/empty cols
    '''
    conll_names_temp = ["TokenID", "Forme", "LEM", "POS", "blank1", "FEAT", "Head", "DEPrel", "blank3", "isPLX", "sent_id"]
    current_udpipe_data = CoNLL.conll2doc(udpipe_file)
    all_rows = []
    # iterate over tokens, with iter_tokens, getting conll text and adding sentID from token.sent.comments item 0
    for token in current_udpipe_data.iter_tokens():
      sent_id_current = token.sent.comments[0]
      string = token.to_conll_text() + "\t" + str(sent_id_current) 
      all_rows.append(string)

    # send to df with 1 column named temp, then split on tab in this named column, expanding to add new cols 
    udpipe_df = pd.DataFrame(data = all_rows, columns = ['temp'])
    udpipe_df = udpipe_df['temp'].str.split('\t', expand=True)
    # assign temporary names
    udpipe_df.columns = conll_names_temp
    udpipe_model = str(udpipe_model)
    
    # remove named columns depending on whether df is first (and thus corresponds to 301/311) in series to be added
    if i ==0:
        udpipe_df = udpipe_df.drop(["blank1",  "blank3", "LEM", "Head", "DEPrel"], axis=1)
        names_real = [name + udpipe_model for name in udpipe_df.columns]
        names_real[0], names_real[1] = "TokenID","Forme"
        udpipe_df.columns = names_real
    else:
        udpipe_df = udpipe_df.drop(["TokenID", "Forme", "blank1",  "blank3","LEM", "Head", "DEPrel"], axis=1)
        
        names_real = [name + udpipe_model for name in udpipe_df.columns]
        udpipe_df.columns = names_real
        
    return udpipe_df
def load_all_UDPipe(path, corpus, ref_file):
    '''
    iterate over the UDPipe inputfiles, calling the udpipe loader
    '''
    udpipe_models = [311, 312, 313, 314, 315]
    for i, udpipe_model in enumerate(udpipe_models):
        if corpus != 'ROM':
            udpipe_json = f'{path}{corpus}-{udpipe_model}/{ref_file}-101-{str(udpipe_model)}.json'
        if corpus == 'ROM':
            udpipe_conllu = f'{path}{ref_file}-{str(udpipe_model)}.conllu'

        udpipe_df = load_udpipe(i, udpipe_conllu, udpipe_model)
        if i ==0:
            udpipe_all = udpipe_df
        else:
            udpipe_all = pd.concat([udpipe_all, udpipe_df], axis=1)
    udpipe_all = change_dtypes(udpipe_all)    
    return udpipe_all

def load_stanza(i, stanza_file, stanza_model):
    '''
    take int, stanza_file and stanza_model and remove redundant columns, empty columns and add model_code to colnames
    Args:
        i: integer from enumeration over list of stanza_models.
        stanza_file: file of annotations from stanza
        stanza_model : code of stanza_model
    Returns:
        stanza_df : pd df of stanza annotations without redundant/empty cols
    '''
    conll_names_temp = ["TokenID", "Forme", "LEM", "POS", "blank1", "FEAT", "Head", "DEPrel", "blank3", "isPLX", "sent_id"]
    current_stanza_data = CoNLL.conll2doc(stanza_file)
    all_rows = []
    for token in current_stanza_data.iter_tokens():
      sent_id_current = token.sent.comments[0]
      string = token.to_conll_text() + "\t" + str(sent_id_current) 
      all_rows.append(string)

    stanza_df = pd.DataFrame(data = all_rows, columns = ['temp'])
    stanza_df = stanza_df['temp'].str.split('\t', expand=True)

    stanza_df.columns = conll_names_temp
    stanza_model = str(stanza_model)
    
    if i ==0:
        stanza_df = stanza_df.drop(["blank1",  "blank3", "LEM", "Head", "DEPrel"], axis=1)
        names_real = [name + stanza_model for name in stanza_df.columns]
        names_real[0], names_real[1] = "TokenID","Forme"
        stanza_df.columns = names_real
    else:
        stanza_df = stanza_df.drop(["TokenID", "Forme", "blank1",  "blank3","LEM", "Head", "DEPrel"], axis=1)
        
        names_real = [name + stanza_model for name in stanza_df.columns]
        stanza_df.columns = names_real
        
    return stanza_df
def load_all_stanza(path, corpus, stanza_files_base):
    '''
    iterate over the Stanza inputfiles, calling the stanza loader
    '''
    stanza_models = [120, 121, 122, 123]
    for i, stanza_model in enumerate(stanza_models):
        if corpus != 'ROM':
            stanza_file = f'{path}{corpus}-{stanza_model}/{ref_file}-{str(stanza_model)}.conllu'
        if corpus == 'ROM':
            stanza_file = f'{path}/{ref_file}-{str(stanza_model)}.conllu'
        stanza_df = load_stanza(i, stanza_file, stanza_model)
        if i ==0:
            stanza_all = stanza_df
        else:
            stanza_all = pd.concat([stanza_all, stanza_df], axis=1)
    
    stanza_all = change_dtypes(stanza_all)
    
    return stanza_all

def load_all_data(path, corpus, ref_file):
    '''
    call each specific loader and thereby the preloaders to load all parses
    Args : 
      path : path to folder on which to operate
      corpus : holder for ROM value which determines structure of filenames
      ref_file : filename of reference file to process
    Returns:
      list of pd dfs with all parses aligned
    '''
    hops_all = load_all_HOPS(path, corpus, ref_file)    
    udpipe_all = load_all_UDPipe(path, corpus, ref_file)
    stanza_all = load_all_stanza(path, corpus, ref_file)
    return hops_all, udpipe_all, stanza_all

# functions## once data is loaded, do comparing and recommending

def change_dtypes(df):
    '''
    change dtypes of columns with HEAD annotations from object to int.
    Args : 
        df : pd df with annotations
    Returns:
        df : pd df with annotations and head
    Note : might need to add errors='coerce'??
    '''
    col_list = [col for col in df.columns if 'Head' in col ]

    for col in col_list:
        if df[col].dtype == 'object':
            try:
                # Convert to numeric dtype
                df[col] = pd.to_numeric(df[col])
            except:
                pass  # Handle exceptions if conversion fails

    # Change dtype to int64
    df[col_list] = df[col_list].astype('int64')
    return df
def compare_id_columns(df1, df2, df3):
    '''
    Run comparisons on the ID columns ie the 0th conll column to ensure correct alignment
    Args : 
      Inputs : df1, df2, df3 : pd dfs `hops_all`, `udpipe_all`, `stanza_all`, `returned by load_all_data`
    Returns :
      One of three statements depending on whether there is a datatype error, the dfs have different lengths or are identical
    Note : the return statement is useful in manually running the script and for finding bugs/errors ; when run in the whole pipeline, the message is not returned, but an error message "ERROR for" and the name of the folder being processed is returned ; this is controlled by an IF statement testing the length of the HOPS_df, Stanza df and UDPipedf. This doesn't catch the dtype errors, but none have yet been attested in development or usage
    '''
  
    # Get the `TokenID` columns only, and coerce to numeric, then convert to numpy array
    df1_id = pd.to_numeric(df1['TokenID'], errors='coerce')
    df2_id = pd.to_numeric(df2['TokenID'], errors='coerce')
    df3_id = pd.to_numeric(df2['TokenID'], errors='coerce')

    df1_id = df1_id.to_numpy()
    df2_id = df2_id.to_numpy()
    df3_id = df3_id.to_numpy()
    
    # Check if the data types of the ID columns are the same
    if not np.array_equal(df1_id.dtype, df2_id.dtype) or not np.array_equal(df1_id.dtype, df3_id.dtype):
        return 'DataFrames have different data types forTokenID columns.'

    # Check if the ID columns have the same shape
    if df1_id.shape != df2_id.shape or df1_id.shape != df3_id.shape:
        return 'DataFrames have different lengths.'

    # Compare the contents of the ID columns
    if not np.array_equal(df1_id, df2_id) or not np.array_equal(df1_id, df3_id):
        return 'The contents of some `TokenID` columns are different.'

    return 'All `TokenID` values are identical.'
  
def compare_and_tidy(udpipe_all, stanza_all, hops_all):
    '''
    Compare the sent_id values for each df, comparing stanza inputs amongst themselves, UDPipe inputs among themselves. If there is a perfect match, redundant values are dropped, with the sentenceID from 311 and 120 being preserved
    This funciton prints a message that appears duing a normal run, confirming that data has been successfully aligned and is now being compared and tidied
    After redundant sentID columns are removed, columns are renamed and reordered, and all dfs concatenated into a single df with named columns
    Args :
        udpipe_all, stanza_all, hops_all : pd dfs of annotations passed from `compare_id_columns`
    Returns : all_data : pd df of all recommendations, aligned,  with tidied colnames
    
    '''
    # 2.5 tidy udpipe internal sent values
    # compare sent_id values to ensure all same : value should be same 1 all through df
    result = pd.DataFrame([1 if (udpipe_all.iloc[i,5] == udpipe_all.iloc[i,10] == udpipe_all.iloc[i,15] == udpipe_all.iloc[i,20]== udpipe_all.iloc[i,25]) else 0 for i in range(len(udpipe_all))]).value_counts()
    if result.iloc[0] == len(udpipe_all):
        udpipe_all.drop(['sent_id312', 'sent_id313', 'sent_id314', 'sent_id315'], axis=1, inplace=True)
        #print("Success : udpipe's sent_id values all match internally")
    print("Comparing, tidying…")
    # 2.6 tidy stanza internal sent values
    # compare sent_id values to ensure all same : value should be same 1 all through df
    result = pd.DataFrame([1 if (stanza_all.iloc[i,5] == stanza_all.iloc[i,9] == stanza_all.iloc[i,13] == stanza_all.iloc[i,17]) else 0 for i in range(len(stanza_all))]).value_counts()
    if result.iloc[0] == len(stanza_all):
        stanza_all.drop(['sent_id121', 'sent_id122', 'sent_id123'], axis=1, inplace=True)
 #       print("Success : Stanza's sent_id values all match")

    # 2.9 : get columns in specified order, rename, then send to all_data
    # restrict columns to specified values then concatenate
    stanza_all = stanza_all[['POS120','POS121', 'POS122','POS123', 'FEAT120','FEAT121', 'FEAT122','FEAT123','isPLX123', 'sent_id120']]
    stanza_all.columns = ['POS120','POS121', 'POS122','POS123', 'FEAT120','FEAT121', 'FEAT122','FEAT123','isPLX123', 'sentID']

    # rename in place, then reorder
    udpipe_all.columns = ['TokenID', 'Forme', 'POS311', 'FEAT311', 'isPLX311', 'sent_id311','POS312', 'FEAT312', 'isPLX312', 'POS313', 'FEAT313', 'isPLX313', 'POS314', 'FEAT314', 'isPLX314', 'POS315', 'FEAT315', 'isPLX315']
    udpipe_all = udpipe_all[['POS311', 'POS312', 'POS313', 'POS314', 'POS315', 'FEAT311', 'FEAT312', 'FEAT313', 'FEAT314', 'FEAT315']]

    hops_all= hops_all[['UUID', 'TokenID', 'Forme', 'POS5262', 'POS5272', 'POS5282', 'POS5287', 'POS5292', 'POS5297']]

    all_data = pd.concat([hops_all, stanza_all, udpipe_all], axis=1)

    return all_data
  
def make_POS_recommendations(df):
    '''
    count occurrences of unique values in rows and summarise to yield 3 columns : index, recommendation, score_breakdown
    Args:
    df : pd df with annotations, with cols 0,1,2 ignored as containing UUID, TokenID, forme
    Returns:
        reccs : pd df of 5 columns of UUID, sentID, index, recommendation, repartition 
    '''
    row_results = []
    corrige = ""
    these_cols = df.columns[4:-1]
    # check if word head in colname and coerce to int to force col as int not str??
    print("Making  recommendations…")
    for index, row in df.iterrows():
        slice = row[these_cols].value_counts()
        formatted_strings = []
        best = slice.index[0]
        token = str(row['Forme'])
        UUID = str(row['UUID'])
        sentID = str(row['sentID'])
        TokenID = str(row['TokenID'])

        row_result = [UUID, sentID, TokenID, token, best]
        row_results.append(row_result)    
    
    reccs = pd.DataFrame(data = row_results, columns = ['UUID','sentID','TokenID','Forme','recc'])
       
    return reccs

# update DATA
def update_dataV2(data_df, reccs_df):
    '''
    merge dfs of data and recommandations on UUID col
    Args : 
        data_df : df of DATA ie all annotations
        reccs_df : df of recommendations produced by `run_recommenders`    
    Returns : data_df, modified df with recommendation + répartition columns added. reccs df not modified.
    '''
    data_df = pd.merge(reccs_df.drop(['TokenID', 'Forme', "sentID"], axis=1), data_df, how="right", on = "UUID")
    data_df = data_df[['UUID',  'sentID', 'TokenID', 'Forme','recc', 'POS5262', 'POS5272',  'POS5282', 'POS5287', 'POS5292', 'POS5297', 'POS120', 'POS121', 'POS122', 'POS123', 'POS311', 'POS312', 'POS313', 'POS314', 'POS315',  'isPLX123']]
    data_df.columns = ['UUID',  'sentID', 'TokenID', 'Forme','recc', 'POS5262', 'POS5272',  'POS5282', 'POS5287', 'POS5292', 'POS5297', 'POS120', 'POS121', 'POS122', 'POS123', 'POS311', 'POS312', 'POS313', 'POS314', 'POS315',  'isPLX']
    return data_df


def get_feats(array, target):
    '''
    helper function to get values of feats, but returning _ if value is None or NaN or nan
    Args :
      array : np array of feats
      taget : index value in the array to get
    Returns : 
      feats : value of feats, either as the string from the df or an underscore if None or nan
    '''
    feats = array[target][0]
    if str(feats) =="nan":
        feats = "_"
    return feats

def restrict_and_reccPOS(all_data):
    '''
    Function to take all_data and rename colums with listcomprehension them run the POS recommender then the function to bind the data and the recommendations.
    Args :
        input: all_data : pd df of all consolidated data
        Returns : pos_update : pd df with updated POS values from recommendations
    '''
    print("Restricting and tidying………")
    # 2.10 : tidy columns, column names
    target_col = all_data.columns.get_loc('sentID')
    all_data['sentID'] = pd.DataFrame([all_data.iloc[a, target_col].replace('#sent_id = ', '') for a in range(len(all_data))])

    # ad dcol to determine row shading
    #all_data['sentGroup'] = pd.DataFrame(['A' if int(all_data.iloc[a, target_col]) % 2 == 0 else 'B' for a in range(len(all_data))])
    # make lists of col names with list comprehensions
    POS_cols = ['UUID', 'sentID', 'TokenID', 'Forme'] + [col for col in all_data.columns if 'POS' in col] + [ 'isPLX123']

    #3.1 restrict ourselves to POS data, make recommendations, update df with recommendations
    POS_data = all_data[POS_cols]
    pos_reccs = make_POS_recommendations(POS_data)
    pos_update = update_dataV2(POS_data, pos_reccs)
    return pos_update
def apply_corrections(pos_update):
    '''
    Function to apply specific corrections to the POS recommendations based on custom rules
    Args:
      input : pos_update : pd df of updated POS+recommendations. From POS_update, arrays are extracted and operated on
      Returns :pos_reccs, formes, PLXtag : arrays of pos_reccs, formes and PLX tags after corrections made
    dans le fonction : 
        x compte combien de corrections on apporte
        forme from formes = pos_update col formes values as array
        u = UPOS tag
        r = recommendation
        tag = PLXtag
    '''
    #3.2 Make arrays for faster matching
    formes = pos_update['Forme'].values
    POS5262 = pos_update['POS5262'].values
    POS5272 = pos_update['POS5282'].values
    POS312 = pos_update['POS312'].values
    reccs = pos_update['recc'].values
    PLXtag = pos_update['isPLX'].values

    ## Here we can apply rules based on the POS tags given by specific parsers : analysis of outputs showed that tagger 302 was better than others at identifying when QUOI and BON, as formes, were INTJ or ADJ or PRON than the recommendation. 
    # We can thus find cases where recommendation goes against this analysis of this specific parser and replace the recommended value with the POS302 value
    # 3.3 : prefer POS302 analysis when form = quoi, bon, as is better at distinguishing PRON/INTJ, ADJ/INTJ
    # prefer analysis from 302 for quoi, bon

    update = []
    x =0
    for forme, u, r ,tag in zip(formes,  POS312, reccs, PLXtag):
        if forme in ['quoi', 'bon'] : # if forme is in list, set r to udpipe value
            #print(u, r) 
          update.append(u)
          # use oral flag to add PLXtag value to existing data rather then overwrite
          if oral_flag == "T":
            current_misc_value = PLXtag[x]
            new_misc_value = '<plx="PLM">' + current_misc_value
            PLXtag[x] = new_misc_value
          else :
            PLXtag[x] = "PLM"
        else:
            update.append(r)
        #print(update)
        x +=1
    # send to array as reccs
    pos_reccs = np.array(update)
    
    return pos_reccs, formes, PLXtag

def run_post_lemm_corrections(formes, lemmas_all, pos_update, reccs):
    '''
    Run POS_LEM corrections that are based on POS tags and lemma found : this for the specific case of differentiating allerAUX from aller VERBLEXICAL
    Args : 
      Inputs : formes, lemmas_all, pos_update, reccs : np arrays of formes, lemmas_all, pos_update and reccs
      Returns : reccs : np array of updated recommendations 
    j is strictly necessary
    '''
    #4.4.1 remap lemma "aller" to analysis 5262
    # parser 5262 was noted to be better than recommendation at distinguishing AUX and VERB uses of aller : we'll thus use this lemma to update the POS. 
    # given that the conjugations of aller are in both the AUX and VERB lists for the lookups, the initial error won't have caused incorrect lemmas to have been attributed:
    POS5262 = pos_update['POS5262'].values

    # prefer analysis from 302 for quoi, bon
    update = []
    j = 0
    for forme, lemma, p, r in zip(formes, lemmas_all, POS5262, reccs):
        if lemma == "aller" : # if forme is in list, set r to udpipe value
            #print(forme, lemma, p, r) 
            update.append(p)
            # add PLStag J to existing col10 value if oral corpus = T
            if oral_flag == "T":
              current_misc_value = PLXtag[j]
              new_misc_value = '<plx="PLA">' + current_misc_value
              PLXtag[j] = new_misc_value
            else :
              PLXtag[j] = "PLA"

        else :
            update.append(r)
        j+=1
    #send to array as reccs
    reccs = np.array(update)    
    return reccs

def update_features(pos_reccs, lemmas_all, all_data, feats_all, PLXtag):
  '''
  Get GSD features as array and add these to feats_all if feats is blank and POS is the same as the GSD_pos
  Args : 
      Inputs : pos_reccs, lemmas_all, all_data, feats_all, PLXtag : np arrays of values
      Returns : feats_all : updated np array of feats
  '''
  # get gsd feats as array
  feats_all =  list(feats_all)
  feat_gsd = list(all_data['FEAT120'])
  pos_gsd = all_data['POS120'].values
  for i in range(len(feats_all)):
    # if we doon'thave any feat and POS agrres with gsd, get gsd tag
    if pos_gsd[i] == pos_reccs[i]:
      if PLXtag[i] == "_" :
        feats_all[i] = feat_gsd[i]
  return feats_all

def rebuild_for_conllu(ref_file_full):
    '''
    initial steps to convert the np arrays back to a conllu output
    Note : numbers is remnant from earlier version; but helps maintain clarity more than anything else
    Args :  input : ref_file_full : absolute path to reference file being operated on, so output name can be created
            Returns : sent_id, tokenids, metastrings : np array, nparray and list of ids, ids and metastrings
    '''
    # 5.1 designate input file for stanza to get metas…
    ref_data = CoNLL.conll2doc(ref_file_full)
    # 5.2 get metadata from strings and make arrays…
    ## get sent ids and send to metastrings list
    meta_strings = []
    for s, sentence in enumerate(ref_data.sentences):
            sent_id = sentence.comments[0].replace("#sent_id = ","")
            meta_strings.append(sentence.comments)
    # get together all the info needed :
    # create arrays for additional data :

    sent_id = np.array(pos_update['sentID'])
    tokenids = np.array(pos_update['TokenID'])
    # make list of arrays
    return sent_id, tokenids, meta_strings

def export_reinjected_annotations(output_file, arrays, meta_strings):
    '''
    Take arrays rebuild conllu lines, adding metastrings from list, and write to .conllu
    export corrected df to conllu, XPOS and feats in correct order now
    Note : current version has all cols in correct order, previous version has Feats and XPOS inverted
    Note2 : order of arrays doesn't seem to have any special significance : correct order made with string concatenation
    Args : input : 
              output_file
              arrays : list of arrays of annotations in order formes, feats, reccs, lemmas, PLXtag, sent_id, tokenIDs
              meta_strings : list of sent_ids
           returns: no return object : strings written to file and message confirming succcess printed on success    
    '''
    formes = arrays[0]
    feats_all  = arrays[1]
    reccs  = arrays[2]
    lemmas_all  = arrays[3]
    PLXtag  = arrays[4]
    sent_id = arrays[5]
    tokenids = arrays[6]
    
    m=0
    with open(output_file, 'w', encoding="UTF-8") as f:
        for t, tokenid in enumerate(tokenids):
            if tokenid in [1, "1"]:
                metachunk = "\n"+ meta_strings[m][0] + "\n" + meta_strings[m][1] + "\n" + meta_strings[m][2] + "\n"
                #metachunk = "\n"+ meta_strings[m][0] + "\n" 
                f.write(metachunk)
                m+=1
                string = str(tokenid) +"\t"+ str(formes[t])+"\t"+ str(lemmas_all[t]) +"\t"+ str(reccs[t])+"\t_\t"+ str(feats_all[t]) +"\t_\t_\t_\t" + str(PLXtag[t]) + "\n"
                f.write(string)
            else:
                string = str(tokenid) +"\t"+ str(formes[t])+"\t"+ str(lemmas_all[t]) +"\t"+ str(reccs[t])+"\t_\t"+ str(feats_all[t]) +"\t_\t_\t_\t" + str(PLXtag[t]) + "\n"
                f.write(string)
    print(f"File {output_file} exported successfully")
  
######  
 
# lemmatisation preparation functions

def run_dataset_preppers(lefff_data):
  '''
  Function to run preparer functions for lemmatiser, extracting data from input df and removing data from df as it progresses through the list
  Note : as items are removed from lefff_data as they are processed, lefff_data must be reloaded if functions are to be run again
  Args :
      input : lefff_data : absolute path to csv file of forme, Lem, POS for import as csv then transformation to specific subarrays
      Return : datasets : a list of datasets in specified order
  '''
  # run functions that go transform LEFFF csv into arrays by POS, 
  lefff_data, VERB_dataset = prepare_VERB_leff_data(lefff_data)
  lefff_data, NOUN_dataset = prepare_NOUN_leff_data(lefff_data)
  lefff_data, AUX_dataset = prepare_AUX_leff_data(lefff_data)
  lefff_data, ADJ_dataset = prepare_ADJ_leff_data(lefff_data)
  lefff_data, ADV_dataset = prepare_ADV_leff_data(lefff_data)
  functword_dataset = prepare_functword_data(lefff_data)
  datasets = [VERB_dataset, NOUN_dataset, AUX_dataset, ADJ_dataset, ADV_dataset, functword_dataset]
  return datasets

# lefff_data['POS'].value_counts()
# VERB     836568
# ADJ      178453
# NOUN     148827
# PROPN     60971
# AUX       28390
# ADV        4393
# ADP         443
# SCONJ       332
# DET         171
# PRON         93
# CCONJ        47
# NUM          43
# INTJ         21


## series of extremely explicit functions to make sets of arrays for specific POS categories to enable faster lookups at price of longer preparation time
## specific functions for VERB, NOUN, ADV, AUX, ADJ
## remaining POS categories are in a grouped 'functword' : INTJ, NUM, CCONJ, SCONJ, PRON, DET
#  @AR : what happened to PROPN ? => went to PLX as both PLX and simplex, so should already have been processed
## TO DO : make these declarations more efficient

def prepare_VERB_leff_data(lefff_data):
  # convert pd df to arrays for verb specific data
  VERB_data = lefff_data.loc[lefff_data['POS']=="VERB"]
  lefff_data = lefff_data.loc[lefff_data['POS']!="VERB"]

  VERB_lemmas_array = VERB_data['Lemme'].values
  VERB_formes_array = VERB_data["Forme"].values
  VERB_POS_array = VERB_data["POS"].values
  VERB_feats_array = VERB_data["Traits"].values
  
  VERB_dataset = [VERB_lemmas_array, VERB_formes_array , VERB_POS_array, VERB_feats_array]
  
  return lefff_data, VERB_dataset

def prepare_NOUN_leff_data(lefff_data):
    # convert pd df to arrays for Noun specific data

  NOUN_data = lefff_data.loc[lefff_data['POS']=="NOUN"]
  lefff_data = lefff_data.loc[lefff_data['POS']!="NOUN"]

  NOUN_lemmas_array = NOUN_data['Lemme'].values
  NOUN_formes_array = NOUN_data["Forme"].values
  NOUN_POS_array = NOUN_data["POS"].values
  NOUN_feats_array = NOUN_data["Traits"].values
  
  NOUN_dataset = [NOUN_lemmas_array, NOUN_formes_array , NOUN_POS_array, NOUN_feats_array]
  
  return lefff_data, NOUN_dataset
def prepare_ADV_leff_data(lefff_data):
  # convert pd df to arrays for ADV specific data

  ADV_data = lefff_data.loc[lefff_data['POS']=="ADV"]
  lefff_data = lefff_data.loc[lefff_data['POS']!="ADV"]

  ADV_lemmas_array = ADV_data['Lemme'].values
  ADV_formes_array = ADV_data["Forme"].values
  ADV_POS_array = ADV_data["POS"].values
  ADV_feats_array = ADV_data["Traits"].values
  
  ADV_dataset = [ADV_lemmas_array, ADV_formes_array , ADV_POS_array, ADV_feats_array]
  
  return lefff_data, ADV_dataset
def prepare_AUX_leff_data(lefff_data):
    # convert pd df to arrays for AUX specific data

  AUX_data = lefff_data.loc[lefff_data['POS']=="AUX"]
  lefff_data = lefff_data.loc[lefff_data['POS']!="AUX"]

  AUX_lemmas_array = AUX_data['Lemme'].values
  AUX_formes_array = AUX_data["Forme"].values
  AUX_POS_array = AUX_data["POS"].values
  AUX_feats_array = AUX_data["Traits"].values
  
  AUX_dataset = [AUX_lemmas_array, AUX_formes_array , AUX_POS_array, AUX_feats_array]
  
  return lefff_data, AUX_dataset
def prepare_ADJ_leff_data(lefff_data):
  # convert pd df to arrays for ADJ specific data

  ADJ_data = lefff_data.loc[lefff_data['POS']=="ADJ"]
  lefff_data = lefff_data.loc[lefff_data['POS']!="ADJ"]

  ADJ_lemmas_array = ADJ_data['Lemme'].values
  ADJ_formes_array = ADJ_data["Forme"].values
  ADJ_POS_array = ADJ_data["POS"].values
  ADJ_feats_array = ADJ_data["Traits"].values
  
  ADJ_dataset = [ADJ_lemmas_array, ADJ_formes_array , ADJ_POS_array, ADJ_feats_array]
  
  return lefff_data, ADJ_dataset
def prepare_functword_data(lefff_data):
  '''
  Prepare arrays for the remaining POS tags : INTJ, NUM, CCONJ, SCONJ, PRON, 
  '''
    # convert pd df to arrays for function_word specific data

  functword_lemmas_array = lefff_data['Lemme'].values
  functword_formes_array = lefff_data["Forme"].values
  functword_POS_array = lefff_data["POS"].values
  functword_feats_array = lefff_data["Traits"].values
  
  functword_dataset = [functword_lemmas_array, functword_formes_array , functword_POS_array, functword_feats_array]
  
  return functword_dataset


def process_nums_for_romains(input_string):
    '''
    determine if a string is a valid roman numeral by testing of the characters are all in the list of valid characters for roman numerals
    If the uppercase version of a character is not in the string, we do not have a roman numeral
    Args : 
        input : input_string : string of characters
        output : input_string : lowercase version of the inputstring, unless all characters are in the set valid_roman_chars
    Note : this catches all regular roman numerals, and a small number of legitimate French words, such as CI, MI IL
    These legitimate French words however pose no special difficulty in lemmatisation : ci and mi have hyphens before/after, and for PREFAB, il is always lemmatised as il

    '''
  
    valid_roman_chars = {'I', 'V', 'X', 'L', 'C', 'D', 'M'}

    for char in input_string:
        if char.upper() not in valid_roman_chars:
            return input_string.lower()

    return input_string
def prepare_fastlists():
    '''
    Load the list of the most common formes, POS and lemmas to be used as a priority checklist : 50-60 % of tokens are in this list of around 1200 forms, enabling fast lookups in the np array created
    Args : 
      input : takes no input argument ; loads from specific csv file with named columns
      Returns : four arrays, one for each of lemmas, formes, POS and feats. Drawn from the same df, the arrays are the same shape and item i is the same row from the df-source
    Notes : the original csv is not returned      

    '''
  
    fast_file = f"{PathHead}/Data/lemm_lookups/fastlookup.csv"
    # load a csv, extract named arrays
    fast_data = pd.read_csv(fast_file, sep="\t")
    fastlemmas = fast_data['Lemme'].values
    fastformes = fast_data["Forme"].values
    fastPOS = fast_data["POS"].values
    fastfeat = fast_data["Traits"].values

    return fastlemmas, fastformes, fastPOS, fastfeat


# lemmatisation running functions :: these change to be used to build verb_data dict…etc
def get_lemma_feats(pos, forme, datasets, k, PLXtag):
  '''
  function to get lemmma and feats for a forme + POStag
  Args : inputs : pos : POStag of forme being considered
                  forme : form of forme being considered
                  datasets : list of datasets yielded by preppers
                  k : index of current term in the formes array
          returns : lemma, feats : lemma and feats of current_form
  '''
  # if POS not in list1 and forme + POS combination gives 0length target in fastlist, we use this method to get the lemma.
  functword_dataset = datasets[5]
  # based on pos of input form, get arrays to look in
  if pos in ("NOUN", "VERB", "AUX", "ADV", "ADJ"):
    formes_array, lemmas_array, POS_array, feats_array = return_subarrays(forme, pos, NOUN_data, VERB_data, AUX_data, ADJ_data, ADV_data)
  if pos in ("CCONJ","SCONJ","ADP","PRON","INTJ","DET"):
    formes_array =  functword_dataset[1] 
    lemmas_array =  functword_dataset[0] 
    POS_array =  functword_dataset[2] 
    feats_array =  functword_dataset[3] 

  # find matches, and return these as a list from LC  
  matches = np.where(formes_array == forme.lower())[0]
  target =  [match for match in matches ]
  ## if the forme isn't in the list, let it be its own lemma, with _ as feats
  if len(target)==0:
    lemma = forme.lower()
    feats = "_"
    ## if forme is in fast formes and there is a match of the right POS:
  else:
      #get lemma from array using index
    lemma = lemmas_array[target][0]
    ## get feats with function, replacing blanks with customtext
    feats = get_feats(feats_array, target)
    if feats != "_" :
      if oral_flag == "T":
        current_misc_value = PLXtag[k]
        new_misc_value = '<plx="PLK">' + current_misc_value
        PLXtag[k] = new_misc_value
      else :
        PLXtag[k] = "PLK"

  return lemma, feats

def set_lemme_to_forme(pos, forme):
  '''
  helper function to set lemma to forme
  '''
  lemma = forme
  feats = "_"
  return lemma, feats

def test_nums_for_lemmas(pos, forme):
  '''
  helper function to determine whether form is a number with a lemma
  '''
  if pos == "NUM":
    lemma = process_nums_for_romains(forme)
    feats = "_"
  return lemma, feats

def get_fastmatchcount(forme, pos):
  '''
  get the count of matches with POS and form match
  this lets us know whether to use fast_arrays or full_arrays
  args:
    inputs : forme, pos : forme and pos being considered
    returns : target : matches in fast arrays of there are any
  '''
  matches = np.where(fastformes == forme.lower())[0]
  target = [match for match in matches if fastPOS[match]==pos]
  return target

def get_fastdata(fastlemmas, fastfeat, PLXtag, target, k):
    lemma = fastlemmas[target][0]
    
    ## get feats with function, replacing blanks with customtext
    feats = get_feats(fastfeat, target)
    if feats != "_" :
      if oral_flag == "T":
        current_misc_value = PLXtag[k]
        new_misc_value = '<plx="PLK">' + current_misc_value
        PLXtag[k] = new_misc_value
      else :
        PLXtag[k] = "PLK"
        
    return lemma, feats, PLXtag
def set_list1_lemmas(forme, pos):
  if pos == "NUM":
      lemma, feats = test_nums_for_lemmas(pos, forme)
  else:
    lemma, feats = set_lemme_to_forme(pos, forme)
  return lemma, feats

def get_non_fast(pos, forme, datasets, list2, k, PLXtag):
    if pos in list2:
      lemma, feats = get_lemma_feats(pos, forme, datasets, k, PLXtag)
    else:
      lemma, feats = unk_postag(pos, forme)
    return lemma, feats

def unk_postag(pos, forme):
  lemma = forme.lower()
  feats = '_unknownpostag_'  
  return lemma, feats




# remove leading -
def make_tidy_lemma(lemma):
    '''
    tidy lemmas by removing leading punctuation and replacing ligatures with exploded forms
    args :
      input : lemma : current_value of lemma
      returns : tidy_lemma : lemma in tidy form
    '''
    # convert lemmas to str, then remove any leading chars from lemmas
    input_string = str(lemma)
    input_string = re.sub("œ", "oe", input_string)
    input_string = re.sub("æ", "ae", input_string)
    input_string = re.sub("Œ", "OE", input_string)
    input_string = re.sub("Æ", "AE", input_string)
    # List of 3 distinct invalid leading characters 
    invalid_chars = ['-', '–', '—']

    # Iterate over the string and find the first valid character
    index = 0
    while index < len(input_string) and input_string[index] in invalid_chars:
        index += 1
    
    # Return the string starting from the first valid character
    return input_string[index:]
    # Remove the leading invalid character if present
    if input_string and input_string[0] in invalid_chars:
        return input_string[1:]
    return tidy_lemma ## tidy lemma isn't defined herein ; what about in C runner ? relationship between input_string and tidy_lemma, and whichb to return when needs to be clarified


# 
# def get_V_hypoarrays()
# 


def return_subarrays(forme, pos, NOUN_data, VERB_data, AUX_data, ADJ_data, ADV_data):
  # get and return the letter and POS-specific subarray
  thisChar = forme[0].upper()
  if pos == "VERB":
    array_set = VERB_data[thisChar]
  # if for other pos
  if pos == "NOUN":
    array_set = NOUN_data[thisChar]
  if pos == "AUX":
    array_set = AUX_data[thisChar]
  if pos == "ADJ":
    array_set = ADJ_data[thisChar]
  if pos == "ADV":
    array_set = ADV_data[thisChar]
  formes_array = array_set[0]
  lemmas_array = array_set[1]
  POS_array = array_set[3]
  feats_array = array_set[2]

  return formes_array, lemmas_array, POS_array, feats_array

# this function gets called
def generate_subarrays(letter, dataset):
    # generate subarrays : this will be called by make_subarray_sets
    formes_array= dataset[1]
    lemmas_array =  dataset[0]
    POS_array =  dataset[2]
    feats_array =  dataset[3]
    index_value_set = [i for i in range(formes_array.size) if str(formes_array[i])[0] == letter.lower()]  
    formes_subset = formes_array[index_value_set]
    lemmas_subset = lemmas_array[index_value_set]
    feats_subset = feats_array[index_value_set]
    POS_subset = POS_array[index_value_set]
    return formes_subset, lemmas_subset, feats_subset, POS_subset

# this function does not get called
def generate_verb_hypoarrays(twogram, dataset):
    # generate subarrays : this will be called by make_subarray_sets
    formes_array= dataset[1]
    lemmas_array =  dataset[0]
    POS_array =  dataset[2]
    feats_array =  dataset[3]
    index_value_set = [i for i in range(formes_array.size) if str(formes_array[i])[0:2] == twogram.lower()]  
    formes_subset = formes_array[index_value_set]
    lemmas_subset = lemmas_array[index_value_set]
    feats_subset = feats_array[index_value_set]
    POS_subset = POS_array[index_value_set]
    return formes_subset, lemmas_subset, feats_subset, POS_subset

# this function gets used, called
def make_subarray_sets_forPOS(formes, datasets):
    # run the generator to make the subarray sets
    #letterset = sorted(set([str(forme[0]).upper() for forme in formes]))
    # get sorted set of all chars which begin a form, so subarrays can be made
    #verb_letterset = set([(str(forme)[0:2]).upper()for forme in formes])
    ## VERB_dataset[0].size == 301710
    ## ADJ_dataset[0].size == 56997
    ## NOUN_dataset[0].size == 87233
    ## AUX_dataset[0].size == 87233
    
    letterset = sorted(set([(str(forme)[0]).upper() for forme in formes]))
    
    VERB_dataset = datasets[0]
    NOUN_dataset = datasets[1]
    AUX_dataset = datasets[2]
    ADJ_dataset = datasets[3]
    ADV_dataset = datasets[4]    
    
    VERB_data = {}
    dataset = VERB_dataset
    # letterset = string.ascii_uppercase + "ÀÂÄÉÈÊËÎÏÔÖÙÜÛÇŸ-—"
    for letter in letterset:
        formes_subset, lemmas_subset, feats_subset, POS_subset = generate_subarrays(letter, dataset)
        VERB_data[letter] = [formes_subset, lemmas_subset, feats_subset, POS_subset]
    
    NOUN_data = {}
    dataset = NOUN_dataset
    for letter in letterset:
        formes_subset, lemmas_subset, feats_subset, POS_subset = generate_subarrays(letter, dataset)
        NOUN_data[letter] = [formes_subset, lemmas_subset, feats_subset, POS_subset]
    
    AUX_data = {}
    dataset = AUX_dataset
    for letter in letterset:
        formes_subset, lemmas_subset, feats_subset, POS_subset = generate_subarrays(letter, dataset)
        AUX_data[letter] = [formes_subset, lemmas_subset, feats_subset, POS_subset]
    
    ADJ_data = {}
    dataset = ADJ_dataset
    for letter in letterset:
        formes_subset, lemmas_subset, feats_subset, POS_subset = generate_subarrays(letter, dataset)
        ADJ_data[letter] = [formes_subset, lemmas_subset, feats_subset, POS_subset]
    
    ADV_data = {}
    dataset = ADV_dataset
    for letter in letterset:
        formes_subset, lemmas_subset, feats_subset, POS_subset = generate_subarrays(letter, dataset)
        ADV_data[letter] = [formes_subset, lemmas_subset, feats_subset, POS_subset]
    
    return NOUN_data, VERB_data, AUX_data, ADJ_data, ADV_data


def lookup_np_v3(formes, pos_reccs, PLXtag, datasets, oral_flag):
  '''
  take each input array, and assign lemmas based on POS and form
  Args :
    formes : array of all forms in text to be lemmatised
    pos_reccs : POS recommendations for each form
    PLXtag : array of all PLXtag data. for oral corpora, this also holds the string of extra columns for time1, time2, speakername, etc, which will appear in conll column 10
    datasets : list of arrays created specifically for each text, subdivided by POS
    oral_flag : T or F, used to control concatenation of strings for conllu col10
  '''
  # the big kahuna : get the lemmas
  list1 = ("SYM","PUNCT","PROPN", "NUM")
  list2 = ("ADJ", "ADV", "ADP","AUX","VERB","NOUN","DET","SCONJ","CCONJ","INTJ","PRON")
  k=0
  lemmas_all, feats_all = [], []
  times_all = []
  # for pairs consisting of FORM and POS, using tqdm to show progress, zip formes and pos_reccs
  for forme, pos in tqdm(zip(formes, pos_reccs)):
      start = time.time() ## optional timing statement to time individual lookuptimes 
      forme = str(forme) # force forme to string, and look in list1 to determine which lookups to use : obly certain POS values present in FASTset
      if pos in list1:
        lemma, feats = set_list1_lemmas(forme, pos)
        #statement = "2"
      else:
        target = get_fastmatchcount(forme, pos)
        if len(target) >0:
          lemma, feats, PLXtag = get_fastdata(fastlemmas, fastfeat, PLXtag, target, k)
          #statement = "3"
        else:
          lemma, feats = get_non_fast(pos, forme, datasets, list2, k, PLXtag)
          #statement = "4"
      tidy_lemma = make_tidy_lemma(lemma)
      lemmas_all.append(tidy_lemma)
      feats_all.append(feats)
      delta = time.time() - start
      times_all.append(delta)
      k+=1

  #return a, b, c
  feats_all = np.array(feats_all)
  lemmas_all = np.array(lemmas_all)
  return feats_all, lemmas_all, PLXtag, times_all



#### end of defintiion of functions


######################### specify paths and things here ##############################
# point to file, specify corpus
lefff_file = f"{PathHead}/Data/lemm_lookups/LEFFF_lookup.csv"
lefff_data = pd.read_csv(lefff_file, sep="\t", low_memory=False, dtype=str)
# print_data = lefff_data[['F','LEM','pos']]
# print_data.to_csv(f"{PathHead}/Data/lemm_lookups/LEFFF_lookup_out.csv", sep="\t", header=None, index=False)
# lefff_data.columns = ['F','pos','LEM']
#lefff_data.shape 1305877
corpus = "ROM" 
## prepare fast datasets, which don't need to be customised to corpus
fastlemmas, fastformes, fastPOS, fastfeat = prepare_fastlists()
# prepare datasets, so they are ready to be set for each text
datasets=  run_dataset_preppers(lefff_data)
# MPF-Adeline1
#current_folder = '/Volumes/Data/ANR_PREFAB/processing_romans_tranches/trancheW/004/lemmatised/'
# path = current_folder = '/Volumes/Data/ANR_PREFAB/CorpusPREFAB/Romans/aligned_prefab_processing_archives/aligned_FR_DE_archive_zips/'
# path = current_folder = '/Volumes/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/Oral_prefab_def/ESLO2/uncompressed/'
# path = current_folder = '/Volumes/Data/ANR_PREFAB/CorpusPREFAB/Romans/aligned_prefab_processing_archives/aligned_FR_DE_archive_zips/'
targetfolders = ['lemmatise_me']
for targetfolder in targetfolders:
  path = current_folder = f'/Volumes/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/{targetfolder}/'
  subfolders = glob.glob(current_folder + "*/" )
  chunks = [item.replace(current_folder,'').replace('/','') for item in subfolders]
  # skippedGE24, 53, 74, 75, TS52
  all_items = chunks #= ["TS52"]
  #all_items = ['HS27','HS28','HS29','HS30','HS31','HS32','HS33','HS34','HS35','HS36']
  errorlist, successlist = [],[]
  # path = '/Volumes/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v5/X/' 
  #item = 'FY18'
  # item = all_items[-1]
  oral_flag = "F"
  for item in tqdm(all_items):
  ##if item == "FY13" :
      ref_file_full = path + f'{item}/{item}-104.conllu'
      ref_file = ref_file_full.replace(path, '').replace('-104.conllu','').replace('101/','')
      output_file = ref_file_full.replace('-104.conllu', '-6000tv8.conllu')
  
      if os.path.exists(ref_file_full)==False:
          ref_file_full = ref_file_full.replace('-104.conllu', '-004.conllu')
          ref_file = ref_file_full.replace(path, '').replace('-004.conllu','').replace('101/','')
          output_file = ref_file_full.replace('-004.conllu', '-6000tv8.conllu')
  
      hops_all, udpipe_all, stanza_all = load_all_data(path, corpus, ref_file)
      if len(hops_all)== len(udpipe_all) == len(stanza_all):
        all_data = compare_and_tidy(udpipe_all, stanza_all, hops_all)
        pos_update = restrict_and_reccPOS(all_data)                                    #  ≈ 2-3k per s ≈ 40-50s for 100k ≈ 130-150k rest+recc/min
        pos_reccs, formes, PLXtag = apply_corrections(pos_update)                                                     # ≈ 2.9-3M/s   ≈  0.03s for 100k ≈ 175-180M corrections/min
        # generate subsets specific to current text ## or can generate generic from list of forms in each set!
        NOUN_data, VERB_data, AUX_data, ADJ_data, ADV_data = make_subarray_sets_forPOS(formes, datasets)
        # use the lookup functionv3 to get lemmas based on POS and formes
        feats_all, lemmas_all, PLXtag, times_all = lookup_np_v3(formes, pos_reccs, PLXtag, datasets, oral_flag) 
        pos_reccs = run_post_lemm_corrections(formes, lemmas_all, pos_update, pos_reccs)                              # 1,1-1.2 M per s ≈ 0.1s for 100k ≈ 67-71 M corr/min
        
        sent_id, tokenids, meta_strings = rebuild_for_conllu(ref_file_full)    # is this where the feats are input into wrong col ?       # 66-80k per s ≈ 1,5 s for 100k ≈ 3,9-4,8M rebuild /s
        feats_all = update_features(pos_reccs, lemmas_all, all_data, feats_all, PLXtag)
        
        arrays = [formes, feats_all, pos_reccs, lemmas_all, PLXtag, sent_id, tokenids]                              #   2.3 M-B per s ≈  40m-µs for 100k ≈ 139 M-B/min
        export_reinjected_annotations(output_file, arrays, meta_strings)
        report = item, len(hops_all), len(udpipe_all), len(stanza_all)
        successlist.append(report)
      else:
        print(f"ERROR FOR ITEM == {item}")
        report = item, len(hops_all), len(udpipe_all), len(stanza_all)
        print(f"Report :: uneven lengths : {report}")
        errorlist.append(report)


## TODO : add intermediate level of lookup, consisting of POS, LEM, forme for all forms used in text thus far:
#### notes
'''
lemmatisation notes:
for oral, need to reroute Hum from PROPN to INTJ in 6001 step
qu'est-ce qui missed, d'accord missed : what other PLX missed in oral ??

string manupulation post-lemmatization to send, eg wrongly tokenised ::
30	qu'est-ce qui	qu'est-ce qui	MOO	_	_	_	_	_<t1="551.44"><t2=551.72"><spk="Mickaël"><cloc="same"><pause="without"><lex="-"><sup="-"?

"^(\d+)\tqu'\tque\tPRON\t_\t_\t_\t_\t_\t<t1="(.+?)"><.+?>$\n^(\d)+\test-ce\test-ce\tPRON\t_\t_\t_\t_\t_\t<.+?>$\n^(\d)+\tqui\tqui\tPRON\t_\t_\t_\t_\t_\t<.+?><t2="(.+?)">$"
>>

"\1\tqu'est-ce qui\tqu'est-ce qui\tMOO\t_\t_\t_\t_\t_<t1="\2"><t2=\5"?""

to 

30	qu'est-ce qui	qu'est-ce qui	PRON	_	_	_	_	_	<t1="551.44"><t2="551.72"><spk="Mickaël"><cloc="same"><pause="without"><lex="-"><sup="-">

-- still need to reattach là, -ci to celui, 
send -moi to 2 tokens
'''
#"adeptes" in ADJ_formes_array#

####
#timesarray = np.array(times_all)#
#

##
##ADJ_dataset = datasets[3]
## VERB_dataset[0]
##      formes_array= dataset[1]
##
##ADJ_formes_array = ADJ_dataset[0]
##new_adjs_array = new_adjs['Forme'].values
##
##add_me = [i for i in tqdm(range((new_adjs_array.size))) if new_adjs_array[i] not in ADJ_formes_array]
##
#adjs_to_add = new_adjs.loc[add_me]
#adjs_to_add.to_csv('/Users/Adam/Library/CloudStorage/Dropbox/ANR_PREFAB/Data/lemm_lookups/add_to_lexique.csv', sep="\t", header=None)
#
#new_adjs = pd.read_csv("/Users/Adam/Library/CloudStorage/Dropbox/ANR_PREFAB/Data/lemm_lookups/all_morphalu_adjs.csv", sep="\t", header=None)
#new_adjs.columns = ['Forme','LEM','POS']
#
