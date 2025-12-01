# metteur_à_jour pour permuter les alignments de phrase sur les phrases nouvellement resegmentées
# updater to permutate alignment IDs onto new sentences based on new segmentation rules and old segmentation alignments.
## alignement :
import pandas as pd
from lxml import etree
# make dict from xml with key value == oldID,newID, 
# specify example file with newly modified sentences
tidy_tree = etree.parse('/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/NUMv3/162/jean-christophe grange_les rivières pourpres-PREFABv3.fr.xml')
s_blocks = tidy_tree.findall(".//s")
tree_results = {}
for q, s_block in enumerate(s_blocks):
  orig_id = s_block.get("id")
  new_id = s_block.get("newID")
  tree_results[orig_id] = new_id
  
# fichier contenant l'alignment FR_DE pour les rivières pourpres : this contains old sent ids for FR and the alignment of these old IDs with German sentences.
alignment_file = '/Users/Data/alignement_pourpores.tsv'
# import as pd df, nommer le colonnes
alignment_data = pd.read_csv(alignment_file, sep="\t", header=None)
alignment_data.columns = ['FR_source','DE']

# fonction pour une liste de valeurs pour
def transform_to_int_list(value):
    # Check if the value is empty (None, NaN, or an empty string)
    if pd.isna(value) or value == '':
        return []
    
    # If the value is a single integer, return it as a list
    if isinstance(value, int):
        return [value]
    
    # If the value is a string of integers separated by commas or spaces
    if isinstance(value, str):
        # Split the string by commas or spaces, then convert each to an integer
        return [int(x) for x in value.replace(',', ' ').split()]
    
    # If the value is already a list of integers, return it as is
    if isinstance(value, list):
        return value

    # Fallback for unexpected types, return an empty list
    return []


# creer une nouvelle colonne avec les id des phrases FR en liste
alignment_data['FR_list'] = alignment_data['FR_source'].apply(transform_to_int_list)

# creer une colonne où stocker les nouvelles données
alignment_data['newIDs'] = ""

# option2 with dict 17k/s
for index, row in tqdm(alignment_data.iterrows()):
    int_list = row['FR_list']
    # créer une liste pour stockage de nouveaux ID
    new_values = []
    for item in int_list:
      # ajouter s devant l'ID
      item =  's' + str(item)
      # rechercher ce bloc dans le tree, puis obtenir le nouvel ID
      this_id = tree_results.get(item)
      new_values.append(this_id)
      # ajouter à la liste
      # if s_blocks is not None:
      #   new_values.append(new_id)
    # ajouter la liste à la df    
    alignment_data.at[index, 'newIDs'] = new_values


outputwritetestdoc = '/Users/Data/outputwritetestdoc.tsv'
with open(outputwritetestdoc, 'w', encoding='UTF-8') as w:
  for i in range(len(alignment_data)):
    fr_out = " ".join([str(a) for a in (alignment_data.iloc[i, 3])])
    de_out =  alignment_data.iloc[i, 1] 
    all_out = str(fr_out) + "\t" + str(de_out) + '\n'
    _ = w.write(all_out)    
    



#### new
alignmentdict = {}
input_file_withmanydcs=  'C:/Users/prefab_de-frV2/all.fr-de.tsv'
with open(input_file_withmanydcs, 'r', encoding='UTF-8') as w:
  lineraw = w.readline()
  if lineraw.startswith('source=fr'):
    newkey = lineraw
    alignmentdict[key] = linedata  
    linedata = []
    key = newkey
  else:
    linedata.append(lineraw)
  
