import pandas as pd
from lxml import etree

alignmentfile='/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/alignments/all.fr-de.full copy.tsv'

alignment_dict = {}
old_key = ""
with open(alignmentfile, 'r', encoding='UTF-8') as f:
  current_data = []
  for raw_line in f.readlines():
    # raw_line = f.readline()
    if raw_line.startswith('source=fr/'):
      new_key = raw_line
      # if len(old_key) >1:
      alignment_dict[old_key] = current_data
      current_data = []
      old_key = new_key
    current_data.append(raw_line)
  alignment_dict[old_key] = current_data
# keylist = list(alignment_dict.keys())      
# keylist[23]

# old_key
# 

tree_folder = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/aligned_v3/processed/XML_v3/NUM/'
alignments_folder = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/aligned_v3/processed/XML_v3/NUM_alignments/'
# key = keylist[1]
print_store = []
missing_list = []
for m, missing in enumerate(missing_list):
  print(m, os.path.basename(missing), "\n")
# alignmentdata = alignment_dict[old_key]
for key in tqdm(alignment_dict.keys()):
  if len(key) > 1:
    print(key)
    # trim and convert to df
    alignmentdata = alignment_dict[key]
    alignmentdata_trimmed = alignmentdata[2:]
    alignmentdata_trimmed = [chunk.replace('\n','') for chunk in alignmentdata_trimmed]
    alignmentdata_split = [chunk.split("\t") for chunk in alignmentdata_trimmed]
    alignment_data = pd.DataFrame(alignmentdata_split)
    alignment_data.columns = ['FR_source','DE']
    # creer une nouvelle colonne avec les id des phrases FR en liste
    alignment_data['FR_list'] = alignment_data['FR_source'].apply(transform_to_int_list)
    # creer une colonne où stocker les nouvelles données
    alignment_data['newIDs'] = ""
    
    
    
    # locate, load, make dict with new version of XML
    tree_pattern = key.split("\t")[0].replace('source=fr/prefab_fr-deV2/','').replace('.fr.xml','-PREFABv3.fr.xml') 
    tree_file = tree_folder + tree_pattern
    # tree_file = glob.glob(tree_folder + tree_pattern + "*.xml")[0]
    if os.path.exists(tree_file) is False:
      missing_list.append(tree_file)
    if os.path.exists(tree_file) is True:
      tidy_tree = etree.parse(tree_file)
      s_blocks = tidy_tree.findall(".//s")
      tree_results = {}
      for q, s_block in enumerate(s_blocks):
            # s_block = s_blocks[5]
            orig_id = s_block.get("id")
            new_id = [s_block.get("prefabID")]
            if orig_id not in tree_results.keys():
              tree_results[orig_id] = new_id
            else:
              current_list = tree_results[orig_id]
              current_list.append(*[item for item in new_id])
              tree_results[orig_id] = current_list
      
        # option2 with dict 17k/s
      for index, row in tqdm(alignment_data.iterrows()):
          int_list = row['FR_list']
          # créer une liste pour stockage de nouveaux ID
          new_values = []
          for item in int_list:
            # ajouter s devant l'ID
            item =  's' + str(item)
            # rechercher ce bloc dans le tree, puis obtenir le nouvel ID
            this_id = " ".join([chunk for chunk in tree_results.get(item)])
            new_values.append(this_id)
            # ajouter à la liste
            # if s_blocks is not None:
            #   new_values.append(new_id)
          # ajouter la liste à la df    
          alignment_data.at[index, 'newIDs'] = new_values
        
      alignment_filename = os.path.basename(tree_file).replace('.fr.xml','_alignment.tsv')
      outputwritetestdoc = alignments_folder + alignment_filename
      with open(outputwritetestdoc, 'w', encoding='UTF-8') as w:
        _ = w.write(key)
        _ = w.write("\n")
        print_store.append(key)
        print_store.append("\n")
        for i in range(len(alignment_data)):
          fr_out = " ".join([str(a) for a in (alignment_data.iloc[i, 3])])
          de_out =  alignment_data.iloc[i, 1] 
          all_out = str(fr_out) + "\t" + str(de_out) + '\n'
          _ = w.write(all_out)
          print_store.append(all_out)

alignments_consolidated_file = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/aligned_v3/processed/XML_v3/NUM/zz__alignments_consolidated.tsv'
with open(alignments_consolidated_file, 'w', encoding='UTF-8') as w:
  for line in print_store:
    _ = w.write(line)
  
# treetidyfile='/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/aligned_v3/processed/XML_v3/DE/walter kempowski_les temps héroïques 3e partie-PREFABv3.fr.xml'
tidy_tree = etree.parse(treetidyfile)
## alignement :
import pandas as pd
# make dict from xml with key value == oldID,newID, 
tidy_tree = etree.parse('/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/RomansV3_d1/NUMv3/162/jean-christophe grange_les rivières pourpres-PREFABv3.fr.xml')
# s_blocks = tidy_tree.findall(".//s")
# tree_results = {}
# for q, s_block in enumerate(s_blocks):
#   orig_id = s_block.get("id")
#   new_id = s_block.get("prefabID")
#   tree_results[orig_id] = new_id
  
# fichier contenant l'alignment FR_DE pour les rivières pourpres
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

# print individually to file, then cat
outputwritetestdoc = '/Users/Data/outputwritetestdoc.tsv'
with open(outputwritetestdoc, 'w', encoding='UTF-8') as w:
  for i in range(len(alignment_data)):
    fr_out = " ".join([str(a) for a in (alignment_data.iloc[i, 3])])
    de_out =  alignment_data.iloc[i, 1] 
    all_out = str(fr_out) + "\t" + str(de_out) + '\n'
    _ = w.write(all_out)    
    
