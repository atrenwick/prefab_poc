# optional function to rename romans based on csv file ; optional as dict of values used in export to XML
import os, glob, shutil

def rename_files(filenames, csv_path):
  """
  Renames files based on a table mapping old names to new names in a CSV file, using absolute paths.

  Args:
      filenames: List of absolute paths to filenames to be renamed.
      csv_path: Path to the CSV file containing the rename table (OldName, NewName columns with tab separator).
  """
  # Read rename table from CSV file
  try:
    rename_table = pd.read_csv(csv_path, delimiter='\t', names=['OldName', 'NewName'])
    rename_table = rename_table.set_index('OldName')['NewName'].to_dict()  # Convert to dictionary for faster lookup
    return rename_table
  except FileNotFoundError:
    print(f"Error: CSV file not found at {csv_path}")
    


# option 1 : use rename files function to rename files based on oldname, newname, as defined in columns of csv file. 
csv_path = '/Users/Data/ANR_PREFAB/Data/Corpus/Romans/romans_tranche5/name_converter_tranche5.csv'
path = '/Users/Data/ANR_PREFAB/Data/Corpus/Romans/romans_tranche5/004/'
filenames = glob.glob(path + "*.conllu")
rename_table = rename_files(filenames, csv_path)
## then use rename_table with the following loop to rename
for filename in filenames:
  # Extract filename from the path

  # Find the corresponding new name in the table
  new_name = rename_table.get(filename)
  if new_name is not None:
    # Construct full paths for old and new files (using absolute paths)
    old_path = filename
    new_path = new_name

    # Rename the file (handle potential errors)
    try:
      os.rename(old_path, new_path)
      print(f"Renamed: {old_path} -> {new_name}")
    except OSError as e:
      print(f"Error renaming {old_path}: {e}")

#rename_table

######## option2 : much easier : specify path + suffix, and use shutil to copy to new destination, without changing name
source_files  =  glob.glob("//Users/Data/ANR_PREFAB/CorpusPREFAB/Corpus_oraux/Oral_prefab_def/MPF/uncompressed/*/" + "*6000tv8.conllu")
for source in source_files:
  destination = "/Users/Data/ANR_PREFAB/CorpusPREFAB/to_depparse_MPF/ "+ os.path.basename(source)
  shutil.copy(source, destination)

  
