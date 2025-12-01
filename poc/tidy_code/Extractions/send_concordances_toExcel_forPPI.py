# send csv concordance to PPI_grille :: remember that inputPath is a folder, and all csvs in it will be processed
import os, sys, glob, argparse, re
import pandas as pd
from tqdm import tqdm


parser = argparse.ArgumentParser()
parser.add_argument('--inputPath', default="")
args = parser.parse_args()
target_path = args.inputPath

# regex to remove ]]] and #
def convert_concordance(target_path):
  
  
  def tidy_string(string):
    tidied_string = re.sub(r'\[\[\[(.+?)#\d+\]\]\]',r'\1' ,string)
    return tidied_string
  
  
  # input_file = '/Users/Data/Downloads/concordances/pas_faux.csv'
  colnames = ["numQuery","sentId","ContexteGauche","Pivot","ContexteDroite","LangAlign","Auteur","Collection","corpusId","DatePubli","MaisonEdition","LieuEdition","URL","LangSource","TitreSource","NomFichierSource","Sous-genre","Titre","DateTraduction","Traducteur","Type","Mots","Année"]
  # def send_csv_to_excel(input_file):
  
  outputfilename1 = target_path.replace('.csv','.xlsx')
  outputfilename2 = target_path.replace('.xlsx','_MAJ.xlsx')
  df = pd.read_csv(target_path, sep=";")
  df.columns = colnames
  
  l_context_tidy = [tidy_string(string) for string in df['ContexteGauche'].values]
  pivot_tidy = [tidy_string(string) for string in df['Pivot'].values]
  tidy_pivot_strs = [re.sub(r' $|^ ', '', pivot_str) for pivot_str in pivot_tidy]
  r_context_tidy = [tidy_string(string) for string in df['ContexteDroite'].values]
  
  df['ContexteGauche'] = l_context_tidy
  df['Pivot'] = tidy_pivot_strs
  df['ContexteDroite'] = r_context_tidy
  
  new_cols = ['Acception',	'PPI',	'Lemme',	'Forme'	,'Modifieurs',	'Cooccurrents'	,'PropsSynt'	,'Position',	'Declench',	'Portée',	'FonctLang'	,'Remarques']
  for col in new_cols:
    df[col] = None

  
  col_order = ['sentId', 'Acception',	'PPI',	'Lemme',	'Forme'	,'Modifieurs',	'Cooccurrents'	,'PropsSynt'	,'Position'	,'Declench',	'Portée',	'FonctLang'	,'Remarques', 'ContexteGauche', 'Pivot', 'ContexteDroite', 'LangAlign',  'Auteur', 'Collection', 'corpusId', 'DatePubli', 'MaisonEdition', 'LieuEdition', 'URL', 'LangSource', 'TitreSource', 'NomFichierSource', 'Sous-genre', 'Titre', 'DateTraduction', 'Traducteur', 'Type', 'Mots', 'Année']


  tidy_df = df[col_order]
    
  # tidy_df.to_excel(outputfilename)
  # print(f'Exported file {str(f+1)} of {len(input_files)}')    
  sheet_name = os.path.basename(target_path).replace(".csv",'')
  # Define the different authorized options for each dropdown
  
  # Write the DataFrame to an Excel file without data validation first
  
  
  v_list_syntax = ['Antéposition', 'Inversion clitique', 'Ajout de NE','Suppression de NE'] # props suyntax : Antéposition, Inversion clitique
  v_list_position = ['Totale', 'Initiale', 'Médiane','Finale'] # positoin : totale, initiale, médiane, finale
  v_list_declench = ['Hétérodéclenché', 'Non-hétérodéclenché', 'Hétérorépetition'] ## déclenchement :: Hétérodéclenché, Non-hétérodéclenché, Hétéro par hétérorépétition
  v_list_portee = ['Pas de portée', 'Hétéroportée', 'Autoportée antérieure','Autoportée postérieure','Autoportée antérieure et postérieure'] ## portée : Pas de portée, Hétéroportée, Autoportée antérieure, Autoportée postérieure, Autoportée antérieure et postérieure
  hide_cols = ['P','Q','R','S','T','U','V','W','X','Y','Z','AA','AB','AC','AD','AE','AF','AG','AH']
  
  
  ## send file to excel with pd
with pd.ExcelWriter(outputfilename1, engine='openpyxl') as writer:
  tidy_df.to_excel(writer, index=False, sheet_name=sheet_name)
  
  ##############################################################
  # Load the workbook and the sheet
  workbook = writer.book
  worksheet = workbook[sheet_name]
  
  
  
  ###### load df, make new writer, make book, sheet, and add validation.
  df = pd.read_excel(outputfilename1)
  max_value = len(df) +1
  writer2 = pd.ExcelWriter(outputfilename1, engine='xlsxwriter')
  df.to_excel(writer2, sheet_name="Sheet1")
  workbook = writer2.book
  worksheet = writer2.sheets['Sheet1']
  # Define the shading format
  shading_format = workbook.add_format({'bg_color': '#D3D3D3'})  # Light gray
  
  # Get the number of rows
  num_rows = len(df) + 1  # +1 for the header row
  
  # Apply shading to alternate rows
  for row in range(1, num_rows, 2):  # Start from 1 to skip header, step by 2
      worksheet.set_row(row, None, shading_format)


  worksheet.data_validation(f"I2:I{max_value}", {'validate':'list', 'source':v_list_syntax})
  worksheet.data_validation(f"J2:J{max_value}", {'validate':'list', 'source':v_list_position})
  worksheet.data_validation(f"K2:K{max_value}", {'validate':'list', 'source':v_list_declench})
  worksheet.data_validation(f"L2:L{max_value}", {'validate':'list', 'source':v_list_portee})
  these_options = {"hidden":True}
  # hide cols with sentID and metas
  worksheet.set_column(first_col=1, last_col=1, options=these_options)
  worksheet.set_column(first_col=16, last_col=34, options=these_options)

  # Define formats for data
  right_format = workbook.add_format({'align': 'right','bg_color': '#359B56'})
  center_bold_format = workbook.add_format({'align': 'center', 'bold': True,'bg_color': '#359B56'})
  left_format = workbook.add_format({'align': 'left','bg_color': '#359B56'})

  # Apply text formats to specific columns
  worksheet.set_column('P:P', 200, left_format)       # Column A (Col1)
  worksheet.set_column('O:O', 30, center_bold_format) # Column B (Col2)
  worksheet.set_column('N:N', 200, right_format)        # Column C (Col3
    
    
    
  workbook.close()
  
    
  

if __name__=="__main__":
  input_files = sorted(glob.glob(target_path + "/*.csv"))

for f, input_file in tqdm(enumerate(input_files)):
  try:
    convert_concordance(input_file)
  except Exception as e:
    print(f"######  ERROR : {os.path.basename(input_file)} : {e}")


