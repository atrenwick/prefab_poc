## get all the metadata from all children of all nodes in all XML files
from typeguard import check_type
import os
from xml.etree import ElementTree as ET


def extract_all_data(xml_file):
  """Extracts all the data from the given XML file and returns a Pandas DataFrame.

  Args:
    xml_file (str): The path to the XML file.

  Returns:
    pd.DataFrame: A Pandas DataFrame containing the extracted data.
  """

  # Create an ElementTree object from the XML file.
  tree = ET.parse(xml_file)

  # Get the root element of the XML tree.
  root = tree.getroot()

  # Create a dictionary to store the extracted data.
  data = {}

  # Recursively iterate over the XML tree and extract the data.
  def extract_data(element, data):
    for child in element:
      data[child.tag.split("}")[-1]] = child.text
      extract_data(child, data)

  extract_data(root, data)
 
  # Add a column containing the local path of the XML file.
  data["xml_file_path"] = xml_file

  # Return a Pandas DataFrame containing the extracted data.
  return pd.DataFrame([data])

def get_xml_files(folder_path):
  """Gets a list of all the XML files in the given folder path.

  Args:
    folder_path (str): The path to the folder.

  Returns:
    list[str]: A list of all the XML files in the folder.
  """

  xml_files = []
  for root, dirs, files in os.walk(folder_path):
    for file in files:
      if file.endswith(".xml"):
        xml_files.append(os.path.join(root, file))

  return xml_files


def tidy_this_df(df):
  """Removes leading and trailing whitespace from all rows and columns of a Pandas DataFrame.

  Args:
    df (pd.DataFrame): The Pandas DataFrame to remove the whitespace from.

  Returns:
    pd.DataFrame: The Pandas DataFrame with the whitespace removed.
  """

  # Iterate over the rows and columns of the DataFrame.
  for row in df.iterrows():
    for col in df.columns:
      # If the cell value is not None, remove leading and trailing whitespace.
      if df.loc[row[0], col] is not None and not check_type(df.loc[row[0], col], (float, pd.NA)):

        df.loc[row[0], col] = df.loc[row[0], col].strip()

  return df

# get list of XML files
xml_files = get_xml_files('/Users/Data/Metadata/clapi_all/TEI/')

# Extract the data from the XML files and put it into a Pandas DataFrame.
df = pd.DataFrame()
for xml_file in xml_files:
  row = (extract_all_data(xml_file))
  df = pd.concat([df, row], axis=0, ignore_index=True)

tidy_df = tidy_this_df(df)
tidy_df.to_csv(f'/Users/Data/Metadata/metadataCLAPI_all.csv')
 