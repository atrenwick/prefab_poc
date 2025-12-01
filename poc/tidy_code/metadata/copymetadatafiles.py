## script to make duplicate all the metadata files from a dump to a new destination

import shutil
import os

def get_all_file_extensions(folder_path):
  """Gets a list of all the file extensions of all folders and subfolders of a given folder path, recursively.

  Args:
    folder_path (str): The folder path to get the file extensions from.

  Returns:
    list[str]: A list of all the unique file extensions.
  """

  # Create a set to store the file extensions. This will prevent duplicates.
  file_extensions = set()

  # Recursively iterate over the folder path and get all the file extensions.
  for root, dirs, files in os.walk(folder_path):
    for file in files:
      # Get the file extension.
      file_extension = os.path.splitext(file)[1]

      # Add the file extension to the set.
      file_extensions.add(file_extension)

  # Convert the set to a list and return it.
  return list(file_extensions)


def copy_all_files_to_target_folder(source_folder_path, target_folder_path, exclude_extensions):
  """Copies all the files from a source folder to a target folder, recursively, except for files with extensions in the list exclude_extensions. The folder structure of the source file is not reproduced in the target folder.

  Args:
    source_folder_path (str): The path to the source folder.
    target_folder_path (str): The path to the target folder.
    exclude_extensions (list[str]): A list of file extensions to exclude from copying.
  """

  # Create the target folder if it doesn't exist.
  if not os.path.exists(target_folder_path):
    os.makedirs(target_folder_path)

  # Recursively iterate over the source folder and copy all the files to the target folder, except for files with extensions in the list exclude_extensions.
  for root, dirs, files in os.walk(source_folder_path):
    for file in files:
      file_extension = os.path.splitext(file)[1]
      if file_extension not in exclude_extensions:
        source_file_path = os.path.join(root, file)
        target_file_path = os.path.join(target_folder_path, file)
        shutil.copy(source_file_path, target_file_path)




folder_path = '/Users/Data/ANR_PREFAB/Corpus_ortolang/pfc/'
file_extensions = get_all_file_extensions(folder_path)


target_folder_path = '/Users/Data/ANR_PREFAB/Data/Metadata/PFC/'
exclude_extensions = ['.wav','.mp4','mp4','.mp3','mp3', 'wav', '.png','png','m4a','.m4a','.xsl','xsl',  '.avi', '.textgrid', '.rtf', '.wmv', '.pdf', 'png','jpg']

source_folder_path= folder_path
copy_all_files_to_target_folder(source_folder_path, target_folder_path, exclude_extensions)


