parent_folder = '/Users/Data/ANR_PREFAB/CorpusPREFAB/Romans/Romans_V3_archive/by_corpus/aligned/TS/'
chunks = [subfolder.replace(parent_folder, '')[:-1] for subfolder in glob.glob(parent_folder + "/*/")]

for chunk in tqdm(chunks):
  files_to_move = glob.glob(parent_folder + chunk + "*.*")
  for file in files_to_move:
    filename = os.path.basename(file)
    newpath = parent_folder + f"{chunk}/"
    new_full = newpath + filename
    # if os.path.exists(new_full):
    #   dest_size = os.path.getsize(new_full)
    #   source_size = os.path.getsize(files_to_move[0])
    os.rename(file, new_full)
