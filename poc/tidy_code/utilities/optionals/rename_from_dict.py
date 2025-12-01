

new_dict = dict([(value.replace('.fr.xml','-PREFABv2.fr.xml'), key.replace("-7200tv8.conllu",'')) for key, value in zip(name_converterDTRS.keys(), name_converterDTRS.values())])
# itemlist = glob.glob(parent_folder + "*.xml")
item = itemlist[-1]
for item in tqdm(glob.glob(parent_folder + "*.xml")):
  shortname = os.path.basename(item)
  foldername = new_dict.get(shortname)
  if foldername and foldername.startswith('T') is False:
    newpath = parent_folder + f'{foldername}/{shortname}'
    print(shortname, newpath)
    os.rename(item, newpath)  

  

count = 0
for chunk in chunks:
  i = len( glob.glob(parent_folder + chunk + "/*PREFABv2*.xml"))
  count =count + i
  if i >1 :
    print(chunk)
