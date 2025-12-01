# script to take a specific folder, defined in path, and check the line counts in the versions returned by HOPS. 
# this can be useful as different transformer models reject different sent lengths
errorlog = []
path = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slice_test/C_04_done/'
ref_items = glob.glob(path + "[A-Z]_[0-9][0-9]-104[ab][a-z].csv")
for ref_item in tqdm(ref_items):
 with open(ref_item, 'r', encoding="UTF-8") as r:
  reflinecount = 0
  for line in r.readlines():
   reflinecount +=1
  
  for suffix in ['5262', '5272', '5282', '5287', '5292', '5297']:
    chall_item = ref_item.replace(f'.csv',f'-{suffix}.csv')
    with open(chall_item, 'r', encoding="UTF-8") as c:
     challlinecount = 0
     for line in c.readlines():
      challlinecount +=1
    if challlinecount != reflinecount:
     error = ref_item, reflinecount, chall_item, challlinecount
     errorlog.append(error)
