# note this script is no longer necessary as a new function, working at chunk level, takes the unhopsable or un-udpipeable chunks, processed then with StanzaGSD and then exports the output as HOPS or UDPipe would render then. 
## this function here, however, is preserved here to allow a more fine-grained analysis with HOPS, rather than sending the whole conllu chunk to StanzaGSD.
# function to combine sliceA, cut, sliceB of HOPSparse, where A and C are parses from HOPS, and B is
# a GSD-stanza parse of an unhopsable chunk

# how to use this script
# to use this script, run HOPS over a file. If the file has a sentence that HOPS says is too long, HOPS
# will return the parsed file up to the chunk that contains the overly-long sentence.
# open the output from HOPS, and get sentID of last sentence processed.
# duplicate HOPS input file 2x to make sliceB and sliceC.
# remove the parsed chunk from B, and then find the overly long sentence, and select everything below it
# copy this to slice C, which will be passed to HOPS.
# then get sentID of sent or sents that are too long, and copy these
# open GSD-120.conllu parse to get the data that will be passed to the lemmatizer masquerading as HOPS output
# copy the sentences to a blank document, do a replace of all meta lines with null, then copy to excel sheet
# open the ab initio HOPS input, and copy the index, tokens and HOPSids to excel
# ensure that the alignent is correct : copying from either source has chance of problem with quotes: 
# quotes need to be escaped prior to sending to excel, to ensure lines are correctly realised
# copy HOPSid column from HOPSinput paste to the GSD chunk, then copy the 10 columns to txt doc
# unescape any quotations that were escaped to ensure correct alignment
## save the annotations as slice B. the single set of annotations will be passed to each slice, regardless of model/suffix
# slice C is passed to HOPS : ensure that HOPS gets the commmands to process this
# once this is all done and HOPS has returned the output of sliceC, we can glue everything back together wiht this script

# designate generic path head element
path = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/'
# designate generic letter head element which will be repeated
letter="F"
# designate generic index element which will be repeated
index = "00"
# set value to look for sliceB
slicetarged = "sliceB"
for suffix in ['5262', '5272', '5282', '5287', '5292', '5297']:
 # suffix = "5262"5262 
 #startp = f'//Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/O/O_00-104-{suffix}.csv'
 startp = f'/{path}{letter}/{letter}_{index}-104-{suffix}.csv'
 endp = f'/{path}{letter}/{letter}_{index}-104-sliceC-{suffix}.csv'
 #G_00-104_sliceC-5262
 thisslice = f'{path}{letter}/{letter}_{index}-104-{slicetarged}.csv'
 output_file = f'{path}{letter}/{letter}_{index}-{suffix}.csv'
        
 with open(output_file, 'w', encoding="UTF-8") as w:
  with open(startp, 'r', encoding="UTF-8") as s:
   start = s.read()
 
  w.write(start)
  with open(thisslice, 'r',encoding="UTF-8") as f:
   sliceIN = f.read()
  
  slicemod = re.sub(r"\t\t\t\t\t\t\t\t\t\n", r"\n", sliceIN)
  slicemod = re.sub(r"^\t\t\n", r"\n", slicemod)
  slicemod = re.sub(r"^\n\n\n", r"\n\n", slicemod)
  slicemod = slicemod + "\n\n"
  w.write(slicemod)
  
  with open(endp, 'r', encoding="UTF-8") as e:
   end = e.read()
  w.write(end)
def compareLeng():

chunk = 'E_00'
hops_abinitio = f'/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slicecheck/{chunk}-104-sliceCv2.csv'
hops_outputfile = f'/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/slicecheck/{chunk}-104-sliceCv2-5272.csv'
with open(hops_outputfile, 'r', encoding="UTF-8") as o:
 outlinecount = 0
 for line in o.readlines():
  outlinecount +=1
  
with open(hops_abinitio, 'r', encoding="UTF-8") as h:
 input_linecount = 0
 for line in h.readlines():
  input_linecount +=1

print(input_linecount, outlinecount)  

