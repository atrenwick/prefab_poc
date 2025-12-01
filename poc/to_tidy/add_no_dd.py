# script to add type="non" to romans if there is not already a type attribute
import xml.etree.ElementTree as ET
import chardet, glob
from tqdm import tqdm
input_file_name = 'C:/Users/Data/V3_romans/phraseorom_prefabV3/GEN/POL.fr.ADAM-PREFABv3.xml'
input_files = glob.glob('C:/Users/CorpusRomans/V3_romans/phraseorom_prefabV3/POL/*v3.xml')
for input_file_name in tqdm(input_files):
  output_file_name = input_file_name.replace('v3.xml','v3b.xml')

  with open(input_file_name, "rb") as f:
      byteS = f.read()

  encod = chardet.detect(byteS)["encoding"]
  content = byteS.decode(encod)

  # deleting namespace
  content = re.sub(r'xmlns[^\s\>]+','',content)
  content = content.strip()
  # get xml root, get sents, and if type not in s attributes, add type as attribute, value = non
  xml_root = ET.ElementTree(ET.fromstring(content))
  sentences=xml_root.findall('.//s')
  for sent in sentences:
    if 'type' not in sent.attrib:
      sent.attrib['type']="non"
  xml_root.write(output_file_name, xml_declaration=True, default_namespace=None, encoding='utf-8')
