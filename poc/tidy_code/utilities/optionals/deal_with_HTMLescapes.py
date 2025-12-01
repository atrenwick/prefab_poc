#WARNING this function is incomplete, and untested : further dev was not necessary

xml_file = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/_depparsed/XML_update/wikidisc_prefabv2_A_00_copy.xml'
output_file = '/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/_depparsed/XML_update/wikidisc_prefabv2_A_00copymod.xml'
from lxml import etree

def preprocess_and_escape_ampersands(xml_file, output_file):
    # Read the content of the XML file
    with open(xml_file, 'r', encoding='utf-8') as file:
        xml_content = file.read()

     # Escape ampersands that are not part of an entity reference
    escaped_xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)', '&amp;', xml_content)

    # Escape '<' characters in attribute values
    escaped_xml_content = re.sub(r'(<[^<]*?="[^"]*?)<([^"]*?")', r'\1&lt;\2', escaped_xml_content)

    # Escape '>' characters in attribute values
    escaped_xml_content = re.sub(r'(<[^<]*?="[^"]*?)>([^"]*?")', r'\1&gt;\2', escaped_xml_content)

    # Write the escaped content back to a new XML file
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(escaped_xml_content)

    # Parse the escaped XML content
    tree = etree.parse(output_file)
    return tree


def modify_xml(xml_file, output_file):
    tree = etree.parse(xml_file)
    root = tree.getroot()

    # Initialize the new id counter
    new_id = 1

    # Iterate over all <s> elements
    for s_element in tqdm(root.findall(".//s")):
      # Preserve the UUID attribute
      uuid = s_element.get("UUID")
      
      # Rename id to seqid
      if "id" in s_element.attrib:
          seqid_value = s_element.attrib.pop("id")
          s_element.set("seqid", seqid_value)
      
      # Set the new id attribute with consecutive integers
      s_element.set("id", str(new_id))
      new_id += 1

    # Save the modified XML to a new file
    tree.write(output_file, pretty_print=True, xml_declaration=True, encoding="UTF-8")


modify_xml(xml_file, output_file)
