import xml.etree.ElementTree as ET
import xml.dom.minidom
import os


mesos_sandbox = os.getenv('MESOS_SANDBOX', '.')
nifi_node_count = os.getenv('NIFI_NODE_COUNT', 3)

tree = ET.parse('{}/authorizers.xml'.format(mesos_sandbox))
root = tree.getroot()

authorizer = root.find('authorizer')

for i in range(1, int(nifi_node_count)+1):
    element = ET.SubElement(authorizer, 'property', dict(name='Node Identity {}'.format(str(i))))
    element.text = 'nifi-{}'.format(str(i))


xml = xml.dom.minidom.parseString(ET.dump(root))

pretty_xml_as_string = xml.toprettyxml()
print(pretty_xml_as_string)
