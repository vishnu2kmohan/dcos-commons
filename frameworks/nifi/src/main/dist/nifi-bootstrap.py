#!/usr/bin/env python

from lxml import etree as ET

import os


mesos_sandbox = os.getenv('MESOS_SANDBOX', '.')
nifi_version = os.getenv('NIFI_VERSION', '1.3.0')
nifi_node_count = os.getenv('NODE_COUNT', 3)
nifi_authorizers_xml_file = '{}/nifi-scheduler/authorizers.xml'.format(
        mesos_sandbox, 
        nifi_version)

parser = ET.XMLParser(remove_blank_text=True)
tree = ET.parse(nifi_authorizers_xml_file, parser)
root = tree.getroot()

authorizer = root.find('authorizer')

for i in range(0, int(nifi_node_count)):
    element = ET.SubElement(authorizer, 
                               'property', 
                               dict(name='Node Identity {}'.format(str(i))))
    element.text = 'nifi-{}'.format(str(i))

with open(nifi_authorizers_xml_file, 'w') as nifi_authorizer_xml:
    nifi_authorizer_xml.write(ET.tostring(root, 
        pretty_print=True, 
        encoding='unicode'))

