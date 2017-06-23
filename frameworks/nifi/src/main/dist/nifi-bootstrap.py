#!/usr/bin/env python

from lxml import etree as ET

import os



DEFAULT_PATH_PREFIX='.'
DEFAULT_NIFI_VERSION='1.3.0'
DEFAULT_NODE_COUNT='1'
DEFAULT_TASK_NAME='nifi-0-node'
DEFAULT_FRAMEWORK_HOST='nifi.autoip.dcos.thisdcos.directory'

NIFI_AUTHORIZERS_XML_FILE_TEMPLATE = '{}/config-templates/authorizers.xml'

NIFI_NODE_KEY_TEMPLATE = 'Node Identity {}'
NIFI_NODE_TEXT_TEMPLATE = 'CN=nifi-{}-node.{}, OU=NIFI'

mesos_sandbox = os.getenv('MESOS_SANDBOX', DEFAULT_PATH_PREFIX)
nifi_version = os.getenv('NIFI_VERSION', DEFAULT_NIFI_VERSION)
nifi_node_count = os.getenv('NODE_COUNT', DEFAULT_NODE_COUNT)
nifi_task_name = os.getenv('TASK_NAME', DEFAULT_TASK_NAME)
nifi_framework_host = os.getenv('FRAMEWORK_HOST', DEFAULT_FRAMEWORK_HOST)

nifi_authorizers_xml_file = NIFI_AUTHORIZERS_XML_FILE_TEMPLATE.format(
        mesos_sandbox)

parser = ET.XMLParser(remove_blank_text=True)
tree = ET.parse(nifi_authorizers_xml_file, parser)
root = tree.getroot()

authorizer = root.find('authorizer')

for i in range(0, int(nifi_node_count)):
    element = ET.SubElement(authorizer, 
                               'property', 
                               dict(name=NIFI_NODE_KEY_TEMPLATE.format(
                                   str(i))))
    element.text = NIFI_NODE_TEXT_TEMPLATE.format(
            str(i),
            nifi_framework_host)

with open(nifi_authorizers_xml_file, 'w') as nifi_authorizer_xml:
    nifi_authorizer_xml.write(ET.tostring(root, 
        pretty_print=True, 
        encoding='unicode'))

