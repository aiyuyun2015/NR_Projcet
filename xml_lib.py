import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import os, re
import pandas as pd


class StringHandler:
    @classmethod
    def replace_special_val(cls, val):
        if val == '46011F':
            val = 0
        elif val == '46001F':
            val = 1
        elif val == '46011f':
            val = 2
        elif val is None:
            val = 999
        elif isinstance(val, float) and np.isnan(val):
            val = 999
        return val


def get_patterns():
    # yello configs
    pattern0 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.Capabilities\.MaxUEsServed'
    pattern1 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.RAN\.Mobility\.ConnMode\.NR\.A[^\.]MeasureCtrl\.[^\.]+\.A[^\.]Threshold[^\.]RSRP'
    pattern2 = r'Services\.FAPService\.[^\.]+\.CellConfig.[^\.]+\.NR\.RAN\.Mobility\.ConnMode\.NR\.[^\.][^\.]MeasureCtrl\.[^\.]+\.Hysteresis'
    pattern3 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.RAN\.Mobility\.ConnMode\.NR\.A3MeasureCtrl\.[^\.]+\.A3OffsetRSRP'
    pattern4 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.RAN\.Mobility\.ConnMode\.IRAT\.B1MeasureCtrl.[^\.]+.B[^\.]ThresholdEUTRARsrp'
    pattern5 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.RAN\.Mobility\.ConnMode\.IRAT\.B1MeasureCtrl\.[^\.]+\.Hysteresis'
    pattern6 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.RAN\.NeighborList\.NRCell\.[^\.]+\.CIO'
    pattern7 = r'Services\.FAPService\.[^\.]+\.CellConfig\.[^\.]+\.NR\.RAN\.NeighborList\.NRCell\.[^\.]+\.PLMNID'
    return [pattern0, pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7]

def is_leaf(element):
    return len(element) == 0


def get_node_tag_and_attrib(node):
    if len(node.attrib) == 1:
        assert len(node.attrib) == 1
        keys = list(node.attrib.keys())
        key_val = ".{" + node.attrib[keys[0]] + "}"
    else:
        key_val = ''
    return node.tag + key_val


def tree_to_strs(node, concated, tags, texts):
    if node.tag:
        if concated == '':
            concated = get_node_tag_and_attrib(node)
        else:
            concated = concated + '.' + get_node_tag_and_attrib(node)
        # print(concated)

    if is_leaf(node):
        tags.append(concated)
        texts.append(node.text)
        return
    else:
        for i in node:
            tree_to_strs(i, concated, tags, texts)


def re_extract(string, pattern=None):
    matches = re.findall(pattern, string)
    if matches:
        return matches[0]

def extract_time(path):
    pattern = r'A_NR_(\d{8})\.(\d{4})\+(\d{4})-(\d{4})\+(\d{4})'
    # Extract date and time using regex
    match = re.search(pattern, path)
    if match:
        date = match.group(1)  # Extract date
        start_time = match.group(2)  # Extract start time
        end_time = match.group(4)  # Extract end time
        return f"{date}:{start_time}-{end_time}"
    else:
        return 0

def find_xml_files(directory):
    xml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.xml') or file.endswith('.csv'):
                xml_files.append(os.path.join(root, file))
    return sorted(xml_files)

def dict_to_str(dictionary):
    return ', '.join([f'{str(key)}:{str(value)}' for key,value in dictionary.items()])

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f'{parent_key}{sep}{k}' if parent_key else k
    
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_url(element):
    return element.tag.split("}")[1]

def print_xml_structure2(element, result):
    # leaves
    if len(element)==0:
        if element.tag:
            if element.text:
                result[element.tag] = element.text.strip()
            else:
                result[element.tag] = ''
        return
    
    # if not the leaves, get
    if element.tag:
        if element.attrib:
            k = element.tag + dict_to_str(element.attrib)
        else:
            k = element.tag
        tmp = {}
        result[k] = tmp
    for child in element:
        print_xml_structure2(child, tmp)




def convert_measInfo(measInfo, debug=False):
    jobs = []
    granPeriods = []
    repPeriods = []
    measTypes = []
    measValues = []
    measTypes_attribs = []
    measValues_attribs = []

    for i in measInfo:
        url = get_url(i)
        #print(i, get_url(i))
        if url=='measType':
            measTypes.append(i.text)
            measTypes_attribs.append(i.attrib)
        elif url=='measValue':
            for val in i:
                measValues.append(val.text)
                measValues_attribs.append(val.attrib)
        elif url=='job':
            jobs.append(i.attrib)
        elif url=='granPeriod':
            granPeriods.append(i.attrib)
        elif url=='repPeriod':
            repPeriods.append(i.attrib)

    assert len(measTypes)==len(measValues)
    assert measTypes_attribs==measValues_attribs # p=1 must be matched!
    assert len(jobs)==1
    assert len(granPeriods)==1
    assert len(repPeriods)==1

    data = pd.DataFrame([[]])
    # job id
    job = jobs[0]
    k, v = 'jobId', job['jobId']
    data[k] = v

    granPeriod = granPeriods[0]
    for k, v in granPeriod.items():
        data[f'grandPeriod_{k}'] = v
        
    repPeriod = repPeriods[0]
    for k, v in repPeriod.items():
        data[f'repPeriod_{k}'] = v

    for i in range(len(measValues)):

        measType = measTypes[i]
        measValue = measValues[i]
        if debug:
            data[f'measType_{i}'] = measType
            data[f'measValue_{i}'] = measValue
        else:

            data[measType] = measValues[i]

    return data