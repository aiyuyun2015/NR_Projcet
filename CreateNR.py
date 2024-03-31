import pandas as pd
import numpy as np
from xml_lib import (print_xml_structure2,
                     convert_measInfo,
                     get_url, find_xml_files,
                     extract_time, tree_to_strs,
                     get_patterns, re_extract,
                     StringHandler,
                     )
import xml.etree.ElementTree as ET
import os
import MyTools as mt
from multiprocessing import Pool
def convert_xml_core(path):
    if 'confdb' in path:
        return
    ### ofile
    bn = os.path.basename(path)
    bn = bn.replace('.xml', '.csv')
    ofile = os.path.join('./results', bn)
    odirnmae = os.path.dirname(ofile)
    mt.safemkdir(odirnmae)
    if os.path.exists(ofile):
        print(f"{ofile} exists skip..")
        return
    tree = ET.parse(path)
    root = tree.getroot()
    measInfo = root[1][1]
    dfs = []
    for i in root[1]:
        if get_url(i) == 'measInfo':
            df = convert_measInfo(i, debug=False)
            dfs.append(df)
    res = pd.concat(dfs)
    res['file_time'] = extract_time(path)
    res.set_index('jobId').to_csv(ofile)
    print(f"write to: {ofile}")


def convert_xmls(xml_files):
    with Pool(30) as p:
        p.map(convert_xml_core, xml_files)


def extract_yellow():
    # 1. Get config
    file_path = './data/四川质优/巴中平昌县信义政府办公楼/confdb_v2.xml'
    # 2. Output ycols
    ofile = './results/ycols.csv'
    tree = ET.parse(file_path)
    root = tree.getroot()
    tags = []
    texts = []
    tree_to_strs(root[9], '', tags, texts)
    # 2. Formatting
    # Use tree data to create df, formatting line
    df = pd.DataFrame({'tags': tags, 'texts': texts})
    # 3.
    ydata = pd.DataFrame([[]])
    for pattern in get_patterns():
        for i in range(len(df)):
            s = df.iloc[i]['tags']
            val = df.iloc[i]['texts']
            s = re_extract(s, pattern)
            if s:
                StringHandler.replace_special_val(val)
                print(s, val)
                ydata[s] = val
    ydata.to_csv(ofile)


def main():
    # 1. Create NR files
    p = './data/四川质优/巴中平昌县信义政府办公楼'
    xml_files = find_xml_files(p)
    convert_xmls(xml_files)
    # 2. Extract yellow rows




if __name__=="__main__":
    main()