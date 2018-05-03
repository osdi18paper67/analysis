# Handy functions used by many analysis notebooks

import os, sys
import string
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from IPython.display import IFrame
from IPython.display import Markdown, display
from IPython.display import FileLink, FileLinks

def column2val(df, column):
    """ For a given dataframe and column name, return the only unique value in the column;
     If there are multiple unique values, print an error and return the first one.
    """
    vals = df[column].unique() 
    if len(vals) != 1: 
        sys.stderr.write("Error: multiple unique values are found in column: %s" % column)
        sys.stderr.flush() 
    return vals[0]

def df2csv(df, filename, dest_dir, index=False, verbose=False):
    """ Save given dataframe to specified file in the specified directory """
    dest = os.path.join(dest_dir, filename) 
    df.to_csv(dest, index=index)
    if verbose: 
        print "Saved dataframe %d record(s) to: %s" % (len(df), dest)

def subset_df(df, filters, verbose=False):
    """ Subset a given dataframe based on keys (column names) and values (selected values 
    in those columns) specified in dict filters """

    tmp = df.copy()
    msg = "Subsetting dataframe. Initial # of rows: %d" % len(df)
    for k,v in filters.iteritems():
        tmp = tmp[tmp[k] == v]
        msg += ", %s==%s: %d" % (str(k), str(v), len(tmp))
    if verbose:
        print msg
    return tmp

def get_cmap(df, column, palette="bright", custom=None):
    """ Assign unique colors to unique values in the specified column in the dataframe.
    If custom dict is provided, add it to the resulting color map. 
    """
    color_labels = df[column].unique().tolist() 
    rgb_values = sns.color_palette(palette, len(color_labels))
    color_map = dict(zip(color_labels, rgb_values))
    if custom:
        color_map.update(custom)
    return color_map

def get_cmap_from_list(vals, palette="bright", custom=None):
    """ Same as above but for a given list of values rather than dataframe  """
    rgb_values = sns.color_palette(palette, len(vals))
    color_map = dict(zip(vals, rgb_values))
    if custom:
        color_map.update(custom)
    return color_map

def plot_show_and_save(fig, filename="", dest_dir="", show_only=False):
    """ Save (optionally) and display a given matplotlib figure """ 
    if show_only:
        display(fig) 
    else: 
        dest = os.path.join(dest_dir, filename) 
        fig.savefig(dest, bbox_inches='tight')
        display(fig)
        display(FileLink(dest))

    plt.close(fig)
    
def process_disk(db):
    raw_disk_all = pd.merge(db['disk_results'], db["env_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"])
    raw_disk_all["disk_name"] = raw_disk_all["device"].apply(lambda x: x.rstrip(string.digits) if "nvm" not in x else x[:-2])
    raw_disk_all = pd.merge(raw_disk_all, db['disk_info'], 
                               on=["run_uuid", "timestamp", "nodeuuid", "disk_name", "nodeid"])  

    raw_disk_all["disk_size"] = raw_disk_all["disk_size"].apply(lambda x: x.lstrip())
    
    exclude_ids = []
    for idx, grp in raw_disk_all.groupby(["site", "hw_type", "device", "disk_type", "disk_model", "disk_size"]):
    
        if len(grp) < 200:
            exclude_ids.extend(grp.index.values)

    disk_all = raw_disk_all.drop(exclude_ids, inplace=False)
    
    # Exclude measurements obtaine on or after April 2, 2018
    disk_all = disk_all[disk_all["timestamp"] <= 1522636071]

    # Exclude failed runs
    disk_all = disk_all[disk_all["run_success"] != 0]
    
    # Exclude run with inconsistent software
    disk_all = disk_all[disk_all["gcc_ver"] == "5.4.0"]
    
    return raw_disk_all, disk_all

def process_memory(db):
    
    raw_mem_all = pd.merge(db['mem_results'], db["env_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"])

    # Add mem info
    raw_mem_all = pd.merge(raw_mem_all, db['membench_info'], on=["run_uuid", "timestamp", "nodeuuid", "nodeid"])  

    # Exclude measurements obtaine on or after April 2, 2018
    mem_all = raw_mem_all[raw_mem_all["timestamp"] <= 1522636071]

    # Exclude failed runs
    mem_all = mem_all[mem_all["run_success"] != 0]
    
    # Exclude run with inconsistent software
    mem_all = mem_all[mem_all["gcc_ver"] == "5.4.0"]
    
    return raw_mem_all, mem_all

def is_rack_local(nodeid):
    if 'clnode' in nodeid:
        if int(nodeid[-3:]) > 40 and int(nodeid[-3:]) < 97:
            return True
        else:
            return False
    elif 'c220' in nodeid:
        if 'c220g1-0306' in nodeid:
            if int(nodeid[-2:]) > 20:
                return False
            else:
                return True
        else:
            return False
    elif 'ms' in nodeid:
        return 'ms09' in nodeid

def process_network(db):
  
    lat_all = subset_df(pd.merge(db['ping_results'], db["env_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"]), {"run_success":1})
    lat_all = pd.merge(lat_all, db["ping_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"])
    lat_all = pd.merge(lat_all, db["network_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"]) 

    bw_all = subset_df(pd.merge(db['iperf3_results'], db["env_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"]), {"run_success":1})
    bw_all = pd.merge(bw_all, db["iperf3_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"])
    bw_all = pd.merge(bw_all, db["network_info"], on=["run_uuid", "nodeid", "nodeuuid", "timestamp"])

    lat_all["test"] = "latency"
    lat_all["directionality"] = ['forward'] * len(lat_all)

    bw_all["test"] = "bandwidth"
    bw_all["directionality"] = bw_all["reverse"].map({True: "reverse", False: "forward"})

    #display(pd.concat([lat_all, bw_all]))

    raw_net_all = pd.concat([lat_all, bw_all])
    

    # Exclude measurements obtaine on or after April 2, 2018
    net_all = raw_net_all[raw_net_all["timestamp"] <= 1522636071]

    # Exclude failed runs
    net_all = net_all[net_all["run_success"] != 0]
    
    # Exclude run with inconsistent software
    net_all = net_all[net_all["gcc_ver"] == "5.4.0"]
    
    net_all["rack_local"] = net_all["nodeid"].apply(is_rack_local)
    
    return raw_net_all, net_all
