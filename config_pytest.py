# -*- coding: utf-8 -*-
"""
Created on Fri Jan  2 10:35:13 2026

@author: breadsp2

python -m pytest config_pytest.py
"""

import yaml
import pytest
import os


@pytest.fixture
def load_config():
    """ Open the config file and make sure in correct format """
    config_file_name = 'C:/Users/breadsp2/Desktop/Memgraph/config/memgraph-config_v2.yml'
    with open(config_file_name) as f:
        config = yaml.safe_load(f)
    assert 'Config' in config, "'Config' Header is expected and is missing"
    assert config is not None
    
    data = {"config": config["Config"]}
    return data

@pytest.fixture
def test_prop_file(load_config):   
    config = load_config["config"] 
    assert "prop_file" in config,  "prop_file must be a parameter in the config file"
    assert os.path.exists(config["prop_file"]), f"Path does not exist: {config['prop_file']}"
   
    with open(config["prop_file"]) as f:
        prop_file = yaml.safe_load(f)
    
    assert "Properties" in prop_file, "Expecting Properties as the primary header of this file"
    assert "id_fields" in prop_file["Properties"]
    assert isinstance(prop_file["Properties"]["id_fields"], dict), "must be a dictionary of primary keys"
    
    return prop_file["Properties"]["id_fields"].keys()


#def test_required_fields(load_config):
#    assert 'temp_folder' in config
#    assert 's3_bucket' in config
#    assert 's3_folder'in config
#    assert 'loading_mode' in config 

    
def test_cheat_mode(load_config):
    config = load_config["config"]  
    assert 'cheat_mode' in config 
    assert config['cheat_mode'] in [True, False]
    
def test_dry_run(load_config):
    config = load_config["config"]  
    assert 'dry_run' in config 
    assert config['dry_run'] in [True, False]
    
def test_wipe_db(load_config):
    config = load_config["config"]  
    assert 'wipe_db' in config 
    assert config['wipe_db'] in [True, False]
    
def test_no_backup(load_config):
    config = load_config["config"]  
    assert 'no_backup' in config 
    assert config['no_backup'] in [True, False]
    
    if config['no_backup'] == True:
        assert 'backup_folder' in config
        assert os.path.exists(config['backup_folder']), f"Path does not exist: {config['backup_folder']}"
          
def test_no_confirmation(load_config):
    config = load_config["config"]  
    assert 'no_confirmation' in config 
    assert config['no_confirmation'] in [True, False]
    
def test_split_transactions(load_config):
    config = load_config["config"]  
    assert 'split_transactions' in config 
    assert config['split_transactions'] in [True, False]
    
def test_max_violations(load_config):
    config = load_config["config"]  
    assert 'max_violations' in config 
    assert config['max_violations'] >= 0
    

def test_data_file(load_config):   
    config = load_config["config"] 
    assert os.path.exists(config["dataset"]), f"Path does not exist: {config['dataset']}"
 
def test_schema_files(load_config, test_prop_file):  
    config = load_config["config"]  
    test_prop_file = list(test_prop_file)
   
    assert 'schema' in config, "Schema must exist in the config file"
    assert isinstance(config["schema"], list), "Schema must be in a list format"   
    assert len(config["schema"]) == 2, "Schema must have 2 elements, model_file and property_file" 
    for curr_file in config["schema"]:
        assert os.path.exists(curr_file), f"Path does not exist: {curr_file}"

    with open(config["schema"][0]) as f:
        model_file = yaml.safe_load(f)
        assert "Nodes" in model_file, "Model file not in the proper format, Exepecting a Nodes Header"
        for i in model_file["Nodes"]:
            assert "Props" in model_file["Nodes"][i] 
        prop_list = [model_file["Nodes"][i]["Props"] for i in model_file["Nodes"]]
        prop_list = [item for sublist in prop_list for item in sublist]
        
    with open(config["schema"][1]) as f:
        defination_file = yaml.safe_load(f)
        assert "PropDefinitions" in defination_file, "Defination File not in the proper format, Exepecting a PropDefinitions Header"
        defination_list = list(defination_file["PropDefinitions"].keys())
     
    diff_1 = list(set(prop_list).difference(defination_list))
    diff_2 = list(set(defination_list).difference(prop_list))
    
    assert len(diff_1) == 0, "Elements in model file but no defination defined" 
    assert len(diff_2) == 0, "Elements in defination file but no node in model" 