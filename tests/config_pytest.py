# -*- coding: utf-8 -*-
"""
Created on Fri Jan  2 10:35:13 2026

@author: breadsp2

python -m pytest config_pytest.py --config-file testing_config.yml

coverage run -m pytest config_pytest.py --config-file testing_config.yml
"""

import yaml
import pytest
import os

from props import Props
from icdc_schema import ICDC_Schema
from bento.common.utils import load_plugin
from bento.common.utils import removeTrailingSlash
from neo4j import GraphDatabase, Driver
from data_loader import DataLoader

def pytest_addoption(parser):
    parser.addoption(
        "--config-file", action="store", default=None, help="Path to the config file to use for tests"
    )

@pytest.fixture
def data_file_path(request):
    return request.config.getoption("--config-file")


@pytest.fixture
def test_process_data(data_file_path):
    if data_file_path is None:
        pytest.fail("No data file path provided. Use --data-file argument.")
       
    return data_file_path


@pytest.fixture
def load_config(data_file_path):
    """ Open the config file and make sure in correct format """
#    config_file_name = 'C:/Users/breadsp2/Desktop/Memgraph/config/memgraph-config_v2.yml'
    config_file_name = data_file_path
    with open(config_file_name, 'r') as f:
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


@pytest.fixture
def neo4j_driver(load_config) -> Driver:
    """Fixture to create and verify a Neo4j driver connection."""
    config = load_config["config"] 
    assert "neo4j" in config, "Missing Neo4j Credentials"
    neo4j_config = config["neo4j"]
    
    neo4j_config["neo4j_uri"] = removeTrailingSlash(neo4j_config["neo4j_uri"])
    driver = GraphDatabase.driver(
        neo4j_config["neo4j_uri"],
        auth=(neo4j_config["neo4j_user"], neo4j_config["neo4j_password"]),
        encrypted=False
        )    
    # verify_connectivity() raises an exception if connection fails
    driver.verify_connectivity() 
    return driver

def test_neo4j_connection(neo4j_driver):
    """A test that uses the fixture and passes if connectivity is verified."""
    # The fixture will raise an exception and fail the test if the connection fails
    # If this point is reached, the connection is successful
    assert neo4j_driver is not None
    
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
    

#def test_data_file(load_config):   
#    config = load_config["config"] 
#    assert os.path.exists(config["dataset"]), f"Path does not exist: {config['dataset']}"
 
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
  
    
@pytest.fixture   
def chek_prop_and_schema(load_config):
    config = load_config["config"]  
    props = Props(config.prop_file)
    loader = ICDC_Schema(config.schema_files, props)
    assert type(loader, ICDC_Schema)
    return loader
   
@pytest.fixture 
def prepare_plugin(load_config, chek_prop_and_schema):
    config = load_config["config"]  
    schema = chek_prop_and_schema

    if not config.params:
        config.params = {}
    config.params['schema'] = schema
    return load_plugin(config.module_name, config.class_name, config.params)

    
def check_data_loader(load_config, chek_prop_and_schema, neo4j_driver):    
    config = load_config["config"]  
    schema = chek_prop_and_schema
    plugins = []
    if len(config.plugins) > 0:
        for plugin_config in config.plugins:
            plugins.append(prepare_plugin(plugin_config, schema))
     
    assert isinstance(DataLoader(neo4j_driver, schema, plugins), DataLoader)


    