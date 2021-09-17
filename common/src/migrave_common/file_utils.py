import os
import yaml

def parse_yaml_config(config_file):
    if config_file and os.path.isfile(config_file):
        configs = {}
        with open(config_file, 'r') as infile:
            configs = yaml.safe_load(infile)
    
        return configs 
    else:
        print("Config not found or not given")

