import yaml
from sling import Replication

#From a YAML file
replication = Replication(file_path='sling_easy.yaml')
replication.run()

# # # Or load into object
# with open('sling_easy.yaml') as file:
#   config = yaml.load(file, Loader=yaml.FullLoader)

# replication = Replication(**config)

# replication.run()