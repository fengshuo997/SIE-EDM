# sdm_rdfizer path

ontology_path = './example/ontology/BuildingEnergyOntology-main/buildingenergyontology.rdf'
ontology_path_owl = './example/ontology/BuildingEnergyOntology-main/building_energy_ontology.owl'
ontology_output_path = './example/ontology_all.csv'
ontology_alliri_path = './example/ontology_alliri.csv'
rawdata_path = './example/raw_data/sampledata.csv'
rawdata_source = './example/raw_data/'
reformdata_path = './example/raw_data/reformdata.csv'
splitdata_path = './example/raw_data/splitdata.csv'
similarity_result_path = './example/similarity_result/'
experiment_result_path = './example/experiment_result.csv'


neo4j_container_id = "ecstatic_sutherland"
neo4j_ip = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "9eaJjkwMUv3GRGn"

mysql_port = 3306
mysql_user = "root"
mysql_password = "12345678"
mysql_database_name = "energydata"

matchname_example = ['electricity', 'billing meter']
query_time_example = {'timestamp':'2021/2/2 16:00','range':['2021/2/2 9:00','2021/2/2 23:00']}



columns = ['DATE', 'TIMESTAMP', 'LOCATION', 'ENERGY_SOURCE', 'MEASURE', 'UNIT_OF_MEASURE', 'INTERVAL', 'VALUE']  # only temporary need to change!!!!
example_column_structure = [['ENERGY_SOURCE', 'MEASURE'], ['INTERVAL', 'UNIT_OF_MEASURE', 'DATE', 'VALUE', 'TIMESTAMP']]  # only temporary need to change!!!!
datasize_sample = 400  #[12000, 24000, 36000, 48000, 72000, 84000, 96000, 108000, 120000]  # only temporary need to change!!!!
experiment_columns=['Database Generate time (methode_doubleDatabases) [s]',
                    'Database Generate time (methode_singleDatabase) [s]',
                    'Database size (methode_doubleDatabases) [KB]',
                    'Database size (methode_singleDatabase) [KB]',
                    'Query time-point (methode_doubleDatabases)',
                    'Query time-point (methode_singleDatabase)',
                    'Query time-range (methode_doubleDatabases)',
                    'Query time-range (methode_singleDatabase)',
                    'Query time-average (methode_doubleDatabases)',
                    'Query time-average (methode_singleDatabase)']


