import rdflib




class Ontology:
    def __init__(self, ontology_path):
        self.ontology_path = ontology_path
        return

    def get_ontology(self):
        """Get the ontology"""
        g = rdflib.Graph()
        g.parse(self.ontology_path)
        return g