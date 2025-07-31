import pandas as pd
import owlready2
import os



def get_ot_alliri(ontology_path, ontology_alliri_path):
    """Get all classes, individuals, object properties, data properties and annotation properties  of ontology
    Input: path of ontology file [str] Output: all iri of components in ontology [list]"""
    ot_alliri = []
    ot_classiri = []
    ot_proertyiri = []
    if not os.path.exists(ontology_alliri_path):
        onto = owlready2.get_ontology(ontology_path).load()
        ontology_alliri = pd.DataFrame()
        print('#########generating csv file of all iri in ontology############')
        for i in list(onto.classes()):
            ot_alliri.append(i.iri)
            ot_classiri.append(i.iri)
        for i in list(onto.individuals()):
            ot_alliri.append(i.iri)
            ot_classiri.append(i.iri)
        for i in list(onto.object_properties()):
            ot_alliri.append(i.iri)
            ot_proertyiri.append(i.iri)
        for i in list(onto.data_properties()):
            ot_alliri.append(i.iri)
            ot_proertyiri.append(i.iri)
        for i in list(onto.annotation_properties()):
            ot_alliri.append(i.iri)
            ot_proertyiri.append(i.iri)
        ontology_alliri['ot_alliri'] = pd.Series(ot_alliri)
        ontology_alliri['ot_classiri'] = pd.Series(ot_classiri)
        ontology_alliri['ot_proertyiri'] = pd.Series(ot_proertyiri)
        ontology_alliri.dropna()
        ontology_alliri.to_csv(ontology_alliri_path)
    else:
        ontology_alliri = pd.read_csv(ontology_alliri_path)
        ot_alliri = ontology_alliri['ot_alliri']
        ot_classiri = ontology_alliri['ot_classiri']
        ot_proertyiri = ontology_alliri['ot_proertyiri']
    return ot_alliri, ot_classiri, ot_proertyiri

def get_namespaces(ot_alliri):
    """Get all namespaces of ontology
    Input: all iri/url of ontology [list] Output: all namespaces of ontology [list]"""
    result = []
    for i in range(0, len(ot_alliri)):
        prefix = ot_alliri[i]
        if '#' in prefix:
            head, sep, tail = prefix.partition('#')
            result.append(head + sep)
        elif '/' in prefix:
            separator = '/'
            prefix = prefix[::-1]
            prefix = prefix.split(separator, 1)[1]
            prefix = prefix[::-1]
            result.append(str(prefix + '/'))
    result = list(set(result))
    result.sort()
    return result

def get_entities(ot_alliri):
    """Get all entities name of ontology
    Input: all iri/url of ontology [list] Output: all entities names of ontology [list]"""
    result = []
    for i in range(0, len(ot_alliri)):
        prefix = ot_alliri[i]
        if type(prefix) == str:
            if '#' in prefix:
                head, sep, tail = prefix.partition('#')
                result.append(tail)
            elif '/' in prefix:
                separator = '/'
                prefix = prefix[::-1]
                prefix = prefix.split(separator, 1)[0]
                prefix = prefix[::-1]
                result.append(str(prefix))
    result = list(set(result))
    result.sort()
    return result



def generate_query(value="test",namespaces=[]):
    """Generate query sentence with query value and all namespace
    Input: query value [str] and all namespaces [list] Output: query sentence [str]"""
    query = []
    prefix = {" ":"Prefix  :<https://w3id.org/sbeo>",
              "dc":"Prefix dc:<http://purl.org/dc/elements/1.1/>",
              "olo":"Prefix olo:<http://purl.org/ontology/olo/core#>",
              "owl":"Prefix owl:<http://www.w3.org/2002/07/owl#>",
              "rdf":"Prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
              "xml":"Prefix xml:<http://www.w3.org/XML/1998/namespace>",
              "xsd":"Prefix xsd:<http://www.w3.org/2001/XMLSchema#>",
              "foaf":"Prefix foaf:<http://xmlns.com/foaf/0.1/>",
              "rdfs":"Prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>",
              "sbeo":"Prefix sbeo:<https://w3id.org/sbeo#>",
              "seas":"Prefix seas:<https://w3id.org/seas/>",
              "skos":"Prefix skos:<http://www.w3.org/2004/02/skos/core#>",
              "sosa":"Prefix sosa:<http://www.w3.org/ns/sosa/>",
              "vann":"Prefix vann:<http://purl.org/vocab/vann/>",
              "schema":"Prefix schema:<http://schema.org/>",
              "dcterms":"Prefix dcterms:<http://purl.org/dc/terms/>"}
    n = 0
    for i in namespaces:
        for x in list(prefix.values()):
            if i in x:
                namespaces.remove(i)
    for i in namespaces:
        pfx_value = str('Prefix pfx_' + str(n) + ':<' + i + ">")
        prefix['pfx_' + str(n)] = pfx_value
        n += 1
    prefix_all = ("Prefix  :<https://w3id.org/sbeo>\n"
                  "Prefix dc:<http://purl.org/dc/elements/1.1/>\n"
                  "Prefix olo:<http://purl.org/ontology/olo/core#>\n"
                  "Prefix owl:<http://www.w3.org/2002/07/owl#>\n"
                  "Prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
                  "Prefix xml:<http://www.w3.org/XML/1998/namespace>\n"
                  "Prefix xsd:<http://www.w3.org/2001/XMLSchema#>\n"
                  "Prefix foaf:<http://xmlns.com/foaf/0.1/>\n"
                  "Prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>\n"
                  "Prefix sbeo:<https://w3id.org/sbeo#>\n"
                  "Prefix seas:<https://w3id.org/seas/>\n"
                  "Prefix skos:<http://www.w3.org/2004/02/skos/core#>\n"
                  "Prefix sosa:<http://www.w3.org/ns/sosa/>\n"
                  "Prefix vann:<http://purl.org/vocab/vann/>\n"
                  "Prefix schema:<http://schema.org/>\n"
                  "Prefix dcterms:<http://purl.org/dc/terms/>")

    subject = "?subject"
    object = "?object"
    limit = 20
    variable = object
    for i in prefix:
        if prefix[i] in prefix_all:
            pass
        else:
            prefix_all = prefix_all + '\n' + prefix[i]
        where = str(i+':'+value[0]+' rdfs:subClassOf '+variable + ' .')
        #where = str(variable + ' rdfs:subClassOf ' + i + ':' + value + ' .')
        knows_query = str(prefix_all+'\n'
                          +'SELECT '+variable+'\n'
                          +'WHERE{ '+'\n'
                          +where+'\n'
                          +'} Limit '+str(limit))
        query.append(knows_query)
    return query

def do_query(query,graph):
    """Implement query sentence in ontology
    Input: query sentence [str] and ontology [rdfg:Graph] Output: query result [dataframe]"""
    result = pd.DataFrame()
    for i in query:
        qres = graph.query(i)
        qres_pd = pd.DataFrame(qres, columns=qres.vars)
        if str(type(qres_pd.values.all()))== "<class 'rdflib.term.URIRef'>":
            result = pd.concat([result,qres_pd])
        result.index = pd.RangeIndex(len(result.index))
    return result