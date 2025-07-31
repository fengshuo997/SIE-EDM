import owlready2
import pandas as pd

def topic_annonation(ontology_path, similarity_column_property_path, similarity_column_class_path):
    """Perform similarity analysis through column, and match to get related properties. And identify the domain by this properties
    Input: query sentence [str] and ontology [rdfg:Graph] Output: query result [dataframe]"""
    selected_properties = []
    selected_classes_column = []
    onto = owlready2.get_ontology(ontology_path).load()
    similarity_property_class = pd.read_csv(similarity_column_property_path)
    similarity_column_class = pd.read_csv(similarity_column_class_path)
    columnlist = set(similarity_property_class['Dataname'])
    for i in columnlist:
        ontologyname = similarity_property_class[(similarity_property_class.Dataname == i)].sort_values(by=['score'], ascending=False)
        selected_properties.append(ontologyname.head(10)['Ontologyname'].array.tolist()) #change the number to select the first several result
        ontologyname_column = similarity_column_class[(similarity_column_class.Dataname == i)].sort_values(by=['score'],ascending=False)
        selected_classes_column.append(ontologyname_column.head(10)['Ontologyname'].array.tolist())
    selected_properties = sum(selected_properties, [])
    selected_properties = set(selected_properties)
    selected_classes = find_relative_class(selected_properties, onto)
    selected_classes['Count'] = selected_classes['Count']*0.5 #factor of property
    selected_classes_column = sum(selected_classes_column, [])
    selected_classes_column = pd.DataFrame(zip(selected_classes_column, selected_classes_column), columns=['Names', 'Names for count'])
    selected_classes_column = selected_classes_column.groupby(['Names'])['Names for count'].count().reset_index(name='Count').sort_values(['Count'], ascending=False).reset_index(drop=True)
    selected_classes_column['Count'] = selected_classes_column['Count'] * 0.5 #factor of column
    selected_classes = pd.concat([selected_classes_column, selected_classes], ignore_index=True).groupby(['Names'])['Count'].sum().sort_values(ascending=False)
    return selected_classes

def find_relative_class(selected_name, ontology):
    """Identify the domain or range of properties or columns
        Input: selected property [list] and ontology [rdfg:Graph] Output: domian and range of property [dataframe]"""
    selected_classes = []
    for i in selected_name:
        properties_ot = ontology.search(iri = "*" + i)
        for property in properties_ot:
            if property.is_a[0].name!=('ObjectProperty' or 'DatatypeProperty' or 'AnnotationProperty'):
                continue
            if property.domain==[] or property.range==[]:
                continue
            if (type(property.domain[0])==owlready2.entity.ThingClass):
                selected_classes.append(property.domain[0].name)
            else:
                for domain in list(property.domain[0].Classes):
                    selected_classes.append(domain.name)

            if (type(property.range[0])==owlready2.entity.ThingClass):
                selected_classes.append(property.range[0].name)
            else:
                for range in list(property.range[0].Classes):
                    selected_classes.append(range.name)
    selected_classes = pd.DataFrame(zip(selected_classes, selected_classes), columns =['Names', 'Names for count'])
    selected_classes = selected_classes.groupby(['Names'])['Names for count'].count().reset_index(name='Count').sort_values(['Count'],ascending=False).reset_index(drop=True)
    return selected_classes

def columntype_annotation(similarity_column_property_path):
    cta = {}
    similarity_property_class = pd.read_csv(similarity_column_property_path)
    columnlist = set(similarity_property_class['Dataname'])
    for i in columnlist:
        i = i.rstrip()  # drop the last whitespace
        ontologyname = similarity_property_class[(similarity_property_class.Dataname == i)].sort_values(by=['score'],ascending=False)
        cta[i] = ontologyname.head(1)['Ontologyname'].array.tolist()
    return cta
