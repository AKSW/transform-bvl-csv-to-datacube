import sys, getopt, os, stat
import pandas as pd


###########################
# GET STATISTICS FROM CSV #
###########################


def execute_query(q, df):
    return q(df)

def create_query_dicts(rows, columns, df):
    '''Helper method for creating all queries'''

    def create_queries(cat, idx):
        return [
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Eingangsbereich-rollstuhlgerecht'] == 'vollständig')]), 'row': rows[idx], 'col': columns[0]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Eingangsbereich-rollstuhlgerecht'] == 'teilweise')]), 'row': rows[idx], 'col': columns[1]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Personenaufzug-rollstuhlgerecht'] == 'ja')]), 'row': rows[idx], 'col': columns[2]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Personenaufzug-vorhanden'] == 'ja')]), 'row': rows[idx], 'col': columns[3]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Toilette-rollstuhlgerecht'] == 'vollständig')]), 'row': rows[idx], 'col': columns[4]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Toilette-rollstuhlgerecht'] == 'teilweise')]), 'row': rows[idx], 'col': columns[5]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Besondere-Hilfestellungen-f-Menschen-m-Hoerbehinderung-vorhanden'] == 'ja')]), 'row': rows[idx], 'col': columns[6]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Besondere-Hilfestellungen-f-Menschen-m-Seebhind-Blinde-vorhanden'] == 'ja')]), 'row': rows[idx], 'col': columns[7]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & (df['Allgemeine-Hilfestellungen-vor-Ort-vorhanden'] == 'ja')]), 'row': rows[idx], 'col': columns[8]},
            {'query': lambda df: len(df[(df['Kategorie'] == cat) & ( (df['Anzahl-Behindertenparkplaetze-v-Einrichtung'] > 0) | (df['Anzahl-Behindertenparkplaetze-auf-hauseigenem-Parkplatz'] > 0) )]), 'row': rows[idx], 'col': columns[9]}
        ]

    categories = ['Bildung', 'Dienst', 'Gastwirtschaft', 'Gesundheit', 'Recht', 'Verbände', 'Verkehr']
    dicts = []
    for idx, cat in enumerate(categories):
        dicts = dicts + create_queries(cat, idx)
    return dicts

def querying(input, directory):
    dataframe = pd.read_csv(input)

    rows = [
        'Gebäude aus Bildung/Freizeit/Kultur',
        'Gebäude aus Dienstleistungen',
        'Gebäude aus Gastronomie/Übernachtung',
        'Gebäude aus Gesundheit/Soziales',
        'Gebäude aus Recht/Verwaltung/Wirtschaft',
        'Gebäude aus Verbände/Vereine',
        'Gebäude aus Verkehr'
    ]
    columns = [
        'Anzahl-mit-Eingangsbereich-vollständig-zugänglich-für-Rollstuhlfahrer',
        'Anzahl-mit-Eingangsbereich-eingeschränkt-zugänglich-für-Rollstuhlfahrer',
        'Anzahl-mit-Aufzug--für-Rollstuhlfahrer-voll-zugänglich',
        'Anzahl-mit-Personenaufzug-vorhanden',
        'Anzahl-mit-Toilette-vollständig-zugänglich-für-Rollstuhlfahrer',
        'Anzahl-mit-Toilette-eingeschränkt-zugänglich-für-Rollstuhlfahrer',
        'Anzahl-mit-Hilfen-für-Hörgeschädigte',
        'Anzahl-mit-Hilfen-für-Blinde-oder-Sehbehinderte-Menschen',
        'Anzahl-mit-speziellen-und-persönlichen-Hilfeleistungen-für-Menschen-mit-Behinderung',
        'Anzahl-mit-markierten-Behindertenparkplätzen'
    ]

    query_dicts = create_query_dicts(rows, columns, dataframe)

    assert len(query_dicts) == len(columns) * len(rows), 'To few queries. The resulting table is not goinig to be fully filled.'
    result_dataframe = pd.DataFrame(index=rows, columns=columns)

    for d in query_dicts:
        r = execute_query(d['query'], dataframe)
        result_dataframe.set_value(d['row'], d['col'], r)

    file_name = 'statistics.csv'
    output_path = os.path.join(directory, file_name)
    result_dataframe.to_csv(output_path)

    return output_path


##################################
# CSV TO DATACUBE TRANSFORMATION #
##################################


from rdflib import Namespace, Graph, RDF, RDFS, Literal, XSD
from slugify import slugify

bvl = Namespace('http://le-online.de/')
qb = Namespace('http://purl.org/linked-data/cube#')

def add_to_graph(tuples, graph):
    for t in tuples:
        graph.add(t)
    return graph

def slugify_text(text):
    return slugify(text, separator='')

def instanciate_dimension_element(instance, ns):
    return ns[slugify_text(instance)]

def create_dimension_elements(mapping, ns):
    dimEls = []
    for l in mapping[0]:
        d = instanciate_dimension_element(l, ns)
        dimEls.append( (d, RDF.type, mapping[1]) )
        dimEls.append( (d, RDFS.label, Literal(l, lang='de')) )
    return dimEls

def create_observation(idx, value, measure, dimEls_t, dims_t, ns, ds):
    ob_instance = ns['obs-' + str(idx)]
    return [
        (ob_instance, RDF.type, qb.Observation),
        (ob_instance, qb.dataSet, ds),
        (ob_instance, measure, Literal(value, datatype=XSD.float)),
        (ob_instance, dims_t[0], dimEls_t[0]),
        (ob_instance, dims_t[1], dimEls_t[1])
    ]

def csv_to_datacube(input, directory):

    dataset_instance = 'dataset'
    dsd_instance = 'dsd'
    cs_instance = 'cd'
    measure_count_instance_cs = 'countCS'
    dimension_feature_instance_cs = 'featureCS'
    dimension_category_instance_cs = 'categoryCS'
    measure_count_instance = 'count'
    dimension_feature_instance = 'feature'
    dimension_category_instance = 'category'

    g = Graph()
    g.add((bvl[dataset_instance], RDF.type, qb.DataSet))
    g.add((bvl[dataset_instance], qb.structure, bvl[dsd_instance]))

    g.add((bvl[dsd_instance], RDF.type, qb.DataStructureDefinition))
    g.add((bvl[dsd_instance], qb.component, bvl[dimension_feature_instance_cs]))
    g.add((bvl[dsd_instance], qb.component, bvl[dimension_category_instance_cs]))
    g.add((bvl[dsd_instance], qb.component, bvl[measure_count_instance_cs]))

    g.add((bvl[dimension_feature_instance_cs], RDF.type, qb.ComponentSpecification))
    g.add((bvl[dimension_feature_instance_cs], qb.dimension, bvl[dimension_feature_instance]))

    g.add((bvl[dimension_category_instance_cs], RDF.type, qb.ComponentSpecification))
    g.add((bvl[dimension_category_instance_cs], qb.dimension, bvl[dimension_category_instance]))

    g.add((bvl[measure_count_instance_cs], RDF.type, qb.ComponentSpecification))
    g.add((bvl[measure_count_instance_cs], qb.measure, bvl[measure_count_instance]))

    g.add((bvl[measure_count_instance], RDF.type, qb.MeasureProperty))
    g.add((bvl[measure_count_instance], RDFS.label, Literal('Anzahl', lang='de')))
    g.add((bvl[measure_count_instance], RDFS.label, Literal('Count', lang='en')))
    g.add((bvl[measure_count_instance], RDFS.range, XSD.decimal))

    g.add((bvl[dimension_feature_instance], RDF.type, qb.DimensionProperty))
    g.add((bvl[dimension_feature_instance], RDFS.label, Literal('Merkmal', lang='de')))
    g.add((bvl[dimension_feature_instance], RDFS.label, Literal('Feature', lang='en')))

    g.add((bvl[dimension_category_instance], RDF.type, qb.DimensionProperty))
    g.add((bvl[dimension_category_instance], RDFS.label, Literal('Kategorie', lang='de')))
    g.add((bvl[dimension_category_instance], RDFS.label, Literal('Category', lang='en')))

    dataframe = pd.read_csv(input, index_col=0)

    mappings = [
        (dataframe.columns, bvl[dimension_feature_instance]),
        (dataframe.index, bvl[dimension_category_instance])
    ]

    for mapping in mappings:
        dimEls = create_dimension_elements(mapping, bvl)
        add_to_graph(dimEls, g)

    for idx, row_t in enumerate(dataframe.iterrows()):
        row_dimEl = instanciate_dimension_element(row_t[0], bvl)
        for idy, i_t in enumerate(row_t[1].iteritems()):
            item_dimEl = instanciate_dimension_element(i_t[0], bvl)
            value = i_t[1]
            ob = create_observation(str(idx) + str(idy), value, bvl[measure_count_instance], (row_dimEl, item_dimEl), (bvl[dimension_category_instance], bvl[dimension_feature_instance]),bvl, bvl[dataset_instance])
            add_to_graph(ob, g)

    file_name = 'datacube.nt'
    output_path = os.path.join(directory, file_name)
    g.serialize(destination=output_path, format='nt')
    return output_path


########
# MAIN #
########


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'i:d:', ['input=', 'directory='])
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)

    input = None
    directory = None

    for o, a in opts:
        if o in ('-i', '--input'):
            input = a
        elif o in ('-d', '--directory'):
            directory = a
        else:
            assert False, 'unhandled option'

    assert input != None, 'no input path given'
    assert directory != None, 'no output directory given'

    result_csv_path  = querying(input, directory)
    print('Finished statistical extraction.')
    result_dc_path = csv_to_datacube(result_csv_path, directory)
    print('Finished transformation to data cube.')

    os.chmod(result_csv_path, stat.S_IROTH | stat.S_IWOTH | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
    os.chmod(result_dc_path, stat.S_IROTH | stat.S_IWOTH | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)

if __name__ == '__main__':
    main()
