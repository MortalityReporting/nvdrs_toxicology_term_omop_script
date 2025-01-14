# Reading an excel file using Python

import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import connect, DatabaseError
import psycopg2.extras as extras
from configparser import ConfigParser
from datetime import datetime

vocab = "vocab"
name_of_table = "{}.concept".format(vocab)
name_of_concept_relationship = "{}.concept_relationship".format(vocab)

# column_name_map = {'Element OMOP Concept ID': 'concept_id',
#                    'Element OMOP Concept Name': 'concept_name',
#                    'Vocabulary': 'vocabulary',
#                    'Element OMOP Concept Code': 'concept_code'}

parser = ConfigParser(interpolation=None)


def config(filename='config_nvdrs.ini'):
    parser.read(filename)


def get_config(section):
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the file'.format(section))

    return db


def connect_db():
    """ Connect to the PostgreSQL database server """
    c = None

    try:
        # read connection parameters
        params = get_config('postgresql')

        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        c = connect(**params)

        # create a cursor
        cursor = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # execute a statement
        print("PostgreSQL database version:")
        cursor.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cursor.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cursor.close()
    except (Exception, DatabaseError) as error:
        print(error)

    return c


def get_next_concept_id(cursor):
    cursor.execute("SELECT MAX(concept_id) FROM %s;" % name_of_table)
    max_val = cursor.fetchone()[0]
    return max_val + 1


def find_by_id(cursor, concept_id):
    cursor.execute("SELECT * FROM %s WHERE concept_id = %s;" % (name_of_table, concept_id))
    concept_table = cursor.fetchone()
    print("Found: {}".format(concept_table))
    return concept_table


def find_by_relationship_ids(cursor, concept_id_1, concept_id_2, relationship_id):
    cursor.execute("SELECT * FROM %s WHERE concept_id_1 = %s and concept_id_2 = %s and relationship_id = '%s';" % (name_of_concept_relationship, concept_id_1, concept_id_2, relationship_id))
    concept_relationship_table = cursor.fetchone()
    print('Found: {}'.format(concept_relationship_table))
    return concept_relationship_table


def find_concept_by_name_vocabulary(cursor, concept_name, vocabulary_id):
    cursor.execute("SELECT * FROM %s WHERE concept_name = '%s' and vocabulary_id = '%s';" % (name_of_table, concept_name, vocabulary_id))
    concept_found = cursor.fetchone()
    print('Found: {}'.format(concept_found))
    return concept_found


def update_concept(c, row):
    cursor = c.cursor()

    if 'standard_concept' in row:
        sql = "UPDATE %s.concept SET concept_name = '%s', domain_id = '%s', concept_code = '%s', concept_class_id = '%s', "\
            "standard_concept = '%s', vocabulary_id = '%s', invalid_reason = null WHERE concept_id = %s" % (vocab, str(row['concept_name']).replace("'", "''"),
            str(row['domain_id']).replace("'", "''"), str(row['concept_code']), str(row['concept_class_id']),
            str(row['standard_concept']), str(row['vocabulary_id']), row['concept_id'])
    else:
        sql = "UPDATE %s.concept SET concept_name = '%s', domain_id = '%s', concept_code = '%s', concept_class_id = '%s', " \
              "vocabulary_id = '%s', invalid_reason = null WHERE concept_id = %s" % (
              vocab, str(row['concept_name']).replace("'", "''"),
              str(row['domain_id']).replace("'", "''"), str(row['concept_code']), str(row['concept_class_id']),
              str(row['vocabulary_id']), row['concept_id'])

    cursor.execute(sql)
    c.commit()


def setup_alias(c, concept_id_2, concept_id_1):
    cursor = c.cursor()
    concept_r = find_by_relationship_ids(cursor, concept_id_1, concept_id_2, 'Alias of')
    if concept_r is None:
        sql = "INSERT INTO %s.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date) VALUES (%s, %s, '%s', '%s', '%s')" % (
            vocab, concept_id_1, concept_id_2, 'Alias of', '2024-12-01', '2099-12-31')
        cursor.execute(sql)
        sql = "INSERT INTO %s.concept_relationship (concept_id_2, concept_id_1, relationship_id, valid_start_date, valid_end_date) VALUES (%s, %s, '%s', '%s', '%s')" % (
            vocab, concept_id_1, concept_id_2, 'Has Alias', '2024-12-01', '2099-12-31')
        cursor.execute(sql)
        c.commit()


def setup_class(c, concept_id_1, concept_id_2):
    cursor = c.cursor()

    concept_r = find_by_relationship_ids(cursor, concept_id_1, concept_id_2, 'Classified as')
    if concept_r is None:
        sql = "INSERT INTO %s.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date) VALUES (%s, %s, '%s', '%s', '%s')" % (
            vocab, concept_id_1, concept_id_2, 'Classified as', '2024-12-01', '2099-12-31')
        cursor.execute(sql)
        c.commit()


def setup_subcategory(c, concept_id_1, concept_id_2):
    cursor = c.cursor()

    concept_r = find_by_relationship_ids(cursor, concept_id_1, concept_id_2, 'Category of')
    if concept_r is None:
        sql = "INSERT INTO %s.concept_relationship (concept_id_2, concept_id_1, relationship_id, valid_start_date, valid_end_date) VALUES (%s, %s, '%s', '%s', '%s')" % (
            vocab, concept_id_1, concept_id_2, 'Category of', '2024-12-01', '2099-12-31')
        cursor.execute(sql)
        sql = "INSERT INTO %s.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date) VALUES (%s, %s, '%s', '%s', '%s')" % (
            vocab, concept_id_1, concept_id_2, 'Has Category', '2024-12-01', '2099-12-31')
        cursor.execute(sql)

        c.commit()


def setup_part_of_subcategory(c, concept_id_1, concept_id_2):
    cursor = c.cursor()

    concept_r = find_by_relationship_ids(cursor, concept_id_1, concept_id_2, 'Category of')
    if concept_r is None:
        sql = "INSERT INTO %s.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date) VALUES (%s, %s, '%s', '%s', '%s')" % (
            vocab, concept_id_1, concept_id_2, 'Part of', '2024-12-01', '2099-12-31')
        cursor.execute(sql)

        c.commit()


def insert_concept(c, row):
    cursor = c.cursor()

    # Get next available concept id
    concept_id = row['concept_id']
    print('Next concept id: %s' % concept_id)
    
    # Retreive default table update
    concept = get_config('default')
    concept['concept_id'] = concept_id
    concept['concept_name'] = row['concept_name'].replace("'", r"''")
    concept['domain_id'] = row['domain_id']
    concept['vocabulary_id'] = row['vocabulary_id']
    concept['concept_class_id'] = row['concept_class_id']
    if 'standard_concept' in row:
        concept['standard_concept'] = row['standard_concept']
    concept['concept_code'] = row['concept_code']

    # execute_batch(cur, insert_str, default_params)
    columns = ", ".join(list(concept.keys()))
    values = list(concept.values())

    sql = "INSERT INTO %s.concept (%s) VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', null)" % (vocab, str(columns), values[0], str(values[1]), str(values[2]), str(values[3]), str(values[4]), str(values[5]), str(values[6]), '2021-01-01', '2099-12-31')

    cursor.execute(sql)
    c.commit()

    return concept


if __name__=="__main__":
    config()

    # Create db connection
    c = connect_db()
    cursor = c.cursor()

    e = get_config('excel')

    # Read excel
    wb_substance = pd.read_excel(e['name'], sheet_name=e['sheet_substance'])
    wb_category = pd.read_excel(e['name'], sheet_name=e['sheet_category'])
    wb_subcategory = pd.read_excel(e['name'], sheet_name=e['sheet_subcategory'])

    # get the start OMOP concept ID to use.
    base_concept_id = int(get_config('database')['start_index'])
    actual_start_concept_id = get_next_concept_id(cursor)
    if actual_start_concept_id < base_concept_id:
        start_concept_id = base_concept_id
    else:
        start_concept_id = actual_start_concept_id

    # First load categories to concept table
    i = 0
    for index, excel_row in wb_category.iterrows():
        # First get the category name
        category_name = excel_row['SubstanceCategoryName'].strip()

        searched = find_concept_by_name_vocabulary(cursor, category_name, 'NvdrsToxCategories')
        if searched is not None:
            concept_id = searched[0]
        else:
            concept_id = start_concept_id + i
            i = i+1

        concept = {'concept_id': concept_id,
               'concept_name': category_name,
               'domain_id': 'Drug',
               'vocabulary_id': 'NvdrsToxCategories',
               'concept_class_id': 'NvdrsToxCategories',
               'standard_concept': 'C',
               'concept_code': concept_id}

        print(concept['concept_id'], concept['concept_name'], concept['domain_id'], concept['vocabulary_id'], str(concept['concept_class_id']), str(concept['standard_concept']), str(concept['concept_code']))

        if searched is not None and concept_id == searched[0]:
            # Update
            update_concept(c, concept)
        else:
            # insert
            concept = insert_concept(c, concept)

    # add subcategory and set up the subcategory relationship with loaded category above
    for index, excel_row in wb_subcategory.iterrows():
        # First get the subcategory name
        subcategory_name = excel_row['SubstanceTypeName (Subcategory)'].strip()

        searched = find_concept_by_name_vocabulary(cursor, subcategory_name, 'NvdrsToxCategories')
        if searched is not None:
            subcategory_concept_id = searched[0]
        else:
            subcategory_concept_id = start_concept_id + i
            i = i+1

        concept = {'concept_id': subcategory_concept_id,
               'concept_name': subcategory_name,
               'domain_id': 'Drug',
               'vocabulary_id': 'NvdrsToxCategories',
               'concept_class_id': 'NvdrsToxCategories',
               'standard_concept': 'C',
               'concept_code': subcategory_concept_id}

        print(concept['concept_id'], concept['concept_name'], concept['domain_id'], concept['vocabulary_id'], str(concept['concept_class_id']), str(concept['standard_concept']), str(concept['concept_code']))

        if searched is not None and subcategory_concept_id == searched[0]:
            # Update
            update_concept(c, concept)
        else:
            # insert
            concept = insert_concept(c, concept)

        category_name = excel_row['SubstanceCategoryName']
        if excel_row['SubstanceTypeId'] == 88:
            continue

        category_concept = find_concept_by_name_vocabulary(cursor, category_name, 'NvdrsToxCategories')
        if category_concept is not None:
            category_concept_id = category_concept[0]
            setup_subcategory(c, category_concept_id, subcategory_concept_id)
        else:
            print("Error: Cannot find Category: %s".format(category_name))
            exit(1)

    # Add substances
    for index, excel_row in wb_substance.iterrows():
        substance_name = excel_row['SubstanceName'].strip()
        substance_alias = excel_row['SubstanceDescription'].strip()

        aliases = None
        if excel_row['SubstanceName'] != excel_row['SubstanceDescription']:
            # setup the aliases
            aliases = excel_row['SubstanceDescription'].split('/')

        searched = find_concept_by_name_vocabulary(cursor, substance_name.replace("'", r"''"), 'NvdrsToxSubstances')
        if searched is not None:
            concept_id = searched[0]
        else:
            concept_id = start_concept_id + i

        concept = {'concept_id': concept_id,
               'concept_name': substance_name,
               'domain_id': 'Drug',
               'vocabulary_id': 'NvdrsToxSubstances',
               'concept_class_id': 'NvdrsToxSubstances',
               'concept_code': concept_id}

        if searched is not None and concept_id == searched[0]:
            # Update
            update_concept(c, concept)
        else:
            # insert
            concept = insert_concept(c, concept)
            i = i+1

        # classification setup
        class_name = excel_row['Category (Class)'].strip()
        class_concept = find_concept_by_name_vocabulary(cursor, class_name.replace("'", r"''"), 'NvdrsToxCategories')
        if class_concept is not None:
            class_concept_id = class_concept[0]
            setup_class(c, concept_id, class_concept_id)
        else:
            print("Error: Cannot find Category: %s for substance classification" % class_name)
            exit(1)

        # sub-category setup
        subcategory_name = excel_row['Subcategory (Type)'].strip()
        subcategory_concept = None

        if subcategory_name != 'Not applicable':
            subcategory_concept = find_concept_by_name_vocabulary(cursor, subcategory_name.replace("'", r"''"), 'NvdrsToxCategories')
            if subcategory_concept is not None:
                subcategory_concept_id = subcategory_concept[0]
                setup_part_of_subcategory(c, concept_id, subcategory_concept_id)

        if aliases is not None:
            orig_id = concept_id
            for alias in aliases:
                alias = alias.strip()
                if substance_name == alias:
                    continue

                searched = find_concept_by_name_vocabulary(cursor, alias.replace("'", r"''"), 'NvdrsToxSubstances')
                if searched is not None:
                    alias_concept_id = searched[0]
                else:
                    alias_concept_id = start_concept_id + i
                    i = i + 1

                concept = {'concept_id': alias_concept_id,
                           'concept_name': alias,
                           'domain_id': 'Drug',
                           'vocabulary_id': 'NvdrsToxSubstances',
                           'concept_class_id': 'NvdrsToxSubstances',
                           'concept_code': alias_concept_id}

                if searched is not None and alias_concept_id == searched[0]:
                    # Update
                    update_concept(c, concept)
                else:
                    # insert
                    concept = insert_concept(c, concept)

                setup_alias(c, orig_id, alias_concept_id)


    if c is not None:
        c.close()
        print('Database connection closed.')

