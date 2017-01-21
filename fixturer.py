import arcpy
import os
import numpy
import datetime
from time import strftime
from xxhash import xxh32

uniqueRunNum = strftime("%Y%m%d_%H%M%S")

"""
text change
short change
long change
float change
double change
date change
add row
delete row
add identical
delete identical
alter identical

"""


def get_exp_shape_where_clause(sample_number, shape_len_mean):
    samples = []
    for n in range(sample_number):
        samples.append(numpy.random.exponential(1/float(shape_len_mean)))

    clause = 'Shape_length in ({})'.format(','.join([str(s) for s in samples]))
    return clause


def change_identical_data(data_path, identical_group_field):
    """Make changes to an identical group of features"""
    pass


def copy_base_data(data_path, workspace):
    working_path = arcpy.CopyFeatures_management(data_path,
                                                 os.path.join(workspace, os.path.basename(data_path) + 'X'))
    return working_path


def create_hash_table(hash_table_name, hash_field, workspace):
    hashes_path = arcpy.CreateTable_management(workspace, hash_table_name)[0]
    arcpy.AddField_management(hashes_path, hash_field, 'TEXT', field_length=32)

    return hashes_path


def change_data(data_path):
    # TODO: find a better way to alter the shape
    # new_shape = arcpy.CopyFeatures_management(r'C:\GisWork\sgidchanges\TestCases.gdb\Lines_NewShape',
    #                                           arcpy.Geometry())[0]
    field_changers = {'TextField': lambda value: value[:-1] + 'X' if value else 'X',
                      'ShortField': lambda value: value - 1 if value else -1,
                      'LongField': lambda value: value + 1 if value else 1,
                      'FloatField': lambda value: value - 0.5 if value else -0.5,
                      'DoubleField': lambda value: value + 0.5 if value else 0.5,
                      'DateField': lambda value: value + datetime.timedelta(seconds=1) if value else datetime.now(),
                      'SHAPE@': lambda value: arcpy.CopyFeatures_management(r'C:\GisWork\sgidchanges\TestCases.gdb\Lines_NewShape',
                                                                            arcpy.Geometry())[0]}  # TODO: find a better way to alter the shape

    identical_group_field = 'IdeniticalGroup'
    change_field = 'FieldToChange'
    fields = field_changers.keys() + [identical_group_field, change_field]

    with arcpy.da.UpdateCursor(data_path, fields) as cursor:
        for row in cursor:
            field_to_change = row[fields.index(change_field)]
            if field_to_change in field_changers:
                value = row[fields.index(field_to_change)]
                row[fields.index(field_to_change)] = field_changers[field_to_change](value)
                cursor.updateRow(row)
            else:
                if field_to_change == 'DeleteRow':
                    cursor.deleteRow()
                elif field_to_change == 'UnchangedRow':
                    continue
                else:
                    print 'Uknown field to change: {}'.format(field_to_change)


def get_hash_lookup(hash_path, hash_field):
    hash_lookup = {}
    with arcpy.da.SearchCursor(hash_path, [hash_field, 'OID@']) as cursor:
        for row in cursor:
            hash_value, hash_oid = row
            if hash_value not in hash_lookup:
                hash_lookup[hash_value] = hash_oid  # hash_oid isn't used for anything yet
            else:
                'Hash OID {} is duplicate wtf?'.format(hash_oid)

    return hash_lookup


def populate_hash_table(data_path, fields, hashes_path, hash_field, shape_token=None):
    cursor_fields = list(fields)
    attribute_subindex = -1
    cursor_fields.append('OID@')
    if shape_token:
        cursor_fields.append(shape_token)
        attribute_subindex = -2

    hashes = {}
    with arcpy.da.SearchCursor(data_path, cursor_fields) as cursor, \
            arcpy.da.InsertCursor(hashes_path, hash_field) as ins_cursor:
        for row in cursor:
            hasher = xxh32()  # Create/reset hash object
            hasher.update(str(row[:attribute_subindex]))  # Hash only attributes first
            if shape_token:
                shape_string = row[-1]
                if shape_string:  # None object won't hash
                    hasher.update(shape_string)
                else:
                    hasher.update('No shape')  # Add something to the hash to represent None geometry object
            # Generate a unique hash if current row has duplicates
            digest = hasher.hexdigest()
            while digest in hashes:
                hasher.update(digest)
                digest = hasher.hexdigest()
            hashes[digest] = 0
            # Add the hash to the hash table
            ins_cursor.insertRow((digest, ))

    return hashes_path


def add_hash(data_path, fields, hash_field, shape_token=None):
    field_list = [f.name for f in arcpy.ListFields(data_path)]
    if hash_field not in field_list:
        arcpy.AddField_management(data_path, hash_field, 'TEXT', field_length=32)

    cursor_fields = list(fields)
    exclude_subindex = -1
    if shape_token:
        cursor_fields.append(shape_token)
        exclude_subindex = -2
    cursor_fields.append(hash_field)
    cursor_fields.append('OID@')
    hashes = {}
    with arcpy.da.UpdateCursor(data_path, cursor_fields) as cursor:
        for row in cursor:
            hasher = xxh32()
            hasher.update(str(row[:-2]))
            digest = hasher.hexdigest()
            while digest in hashes:
                hasher.update(digest)
                digest = hasher.hexdigest()
            row[-2] = digest
            hashes[digest] = 0
            cursor.updateRow(row)


def detect_changes(data_path, fields, hashes_path, hash_field, shape_token=None):
    past_hashes = get_hash_lookup(hashes_path, hash_field)

    cursor_fields = list(fields)
    attribute_subindex = -1
    cursor_fields.append('OID@')
    if shape_token:
        cursor_fields.append(shape_token)
        attribute_subindex = -2

    hashes = {}
    with arcpy.da.SearchCursor(data_path, cursor_fields) as cursor:
        for row in cursor:
            hasher = xxh32()  # Create/reset hash object
            hasher.update(str(row[:attribute_subindex]))  # Hash only attributes first
            if shape_token:
                shape_string = row[-1]
                if shape_string:  # None object won't hash
                    hasher.update(shape_string)
                else:
                    hasher.update('No shape')  # Add something to the hash to represent None geometry object
            # Generate a unique hash if current row has duplicates
            digest = hasher.hexdigest()
            while digest in hashes:
                hasher.update(digest)
                digest = hasher.hexdigest()
            if digest not in past_hashes:
                oid = row[attribute_subindex]
                print 'OID {} is an update'.format(oid)


if __name__ == '__main__':
    print '*** Run: {}'.format(uniqueRunNum)
    base_data = r'C:\GisWork\sgidchanges\TestCases.gdb\Lines'
    working_directory = r'C:\GisWork\sgidchanges\temp'
    working_gdb = arcpy.CreateFileGDB_management(working_directory,
                                                 'Working_{}.gdb'.format(uniqueRunNum))[0]
    working_data = copy_base_data(base_data, working_gdb)
    print 'Working data created: {}'.format(working_data)
    hash_field = 'RowHash'
    hash_table = create_hash_table(os.path.basename(str(working_data)) + '_hashes', hash_field, working_gdb)
    print 'Hash table created: {}'.format(hash_table)
    relevant_fields = ['TextField',
                       'ShortField',
                       'LongField',
                       'FloatField',
                       'DoubleField',
                       'DateField']
    # add_hash(working_data, relevant_fields, 'hash1', 'SHAPE@WKT')
    # change_data(working_data)
    # add_hash(working_data, relevant_fields, 'hash2', 'SHAPE@WKT')
    populate_hash_table(working_data, relevant_fields, hash_table, hash_field, 'SHAPE@WKT')
    change_data(working_data)
    detect_changes(working_data, relevant_fields, hash_table, hash_field, 'SHAPE@WKT')

    print 'Completed'
