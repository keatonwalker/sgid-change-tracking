import arcpy
from time import strftime
from time import time
import hashlib
import numpy
import os
import sqlite3
import cProfile
import sys

uniqueRunNum = strftime("%Y%m%d_%H%M%S")
historyDb = r'C:\GisWork\sgidchanges\sgidPolyHash.db'


def getNewAndOutdated(src, historyTable):
    """Get features that are new to hash lookup and features no longer needed."""
    geoHashColumn = 'geohash'
    newHashes = []
    nullGeoList = []
    newRows = []
    hashLookup = getGeometryHashLookup(historyTable)
    # with sqlite3.connect(historyDb) as histDb:
    #     histCursor = histDb.cursor()
    with arcpy.da.SearchCursor(src, ['OID@', 'SHAPE@WKT']) as srcCursor:
        for row in srcCursor:
            oid, wkt = row
            if wkt is None:
                nullGeoList.append((oid))
                continue
            hasher = hashlib.md5(wkt)
            geoHexDigest = hasher.hexdigest()
            if geoHexDigest not in hashLookup:
                newHashes.append((geoHexDigest, oid, uniqueRunNum))
                # listRow = list(row)
                # listRow.append(geoHexDigest)
                # newRows.append(listRow)
            else:
                hashLookup[geoHexDigest] = 1
                # histCursor.execute('''SELECT COUNT(*) FROM {} WHERE {} = "{}"'''.format(
                #                     historyTable,
                #                     geoHashColumn,
                #                     geoHexDigest))
                # if histCursor.fetchone()[0] < 1:
                #     newHashes.append((geoHexDigest, oid, uniqueRunNum))
    if len(newHashes) > 0:
        with sqlite3.connect(historyDb) as histDb:
            histCursor = histDb.cursor()
            histCursor.executemany('''INSERT INTO {} (geohash,oid,rundate)
                                      VALUES (?,?,?)'''.format(historyTable), newHashes)
            histDb.commit()

    oldHashes = [h for h in hashLookup if hashLookup[h] == 0]
    if len(oldHashes) > 0:
        with sqlite3.connect(historyDb) as histDb:
            histCursor = histDb.cursor()
            histCursor.execute('''DELETE FROM {}
                                      WHERE {} IN ("{}")'''.format(historyTable,
                                                                   geoHashColumn,
                                                                   '","'.join(oldHashes)))
            histDb.commit()

    print '\nOld attribute hashes {}'.format(len(oldHashes))
    print 'New attribute hashes {}\n'.format(len(newHashes))


def getNewAndOutdatedWkb(src, historyTable):
    """Get features that are new to hash lookup and features no longer needed."""
    geoHashColumn = 'geohash'
    newHashes = []
    nullGeoList = []
    newRows = []
    hashLookup = getGeometryHashLookup(historyTable)
    # with sqlite3.connect(historyDb) as histDb:
    #     histCursor = histDb.cursor()
    with arcpy.da.SearchCursor(src, ['OID@', 'SHAPE@WKB']) as srcCursor:
        for row in srcCursor:
            oid, wkt = row
            if wkt is None:
                nullGeoList.append((oid))
                continue
            hasher = hashlib.md5(wkt)
            geoHexDigest = hasher.hexdigest()
            if geoHexDigest not in hashLookup:
                newHashes.append((geoHexDigest, oid, uniqueRunNum))
                # listRow = list(row)
                # listRow.append(geoHexDigest)
                # newRows.append(listRow)
            else:
                hashLookup[geoHexDigest] = 1
                # histCursor.execute('''SELECT COUNT(*) FROM {} WHERE {} = "{}"'''.format(
                #                     historyTable,
                #                     geoHashColumn,
                #                     geoHexDigest))
                # if histCursor.fetchone()[0] < 1:
                #     newHashes.append((geoHexDigest, oid, uniqueRunNum))
    with sqlite3.connect(historyDb) as histDb:
        histCursor = histDb.cursor()
        histCursor.executemany('''INSERT INTO {} (geohash,oid,rundate)
                                  VALUES (?,?,?)'''.format(historyTable), newHashes)
        histDb.commit()

    oldHashes = [h for h in hashLookup if hashLookup[h] == 0]
    print '\nOld attribute hashes {}'.format(len(oldHashes))
    print 'New attribute hashes {}\n'.format(len(newHashes))


def getNewAndOutdatedShape(src, historyTable):
    """Get features that are new to hash lookup and features no longer needed."""
    geoHashColumn = 'geohash'
    newHashes = []
    nullGeoList = []
    with sqlite3.connect(historyDb) as histDb:
        histCursor = histDb.cursor()
        with arcpy.da.SearchCursor(src, ['OID@', 'SHAPE@']) as srcCursor:
            for row in srcCursor:
                oid, shape = row

                if shape is None:
                    nullGeoList.append((oid))
                    continue

                hasher = hashlib.md5()
                for part in row[1]:
                    for pnt in part:
                        if pnt:
                            hasher.update(str(round(pnt.X, 4)))
                            hasher.update(str(round(pnt.Y, 4)))
                        else:
                            print("Interior Ring:")

                geoHexDigest = hasher.hexdigest()
                # histCursor.execute('''SELECT COUNT(*) FROM {} WHERE {} = "{}"'''.format(
                #                     historyTable,
                #                     geoHashColumn,
                #                     geoHexDigest))
                # if histCursor.fetchone()[0] < 1:
                #     newHashes.append((geoHexDigest, oid, uniqueRunNum))

        histCursor.executemany('INSERT INTO {} VALUES (?,?,?)'.format(historyTable), newHashes)
        histDb.commit()
    print len(newHashes)
    print len(nullGeoList)


def srcToNumpy(src, fields):
    oids = arcpy.da.FeatureClassToNumPyArray(src,
                                             fields,
                                             skip_nulls=False)
    print oids.shape


def srcCursorThrough(src, fields):
    l = []

    with arcpy.da.SearchCursor(src, fields) as srcCursor:
        # l = list(srcCursor)
        for row in srcCursor:
            hasher = hashlib.md5()
            hasher.update(str(row))
        # histCursor.execute('''SELECT COUNT(*) FROM {} WHERE {} = "{}"'''.format(
        #                     historyTable,
        #                     geoHashColumn,
        #                     geoHexDigest))
        # if histCursor.fetchone()[0] < 1:
        #     newHashes.append((geoHexDigest, oid, uniqueRunNum))
    print len(l)


def getAttributeHashLookup(historyTable):
    hashLookup = {}
    attHashColumn = 'atthash'
    with sqlite3.connect(historyDb) as histDb:
        histCursor = histDb.cursor()
        for attHash in histCursor.execute('''SELECT {} FROM {}'''.format(
                            attHashColumn,
                            historyTable)):
            hashLookup[attHash[0]] = 0
    print 'hashLookup Keys: {}'.format(len(hashLookup.keys()))
    return hashLookup


def getGeometryHashLookup(historyTable):
    hashLookup = {}
    attHashColumn = 'geohash'
    with sqlite3.connect(historyDb) as histDb:
        histCursor = histDb.cursor()
        for attHash in histCursor.execute('''SELECT {} FROM {}'''.format(
                            attHashColumn,
                            historyTable)):
            hashLookup[attHash[0]] = 0
    print 'hashLookup Keys: {}'.format(len(hashLookup.keys()))
    return hashLookup


def getNewAndOutdatedAttributesOld(src, historyTable, fields):
    """Get features that are new to hash lookup and features no longer needed."""
    attHashColumn = 'atthash'
    newHashes = []
    with sqlite3.connect(historyDb) as histDb:
        histCursor = histDb.cursor()
        with arcpy.da.SearchCursor(src, fields) as srcCursor:
            for row in srcCursor:
                hasher = hashlib.md5()
                hasher.update(str(row))

                attHexDigest = hasher.hexdigest()
                histCursor.execute('''SELECT COUNT(*) FROM {} WHERE {} = "{}"'''.format(
                                    historyTable,
                                    attHashColumn,
                                    attHexDigest))
                if histCursor.fetchone()[0] < 1:
                    newHashes.append((attHexDigest, 1, uniqueRunNum))

        histCursor.executemany('''INSERT INTO {} (atthash,oid,rundate)
                                  VALUES (?,?,?)'''.format(historyTable), newHashes)
        histDb.commit()
    print 'New attribute hashes {}'.format(len(newHashes))


def getNewAndOutdatedAttributes(src, historyTable, fields):
    """Get features that are new to hash lookup and features no longer needed."""
    attHashColumn = 'atthash'
    newHashes = []
    hashLookup = getAttributeHashLookup(historyTable)
    with arcpy.da.SearchCursor(src, fields) as srcCursor:
        for row in srcCursor:
            hasher = hashlib.md5()
            hasher.update(str(row))
            attHexDigest = hasher.hexdigest()
            if attHexDigest not in hashLookup:
                newHashes.append((attHexDigest, 1, uniqueRunNum))
            else:
                hashLookup[attHexDigest] = 1
    #
    if len(newHashes) > 0:
        with sqlite3.connect(historyDb) as histDb:
            histCursor = histDb.cursor()
            histCursor.executemany('''INSERT INTO {} (atthash,oid,rundate)
                                      VALUES (?,?,?)'''.format(historyTable), newHashes)
            histDb.commit()
    print '\nNew attribute hashes {}\n'.format(len(newHashes))


def getAttributeHashLookupArc(dest):
    hashLookup = {}
    attHashColumn = 'attHash'
    with arcpy.da.SearchCursor(dest, ['OID@', attHashColumn]) as cursor:
        for row in cursor:
            oid, attHash = row
            if attHash is not None:
                hashLookup[attHash] = 0
    print 'hashLookup Keys: {}'.format(len(hashLookup.keys()))
    return hashLookup


def getHashLookups(dest):
    hashLookup = {}
    geoHashLookup = {}
    attHashColumn = 'attHash'
    geoHashColumn = 'geoHash'
    with arcpy.da.SearchCursor(dest, ['OID@', attHashColumn, geoHashColumn]) as cursor:
        for row in cursor:
            oid, attHash, geoHash = row
            if attHash is not None:
                hashLookup[attHash] = 0
            if geoHash is not None:
                geoHashLookup[geoHash] = 0
    print '\natt hashLookup Keys: {}'.format(len(hashLookup.keys()))
    print 'geo hashLookup Keys: {}\n'.format(len(geoHashLookup.keys()))
    return (hashLookup, geoHashLookup)


def getGeoHash(wkt):
    hasher = hashlib.md5(wkt)
    return hasher.hexdigest()


def updateData(src, dest, historyTable, fields):
    """Get features that are new to hash lookup and features no longer needed."""
    is_table = False
    attHashColumn = 'attHash'
    geoHashColumn = 'geoHash'
    newRows = []
    hashLookup, geoHashLookup = getHashLookups(dest)
    fields.remove('OID@')
    if not is_table:
        fields.append('OID@')
        fields.append('SHAPE@')
    sql_clause = (None, 'ORDER BY OBJECTID')
    orderNum = 0
    with arcpy.da.SearchCursor(src, fields, sql_clause=sql_clause) as srcCursor:
        for row in srcCursor:
            orderNum += 1
            # Shape hash
            wkt = row[-1].WKT
            geoHexDigest = getGeoHash(wkt)
            # Attribute hash
            hasher = hashlib.md5()
            hs = str(row[:-2]) + str(orderNum)
            hasher.update(hs)
            # hasher.update(orderNum)
            attHexDigest = hasher.hexdigest()
            # Check for new feature
            if attHexDigest not in hashLookup:
                listRow = list(row)
                listRow.extend([geoHexDigest, attHexDigest])
                newRows.append(listRow)
            elif geoHexDigest not in geoHashLookup:
                listRow = list(row)
                listRow.extend([geoHexDigest, attHexDigest])
                newRows.append(listRow)
            else:
                hashLookup[attHexDigest] = 1
                geoHashLookup[geoHexDigest] = 1

    oldGeoHashes = [h for h in geoHashLookup if geoHashLookup[h] == 0]
    oldHashes = [h for h in hashLookup if hashLookup[h] == 0]
    if len(oldGeoHashes) > 0 or len(oldHashes) > 0:
        whereSelection = """{} IN ('{}')""".format(attHashColumn,
                                                   "','".join(oldHashes))
        # print whereSelection
        print 'remove'
        with arcpy.da.UpdateCursor(dest, [attHashColumn], whereSelection) as uCursor:
            for row in uCursor:
                uCursor.deleteRow()

    if len(newRows) > 0:
        fields.extend([geoHashColumn, attHashColumn])
        print 'insert'
        with arcpy.da.InsertCursor(dest, fields) as iCursor:
            for row in newRows:
                iCursor.insertRow(row)

    print '\nOld attribute hashes {}'.format(len(oldHashes))
    print 'Old geometry hashes   {}'.format(len(oldGeoHashes))
    print 'New rows              {}\n'.format(len(newRows))


def _filter_fields(lst):
    '''
    lst: String[]
    returns: String[]
    Filters out fields that mess up the update logic.
    '''

    new_fields = []
    for fld in lst:
        if fld == 'OBJECTID':
            new_fields.insert(0, 'OID@')
        elif not _is_naughty_field(fld):
            new_fields.append(fld)

    return new_fields


def _is_naughty_field(fld):
    return 'SHAPE' in fld.upper() or fld.upper() in ['GLOBAL_ID', 'GLOBALID'] or fld.startswith('OBJECTID_')


class Crate(object):

    def __init__(self, src, dest):
        self.destination = dest
        self.source = src

if __name__ == '__main__':
    src = r'C:\GisWork\sgidchanges\TestFeatures.gdb\SrcRailroads'
    dest = r'C:\GisWork\sgidchanges\TestFeatures.gdb\RailRoads'
    crate = Crate(src,
                  dest)
    historyTable = 'railshistory'
    fields = fields = set([fld.name for fld in arcpy.ListFields(crate.destination)]) & set([fld.name for fld in arcpy.ListFields(crate.source)])
    fields = _filter_fields(fields)
    fields.sort()

    #updateData(src, dest, historyTable, fields)

    pr = cProfile.Profile()
    pr.enable()
    updateData(src, dest, historyTable, fields)
    pr.create_stats()
    pr.print_stats('cumulative')
