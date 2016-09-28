import arcpy
from time import strftime
from time import time
import hashlib
import numpy
import os
import sqlite3

uniqueRunNum = strftime("%Y%m%d_%H%M%S")


def listPolygonsInSgid(connection):
    arcpy.env.workspace = connection
    polygonList = arcpy.ListFeatureClasses(feature_type='Polygon')
    return polygonList


def getHashValuesFromWKB(workspace, featureClass):
    """Hash the well known binary of each shape."""
    hashList = []
    with arcpy.da.SearchCursor(os.path.join(workspace, featureClass),
                               ['OID@', 'SHAPE@WKB']) as cursor:
        for row in cursor:
            oid, wkb = row
            if wkb is None:
                continue
            hasher = hashlib.md5(wkb)
            hexDigest = hasher.hexdigest()
            hashList.append((featureClass, oid, hexDigest, uniqueRunNum))

    return hashList


def getHashRow(workspace, featureClass):
    """Hash the well known binary, featureClass name, and oid of each feature."""
    hashList = []
    nullGoeList = []
    logDbPath = r'C:\GisWork\sgidchanges\sgidPolyHash.db'
    featureAccessTime = time()
    with arcpy.da.SearchCursor(os.path.join(workspace, featureClass),
                               ['OID@', 'SHAPE@WKB']) as cursor:
        for row in cursor:
            oid, wkb = row
            if wkb is None:
                nullGoeList.append((featureClass, oid))
                continue
            hasher = hashlib.md5(wkb)
            geoHexDigest = hasher.hexdigest()
            hasher.update(str(featureClass))
            hasher.update(str(oid))
            uniqueHexDigest = hasher.hexdigest()
            hashList.append((featureClass, oid, geoHexDigest, uniqueHexDigest, uniqueRunNum))
    featureAccessTime = time() - featureAccessTime
    with sqlite3.connect(logDbPath) as logDb:
        logCur = logDb.cursor()
        if len(nullGoeList) > 0:
            logCur.executemany('INSERT INTO nullpgons VALUES (?,?)', nullGoeList)
        logCur.execute('INSERT INTO accesstime VALUES (?,?,?,?)',
                       ('search|hash', featureClass, featureAccessTime, uniqueRunNum))
        logDb.commit()

    return hashList


def hashAllVerticesNumPy(featureClass):
    """Explode to points."""
    hasher = hashlib.md5()
    # arrayTime = time()
    xyArray = arcpy.da.FeatureClassToNumPyArray(featureClass,
                                                ('SHAPE@X', 'SHAPE@Y'),
                                                explode_to_points=True,
                                                skip_nulls=True)
    # print '{}'.format(time() - arrayTime)
    xyArray = numpy.around(xyArray.view(numpy.float64), 4)
    xyArray.sort()
    byteArray = xyArray.view(numpy.uint8)
    hasher.update(byteArray)
    del xyArray
    del byteArray
    return hasher.hexdigest()


def addHashedFeaturesToDb(workspace, featureClassList, table):
    pgonDb = sqlite3.connect(r'C:\GisWork\sgidchanges\sgidPolyHash.db')
    with pgonDb:
        pgonHashCursor = pgonDb.cursor()
        for p in featureClassList:
            curTime = time()
            print p
            hashes = getHashRow(workspace,
                                p)
            pgonHashCursor.executemany('INSERT INTO {} VALUES (?,?,?,?,?)'.format(table), hashes)
            pgonDb.commit()

            print ('{}'.format(round(time() - curTime, 4)))


def findChangedFeatures(workspace, featureClass, hashDb):
    pgonDb = sqlite3.connect(hashDb)
    oldHashes = {}
    with pgonDb:
        hashCursor = pgonDb.cursor()
    #     for row in hashCursor.execute('SELECT hash FROM polygons WHERE feature=?', (featureClass, )):
    #         oldHashes[row[0]] = 0
    # print len(oldHashes.keys())
        with arcpy.da.SearchCursor(os.path.join(workspace, featureClass),
                                   ['OID@', 'SHAPE@WKB']) as cursor:
            for row in cursor:
                oid, wkb = row
                if wkb is None:
                    continue
                hasher = hashlib.md5(wkb)
                hexDigest = hasher.hexdigest()
                hashCursor.execute('SELECT hash FROM polygons WHERE hash = ?', (hexDigest, ))


def getOwnerGeoTypeNumbers(workspace, featureClassList):
    owners = {}
    for fc in featureClassList:
        owner = fc.split('.')[1]
        with arcpy.da.SearchCursor(os.path.join(workspace, fc),
                                   ['OID@']) as cursor:
            featureCount = len(list(cursor))
            if owner not in owners:
                owners[owner] = featureCount
            else:
                owners[owner] += featureCount

    return owners


def listFcIntoDb(connection):
    db = r'C:\GisWork\sgidchanges\sgidPolyHash.db'
    table = 'fcnames'
    db = sqlite3.connect(db)
    arcpy.env.workspace = connection
    with db:
        print 'polygons'
        polygonList = arcpy.ListFeatureClasses(feature_type='Polygon')
        print len(polygonList)
        polygonList = [(p, p.split('.')[1], p.split('.')[2], 'polygon') for p in polygonList]
        cursor = db.cursor()
        cursor.executemany('INSERT INTO fcnames VALUES (?,?,?,?)', polygonList)
        db.commit()

        print 'polylines'
        polylineList = arcpy.ListFeatureClasses(feature_type='Polyline')
        print len(polylineList)
        polylineList = [(p, p.split('.')[1], p.split('.')[2], 'polyline') for p in polylineList]
        cursor = db.cursor()
        cursor.executemany('INSERT INTO fcnames VALUES (?,?,?,?)', polylineList)
        db.commit()

        print 'points'
        pointList = arcpy.ListFeatureClasses(feature_type='Point')
        print len(pointList)
        pointList = [(p, p.split('.')[1], p.split('.')[2], 'point') for p in pointList]
        cursor = db.cursor()
        cursor.executemany('INSERT INTO fcnames VALUES (?,?,?,?)', pointList)
        db.commit()


def getGeoTypeByOwner(hashDb):
    db = sqlite3.connect(hashDb)
    pgonOwners = None
    plineOwners = None
    pointOwners = None
    with db:
        cursor = db.cursor()
        print 'pgon'
        cursor.execute('SELECT featureclass FROM fcnames WHERE geotype=?', ('polygon', ))
        pgonFcs = [row[0] for row in cursor.fetchall()]
        pgonOwners = getOwnerGeoTypeNumbers(sgidConnection, pgonFcs)
        print 'pline'
        cursor.execute('SELECT featureclass FROM fcnames WHERE geotype=?', ('polyline', ))
        plineFcs = [row[0] for row in cursor.fetchall()]
        plineOwners = getOwnerGeoTypeNumbers(sgidConnection, plineFcs)
        print 'point'
        cursor.execute('SELECT featureclass FROM fcnames WHERE geotype=?', ('point', ))
        pointFcs = [row[0] for row in cursor.fetchall()]
        pointOwners = getOwnerGeoTypeNumbers(sgidConnection, pointFcs)
        print 'summary'
        for owner in cursor.execute('SELECT DISTINCT owner FROM fcnames'):
            o = owner[0]
            print '{},{},{},{}'.format(o,
                                       pgonOwners.get(o, 0),
                                       plineOwners.get(o, 0),
                                       pointOwners.get(o, 0))


def createPolygonTable(dbPath):
    db = sqlite3.connect(dbPath)
    with db:
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE polygons
                          (feature text, oid integer, geohash text, hash text, hashDate text)''')


def createPointTable(dbPath):
    db = sqlite3.connect(dbPath)
    with db:
        cursor = db.cursor()
        cursor.execute('''CREATE TABLE pointsnew
                          (feature text, oid integer, geohash text, hash text, hashDate text)''')

if __name__ == '__main__':
    sgidConnection = r'Database Connections\Connection to sgid.agrc.utah.gov.sde'
    hashDb = r'C:\GisWork\sgidchanges\sgidPolyHash.db'

    db = sqlite3.connect(hashDb)
    startTime = time()
    # pgonFcs = None
    # with db:
    #     cursor = db.cursor()
    #     cursor.execute('SELECT featureclass FROM fcnames WHERE geotype=?', ('polygon', ))
    #     pgonFcs = [row[0] for row in cursor.fetchall()]
    #createPointTable(hashDb)
    addHashedFeaturesToDb(sgidConnection, ['SGID10.SOCIETY.StateFacilities'], 'pointsnew')

    print 'time: {}'.format(time() - startTime)
