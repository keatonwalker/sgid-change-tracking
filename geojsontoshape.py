import arcpy
import json


if __name__ == '__main__':
    geojsonFile = r'C:\GisWork\temp\downloadstats_18up_at14.json'
    jsonPolygons = None
    with open(geojsonFile) as jsonPgons:
        jsonPolygons = json.load(jsonPgons)

    for feature in jsonPolygons['features']:
        if len(feature['geometry']['coordinates']) > 1:
            print 'weird one!!!!'
        for coord in feature['geometry']['coordinates'][0]:
