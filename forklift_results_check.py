''' Check results of forklift '''
import arcpy
from os.path import join
from hashlib import md5
from time import strftime

unique_run_num = strftime("_%Y%m%d_%H%M%S")


class Crate(object):

    def __init__(self,
                 source_name,
                 source_workspace,
                 destination_workspace,
                 destination_name=None,
                 destination_coordinate_system=None,
                 geographic_transformation=None):

        #: the name of the source data table
        self.source_name = source_name
        #: the name of the source database
        self.source_workspace = source_workspace
        #: the name of the destination database
        self.destination_workspace = destination_workspace
        #: the name of the output data table
        self.destination_name = destination_name or source_name

        #: optional definition of destination coordinate system to support reprojecting
        if destination_coordinate_system is not None and isinstance(destination_coordinate_system, int):
            destination_coordinate_system = arcpy.SpatialReference(destination_coordinate_system)

        self.destination_coordinate_system = destination_coordinate_system
        #: optional geographic transformation to support reprojecting
        self.geographic_transformation = geographic_transformation
        #: the full path to the destination data
        self.destination = join(self.destination_workspace, self.destination_name)
        #: the hash table name of a crate
        self.name = '{1}_{0}'.format(md5(self.destination).hexdigest(), self.destination_name).replace('.', '_')

        #: the full path to the source data
        self.source = join(source_workspace, source_name)


class Feature_Check(object):

    def __init__(self, path, name, output_workspace):
        self.output_workspace = output_workspace
        self.path = path
        self.row_count = None
        self.empty_geometry_count = None
        self.not_in_other_count = None
        self.not_identical_table = join(output_workspace, name + '_notin_other')
        self.get_rows_not_identical(2)

    def get_rows_not_identical(self, other_features, identical_distance_limit):
        # select by NEAR_DIST > indenical dist and not -1
        working_layer = 'NotIdentical'
        where = ''
        arcpy.MakeFeatureLayer_management(self.path, working_layer)
        self.row_count = int(arcpy.GetCount_management(working_layer).getOutput(0))
        arcpy.NearField_management(working_layer, other_features)
        arcpy.SelectLayerByAttribute_management(working_layer, where)
        with arcpy.SearchCursor(working_layer, ['SHAPE@']) as cursor:
            for row in cursor:
                shape = row[0]
                if shape is None:
                    self.empty_geometry_count += 1
                else:
                    self.source_not_count += 1
        arcpy.CopyFeatures_management(working_layer, join(self.output_workspace, self.not_identical_table))


class Crate_Check(object):

    def __init__(self, crate, output_workspace):
        self.crate = crate
        self.output_workspace = output_workspace
        self.src_check = Feature_Check(self._copy_source_into_projection(crate), output_workspace)
        self.dst_check = Feature_Check(arcpy.CopyFeatures_management(crate.destination), output_workspace)

    def _copy_source_into_projection(self, crate):
        output = ''
        if crate.destination_coordinate_system and crate.destination_coordinate_system is not crate.source_coordinate_system:
            output = arcpy.Project_management(crate.source,
                                              self.output_workspace + crate.source_name + unique_run_num,
                                              crate.destination_coordinate_system,
                                              crate.geographic_transformation)[0]
        else:
            output = join(self.output_workspace, crate.source_name + '_src')
            arcpy.CopyFeatures_management(crate.source, output)

        return output
