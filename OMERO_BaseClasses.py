
import omero
import omero.scripts as scripts

from omero.gateway import BlitzGateway
from omero.rtypes import rlong, robject, rstring, rtime, wrap, unwrap

import sys
import itertools
import numpy as np


class OMERO_Object:
    """
    Generic base class for an OMERO connection.
    """
    
    def __init__ (self, *args, **kwargs): 
        self.args = args
        self.kwargs = kwargs
        
        self.server = "littlestar.camm.usc.edu"
        self.port = 4064
        
        for key,value in self.kwargs.items():
            if   key == "username": self.username = value
            elif key == "password": self.password = value
            elif key == "server":   self.server = value
            elif key == "port":     self.port = value
        
        if (self.username is None): raise ValueError("Cannot connect: No username.")
        if (self.password is None): raise ValueError("Cannot connect: No password.")
        
        self.connect()
    
    def connect (self):
        "Initiate a connection to the OMERO server"
        self.conn = BlitzGateway(   username=self.username, 
                                    passwd=self.password, 
                                    host=self.server, 
                                    port=self.port)
        self.connected = self.conn.connect()
        if self.connected is False:
            raise IOError("OMERO connection {user}@{server}:{port} failed.".format(user=self.username, server=self.server, port=self.port))

    def _reconnect (f):
        "Decorator for re-connecting if the connection times out"
        def wrapper (self):
            try:
                return f(self)
            except ConnectionLostException:
                self.connect()
            return f(self)
        return wrapper

    @_reconnect
    def getUpdateService (self):
        return self.conn.getUpdateService()
    
    @_reconnect
    def getRoiService (self):
        return self.conn.getRoiService()
    
    @_reconnect
    def getQueryService (self):
        return self.conn.getQueryService()


    def query (self, **kwargs):
        """
        Function to construct OMERO HQL queries by passing keyword arguments
        
        Available keywords:
            - "filename"    : Search for images by the image file name
            - "plate"       : Search for plates by plate name
            - "acquisition" : Search by acquisition name
            - "with_tag"    : Search for images with a particular tag
            - "without_tag" : Search for images without a particular tag
            - "daterange"   : Search for images by date range
            - "noqc"        : Search for images tagged with "noqc"
        """
        # Init some variables
        noqc = False
        parameters = omero.sys.Parameters()
        
        # Parse out query parameters from kwargs
        for key,value in kwargs.items():
            if key in ('noqc'):
                if not isinstance(value, bool):
                    raise ValueError("Expected boolean value (True/False) with 'noqc' keyword")
                noqc = value
            elif key in ('daterange'):
                if not isinstance(value, list):
                    raise ValueError("Expected list type for 'daterange' parameter")
                if not isinstance(value[0], datetime.datetime) or not isinstance(value[1], datetime.datetime):
                    raise ValueError("Expected datetime type for 'daterange' entry")
                parameters.map.update({ 'startDate':    rtime(time.mktime(value[0].timetuple())),
                                        'endDate':      rtime(time.mktime(value[1].timetuple())) })
            elif key in ('filename', 'plate', 'acquisition', 'with_tag', 'without_tag'):
                parameters.map.update({ key:rstring(value) })
            else:
                raise ValueError('Unknown query parameter: {}'.format(key))
        
        # If this has no entries, then we have nothing to look for
        if len(parameters.map) == 0: 
            raise ValueError('No parameters to query.')
        
        # Build the where clause
        where = []
        if 'without_tag' in kwargs: where.append("image not in ( {} )".format(self.__tag_query()))
        if 'acquisition' in kwargs: where.append("image in ( {} )".format(self.__acquisition_query()))
        if 'daterange' in kwargs:   where.append("image in ( {} )".format(self.__date_query()))
        if 'filename' in kwargs:    where.append("image in ( {} )".format(self.__filename_query()))
        if 'with_tag' in kwargs:    where.append("image in ( {} )".format(self.__tag_query()))
        if 'plate' in kwargs:       where.append("image in ( {} )".format(self.__plate_query()))
        if noqc:                    where.append("image in ( {} )".format(self.__noqc_query()))
        
        # Construct the query
        query = "select image from Image image where " + " and ".join(where)
        
        # Get the results
        queryService = self.getQueryService()
        results = queryService.findAllByQuery(query, params)
        
        # Return image ID's
        return [ image.id for image in results ]
    
    def __noqc_query (self):
        return """  select image from Image image
                    left outer join image.annotationLinks as annotations 
                    left outer join annotations.child as annotation 
                    where annotation.textValue like '%noqc' """
    
    def __filename_query (self):
        return """  select image from Image image
                    left outer join image.fileset as fileset
                    left outer join fileset.usedFiles as file
                    where file.clientPath like :filename"""
    
    def __plate_query (self):
        return """  select image from Plate plate 
                    left outer join plate.plateAcquisition as acquisition
                    left outer join acquisition.wellSample as sample
                    left outer join sample.image as image
                    where plate.name like :plate"""
    
    def __acquisition_query (self):
        return """  select image from Plate plate
                    left outer join plate.plateAcquisition as acquisition
                    left outer join acquisition.wellSample as sample
                    left outer join sample.image as image
                    where acquisition.name like :acquisition"""
    
    def __date_query (self):
        return """  select image from Image image
                    left outer join image.details.creationEvent as event
                    where event.time between :startDate and :endDate"""
    
    def __tag_query (self):
        return """  select image from Image image
                    left outer join image.annotationLinks as annotations 
                    left outer join annotations.child as annotation 
                    where annotation.textValue like :tag"""








class OMERO_QualityCheck (OMERO_Object):
    "Base class for a quality check in OMERO"


    def run (self, *args, **kwargs):
        "Pattern for running quality checks"
        for objectid in self.query(without_tag=self.name):
            result = self.check(objectid)
            self.store(objectid, result)

    @classmethod
    def autotag (cls, f):
        "Decorator for automatically tagging images with the QC name during store()"
        def wrapper (self, objectid, result):
            tagAnn = omero.gateway.TagAnnotationWrapper(self.conn)
            tagAnn.setValue(self.name)
            tagAnn.save()
            self.conn.getObject("Image", objectid).linkAnnotation(tagAnn)
            f(self, objectid, result)
        return wrapper

    @property
    def namespace (self):
        "The namespace of the current object"
        return "{check}.qualitycheck".format(check=self.qc_name)

    @property
    def name (self):
        "Name of the quality check"
        return "#{check}_v{version}".format(check=self.qc_name, version=self.qc_version)

    def query (self):
        "Base function for querying and returning objects for quality checking."
        params = omero.sys.Parameters()
        queryService = self.getQueryService()
        
        # Create a map for the parameters
        params.map = {  
            'qcTag':    rstring(self.name),     # tag of the current quality check
            'noqc':     rstring("#noqc")        # tag to not run a quality check
        }
        
        # The query should return:
        #   - images that have not been quality checked (no "qcTag")
        #   - images that are not tagged as "noqc" 
        #   - images from plates that are not tagged "noqc"
        query = """
select image from Plate plate

left outer join plate.wells as wells
left outer join wells.wellSamples as samples
left outer join samples.image as image

where image not in (    select img from Image img 
                        left outer join img.annotationLinks as annotations 
                        left outer join annotations.child as annotation 
                        where annotation.textValue like :qcTag 
                        or    annotation.textValue like :noqc )

and plate not in (      select p from Plate p
                        left outer join p.annotationLinks as annotations 
                        left outer join annotations.child as annotation 
                        where annotation.textValue like :noqc )
"""

        results = queryService.findAllByQuery(query, params)
        ids = [ obj.id for obj in results ]
        return ids
        
    def remove (self, obj):
        "Remove all quality check tags from 'obj' from the current namespace"
        obj.removeAnnotations(obj.listAnnotations(ns=self.namespace))
    
