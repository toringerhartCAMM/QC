

import omero
import omero.scripts as scripts

from omero.gateway import BlitzGateway
from omero.rtypes import rlong, robject, rstring, rfloat, wrap, unwrap

import sys
import itertools
import numpy as np

from OMERO_BaseClasses import OMERO_QualityCheck


class OMERO_SaturationCheck (OMERO_QualityCheck):

    qc_name = "SaturationCheck"
    qc_version = 0.1

    def check (self, imageid):
        image = self.conn.getObject("Image", imageid)
        pixels = image.getPrimaryPixels()
        labels = image.getChannelLabels()
        planes_zct = itertools.product( range(image.getSizeZ()), 
                                        range(image.getSizeC()), 
                                        range(image.getSizeT())) 
        
        results = []
        return results

    @OMERO_QualityCheck.autotag
    def store (self, imageid, results):
        # Init the map (key-value) annotation
        mapAnn = omero.gateway.MapAnnotationWrapper(self.conn)
        
        # Create a key/value pair for this check
        mapAnn.setNs(self.namespace)
        mapAnn.setValue(results)
        mapAnn.save()
        
        # Link the annotation to the image
        self.conn.getObject("Image", imageid).linkAnnotation(mapAnn)


if __name__ == "__main__":
    contrast_check = OMERO_ContrastMeasure(username="importer", password="omero")
    contrast_check.run()



