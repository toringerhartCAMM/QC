

import omero
import omero.scripts as scripts

from omero.gateway import BlitzGateway
from omero.rtypes import rlong, robject, rstring, wrap, unwrap

import os
import sys
import math
import tempfile
import itertools

import numpy as np
import matplotlib.pyplot as plt

from PIL import Image
from cStringIO import StringIO


from OMERO_BaseClasses import OMERO_QualityCheck



class OMERO_PowerSpectrum (OMERO_QualityCheck):
    """
    Power Spectrum Quality Check
    """
    
    qc_name = "PowerSpectrum"
    qc_version = 0.1

    def check (self, imageid):
        image = self.conn.getObject("Image", imageid)
        pixels = image.getPrimaryPixels()
        labels = image.getChannelLabels()
        planes_zct = itertools.product( range(image.getSizeZ()), 
                                        range(image.getSizeC()), 
                                        range(image.getSizeT()))
        
        rows = image.getSizeY()
        columns = image.getSizeX()
        
        midrow = int(math.ceil(0.5 * rows)) 
        midcol = int(math.ceil(0.5 * columns)) 
        midpoint = (midrow, midcol) 
        
        # Change of variables for computing distance form the midpoint
        # Let f = (w/2) - |x - (w/2)| 
        #     g = (h/2) - |y - (h/2)|
        def distance (y, x): 
            return np.linalg.norm(np.array((midcol - abs(x - midcol), 
                                            midrow - abs(y - midrow)))) 
        
        # Index the pixels by (x,y) location
        indexes = tuple(itertools.product(range(rows), range(columns)))
        
        # Calculate the distance from each pixel to the center
        distances = tuple(itertools.starmap(distance, indexes))
        maxdistance = np.ceil(max(distances)).astype(int)
        
        # These distances are by pixel-index, so they will be the same for each 
        # plane. So pre-compute which pixels should be grouped together by distance 
        distance_groups = []
        for i,(a,b) in enumerate(zip(range(maxdistance-1), range(1, maxdistance))):
            interval = lambda x: a < x and x <= b
            distance_groups.append(map(interval, distances))
        
        # Iterate through each plane in the (z,c,t) list and compute power spectrum 
        results = {}
        for zct in planes_zct:
            z,c,t = zct
            plane = pixels.getPlane(theZ=z, theC=c, theT=t)
            
            # Compute the (log) power spectrum for the correspoding plane
            powerspectrum = np.log10(np.abs(np.fft.fft2(plane))**2)
            powerspectrum = powerspectrum.reshape(1,powerspectrum.size)[0]
            
            # Radial averaging
            radial_average = []
            for distance_group in distance_groups:
                radial_average.append(np.mean(tuple(itertools.compress(powerspectrum, distance_group))))
            
            # Store result
            results[labels[c]] = radial_average
        
        return results
    
    @OMERO_QualityCheck.autotag
    def store (self, imageid, results):
        """
        Store power spectrum data
            - Store numerical values in as a double annotation attached to the image
            - Store plots of the power spectrum as PNG files attached to the image
        """
        for label,result in results.items():
            # Create a "double" annotation for the results
            doubleAnn = omero.gateway.DoubleAnnotationWrapper(self.conn)
            doubleAnn.setName(label + " power spectrum")
            doubleAnn.setNs(self.namespace)
            doubleAnn.setValue(results)
            self.conn.getObject("Image", imageid).linkAnnotation(doubleAnn)
            
            # Plot the data
            plt.plot(result)
            plt.title(label + " Power Spectrum")
            
            # Prep label
            label = label.replace(' ','_')
            
            # Create a temporary file named after the channel label
            _f, filename = tempfile.mkstemp(prefix=label + "_", suffix='_powerspectrum.png')
            
            # Save the image
            plt.savefig(filename, bbox_inches='tight')
            plt.close()
            
            # Create a file annotation and link it to the image
            fileAnn = self.conn.createFileAnnfromLocalFile(filename, mimetype="image/png", ns=self.namespace, desc=None)
            self.conn.getObject("Image", imageid).linkAnnotation(fileAnn)
            
            # Remove the tmp file
            os.remove(filename)


if __name__ == "__main__":
    powerspectrum = OMERO_PowerSpectrum(username="importer", password="omero")
    powerspectrum.run()







