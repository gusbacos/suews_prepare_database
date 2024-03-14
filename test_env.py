# Initialisation
# from doctest import OutputChecker
# # from tkinter.messagebox import NO
from osgeo import gdal
from qgis.core import QgsApplication, QgsVectorLayer, QgsCoordinateReferenceSystem
import numpy as np
from osgeo.gdalconst import GDT_Float32, GA_ReadOnly
import sys, os
import shutil
#import matplotlib.pylab as plt
import time
# from pathlib import Path
# import subprocess
# *****************************************************************************
import warnings
warnings.filterwarnings("ignore")
# *****************************************************************************


windowsuser = 'xlinfr'  


# Initiating a QGIS application and connect to processing
qgishome = 'C:/OSGeo4W/apps/qgis/'
QgsApplication.setPrefixPath(qgishome, True)
app = QgsApplication([], False)
app.initQgis()


sys.path.append('C:/OSGeo4W/apps/qgis/python/plugins')
sys.path.append('C:/Users/' + windowsuser + '/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins') # path to third party plugins
sys.path.append('C:/FUSION')


import processing
# from processing_umep.processing_umep_provider import ProcessingUMEPProvider
# umep_provider = ProcessingUMEPProvider()
# QgsApplication.processingRegistry().addProvider(umep_provider)


# from processing_fusion.processing_fusion_provider import ProcessingFUSIONProvider
# fusion_provider = ProcessingFUSIONProvider()
# QgsApplication.processingRegistry().addProvider(fusion_provider)


# from QuickOSM.quick_osm_processing.provider import Provider
# quickOSM_provider = Provider()
# QgsApplication.processingRegistry().addProvider(quickOSM_provider)


try:
    from qgis.core import QgsApplication
    qgis_available = True
except ImportError:
    qgis_available = False


if qgis_available:
    print("QGIS är tillgängligt.")
    print(sys.path)
    print(sys.executable)
    print(os.environ["PYTHONHOME"])
    print(os.environ["PYTHONPATH"])
else:
    print("QGIS är inte tillgängligt.")
    print(sys.path)
    print(sys.executable)


try:
    from processing_fusion.processing_fusion_provider import ProcessingFUSIONProvider
    fusion_provider = ProcessingFUSIONProvider()
    QgsApplication.processingRegistry().addProvider(fusion_provider)
    fusion_available = True
except ImportError:
    fusion_available = False


if fusion_available:
    print("Fusion är tillgängligt.")
else:
    print("Fusion är inte tillgängligt.")




try:
    from processing_umep.processing_umep_provider import ProcessingUMEPProvider
    umep_provider = ProcessingUMEPProvider()
    QgsApplication.processingRegistry().addProvider(umep_provider)
    umep_available = True
except ImportError:
    umep_available = False


if umep_available:
    print("Umep är tillgängligt.")
else:
    print("Umep är inte tillgängligt.")


from processing.core.Processing import Processing
Processing.initialize()