# -*- coding: utf-8 -*-
'''
Calcualte input for spartacus and write info to Gridlayout namelist

Fredrik Lindberg 2023-07-06
'''
import numpy as np
from ..Utilities import wallalgorithms as wa
from ..Utilities.umep_suewsss_export_component import write_GridLayout_file, create_GridLayout_dict

# import matplotlib as plt

def ss_calc(build, cdsm, walls, numPixels, feedback):

    #noOfPixels = int(dsm.shape[0] * dsm.shape[1])
    walllimit = 0.3 # 30 centimeters height variation identifies a vegetation edge pixel
    total = 100. / (int(build.shape[0] * build.shape[1]))

    if cdsm.max() > 0:
        vegEdges = wa.findwalls(cdsm, walllimit, feedback, total)
 
    buildvec = build[np.where(build > 0)]
    if buildvec.size > 0:
        #zH_all = buildvec.mean()
        zHmax_all = buildvec.max()
        #zH_sd_all = buildvec.std()
        #pai_ground = (buildvec.size * 1.0) / numPixels
        iterHeights = int(np.ceil(zHmax_all))
    else:
        #zH_all = 0
        zHmax_all = 0
        #zH_sd_all = 0
        #pai_all = 0
        iterHeights = int(0)

    z = np.zeros((iterHeights, 1))
    paiZ_b = np.zeros((iterHeights, 1))
    bScale = np.zeros((iterHeights, 1)) # building scale
    paiZ_v = np.zeros((iterHeights, 1))
    vScale = np.zeros((iterHeights, 1)) # vegetation scale

    for i in np.arange(0, iterHeights):
        z[i] = i
        buildZ = build - i
        wallsZ = walls - i
        paiZ_b[i] = np.where(buildZ > 0)[0].shape[0] / numPixels
        waiZ_b = np.where(wallsZ > 0)[0].shape[0] / numPixels
        if waiZ_b == 0:
            bScale[i] = 0
        else:
            bScale[i] = (4*paiZ_b[i]) / waiZ_b
        # feedback.setProgressText('z = ' + str(z[i]))
        # feedback.setProgressText('numPixels = ' + str(numPixels))
        # feedback.setProgressText('paipixels = ' + str(np.where(buildZ > 0)[0].shape[0]))
        # feedback.setProgressText('wallpixels = ' + str(np.where(wallsZ > 0)[0].shape[0]))
        # feedback.setProgressText('waiZ_b = ' + str(waiZ_b))
        # feedback.setProgressText('bScale[i] = ' + str(bScale[i]))

        if cdsm.max() > 0:
            vegZ = cdsm - i
            vegedgeZ = vegEdges - i
            paiZ_v[i] = np.where(vegZ > 0)[0].shape[0] / numPixels
            if paiZ_v[i] == 0:
                vScale[i] = 0
            else:
                waiZ_v = np.where(vegedgeZ > 0)[0].shape[0] / numPixels
                if waiZ_v == 0:
                    vScale[i] = 0
                else:
                    vScale[i] = (4*paiZ_v[i]) / waiZ_v
        else:
            paiZ_v[i] = 0
            vScale[i] = 0

    ssResult = {'z': z, 'paiZ_b': paiZ_b, 'bScale': bScale,'paiZ_v': paiZ_v, 'vScale': vScale}

    return ssResult


def getVertheights(ssVect, heightMethod, vertHeightsIn, nlayerIn, skew):
    '''
    Input:
    ssVect: array from xx_IMPGrid_SS_x.txt
    heightMethod: Method used to set vertical layers
    vertheights: heights of intermediate layers (bottom is 0 and top is maxzH) [option 1]
    nlayersIn: no of vertical layers [option 1 and 2]
    skew: 1 is equal interval between heights and 2 is exponential [option 2 and 3]
    '''
    print('Maxheight=' + str(ssVect[:,0].max()))
    # print('max vertHeightsIn=' + str(max(vertHeightsIn)))

    if heightMethod == 1: # static levels (taken from interface). Last value > max height
        if ssVect[:,0].max() > max(vertHeightsIn):
            vertHeightsIn.append(ssVect[:,0].max())
        heightIntervals = vertHeightsIn
        nlayerOut = len(heightIntervals) - 1
        # vertHeightsIn.clear()
    elif heightMethod == 2: # always nlayers layer based on percentiles
        nlayerOut = nlayerIn
    elif heightMethod == 3: # vary number of layers based on height variation. Lowest no of nlayers always 3
        nlayerOut = 3
        if ssVect[:,0].max() > 40: nlayerOut = 4
        if ssVect[:,0].max() > 60: nlayerOut = 5
        if ssVect[:,0].max() > 80: nlayerOut = 6
        if ssVect[:,0].max() > 120: nlayerOut = 7

    if heightMethod > 1:
        intervals = np.ceil(ssVect[:,0].max() / nlayerOut) #TODO: Fix if no buildings and/or no veg is present.
        heightIntervals = []
        heightIntervals.append(.0)
        for i in range(1, nlayerOut):
            heightIntervals.append(float(round((intervals * i) / skew)))
        heightIntervals.append(float(ssVect[:,0].max()))

    return heightIntervals, nlayerOut 


def writeGridLayout(ssVect, fileCode, featID, outputFolder, gridlayoutIn):
    '''
    Input:
    ssVect: array from xx_IMPGrid_SS_x.txt
    heightMethod: Method used to set vertical layers
    vertheights: heights of intermediate layers (bottom is 0 and top is maxzH) [option 1]
    nlayers: no of vertical layers [option 2]
    skew: 1 is equal interval between heights and 2 is exponential [option 2 and 3]
    '''

    #heightMethod = 2 # input from gui
    #vertHeights = [0, 15, 40]
    #nlayer = 3 # input from gui
    #skew = 2 input from gui. linear or shewed shewed = 1 or 2

    ssDict = create_GridLayout_dict()
    # print(ssVect[:,0].max())
    # print(vertHeights)
    # if heightMethod == 1: # static levels (taken from interface). Last value > max height
    #     if ssVect[:,0].max() > max(vertHeights):
    #         ssDict['height'] = vertHeights.append(ssVect[:,0].max())
    #     ssDict['nlayer'] = len(ssDict['height']) - 1
    # elif heightMethod == 2: # always nlayers layer based on percentiles
    #     ssDict['nlayer'] = nlayer
    # elif heightMethod == 3: # vary number of layers based on height variation. Lowest no of nlayers always 3
    #     nlayer = 3
    #     if ssVect[:,0].max() > 40: nlayer = 4
    #     if ssVect[:,0].max() > 60: nlayer = 5
    #     if ssVect[:,0].max() > 80: nlayer = 6
    #     if ssVect[:,0].max() > 120: nlayer = 7
    #     ssDict['nlayer'] = nlayer

    # intervals = np.ceil(ssVect[:,0].max() / nlayer) #TODO: Fix if no buildings and/or no veg is present.
    # ssDict['height'] = []
    # ssDict['height'].append(.0)
    # for i in range(1, nlayer):
    #     ssDict['height'].append(float(round((intervals * i) / skew)))
    # ssDict['height'].append(float(ssVect[:,0].max()))
    
    ssDict['nlayer'] = gridlayoutIn[featID]['nlayer']
    ssDict['height'] = gridlayoutIn[featID]['height']
    ssDict['building_frac'] = []
    ssDict['veg_frac'] = []
    ssDict['building_scale'] = []
    ssDict['veg_scale'] = []

    index = int(0)
    for i in range(1,len(ssDict['height'])): #TODO this loop need to be confirmed by Reading
        index += 1
        startH = int(ssDict['height'][index-1])
        endH = int(ssDict['height'][index])
        if index == 1:
            ssDict['building_frac'].append(ssVect[0,1]) # first is plan area index of buildings
            ssDict['veg_frac'].append(ssVect[0,3]) # first is plan area index of trees
        else:
            ssDict['building_frac'].append(np.round(np.mean(ssVect[startH:endH, 1]),3)) # intergrated pai_build mean in ith vertical layer
            ssDict['veg_frac'].append(np.round(np.mean(ssVect[startH:endH, 3]),3)) # intergrated pai_veg mean in ith vertical layer

        ssDict['building_scale'].append(np.round(np.mean(ssVect[startH:endH, 2]),3)) # intergrated bscale mean in ith vertical layer
        ssDict['veg_scale'].append(np.round(np.mean(ssVect[startH:endH, 4]),3)) # intergrated vscale mean in ith vertical layer

    #TODO here we need to add other parameters based on typology

    write_GridLayout_file(ssDict, outputFolder + '/', 'GridLayout' +  fileCode + str(featID))


