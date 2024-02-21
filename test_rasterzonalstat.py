# test rasterzonalstat

from osgeo import gdal
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

temp_folder = 'C:/Users/xlinfr/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/suews_prepare_database/tempdata'

heightIntervals=[0.0, 2.0, 5.0, 14.0] 

dataset = gdal.Open(temp_folder + '/clipbuild.tif')
build_array = dataset.ReadAsArray().astype(float)

dataset = gdal.Open(temp_folder + '/clipwall.tif')
wall_array = dataset.ReadAsArray().astype(float)

dataset = gdal.Open(temp_folder + '/cliptypo.tif')
typo_array = dataset.ReadAsArray().astype(int)

typoList = np.unique(typo_array)
print(typoList)
id = 1
grid_dict = {1: {2028276682: {'SAreaFrac': 0.03053530958499169, 'uvalue_wall': 0.6793308373874489, 'uvalue_roof': 0.8294821661334282, 'albedo_roof': 0.25, 'albedo_wall': 0.2, 'emissivity_roof': 0.9, 'emissivity_wall': 0.9}, 2028252320: {'SAreaFrac': 0.03167886374055807, 'uvalue_wall': 0.7763114117777532, 'uvalue_roof': 0.8045977011494253, 'albedo_roof': 0.2, 'albedo_wall': 0.22, 'emissivity_roof': 0.9, 'emissivity_wall': 0.9}, 2028258089: {'SAreaFrac': 0.3700636950518986, 'uvalue_wall': 0.3310404127257094, 'uvalue_roof': 0.6473164904207778, 'albedo_roof': 0.18, 'albedo_wall': 0.2, 'emissivity_roof': 0.92, 'emissivity_wall': 0.9}, 2030434306: {'SAreaFrac': 0.5677221316225517, 'uvalue_wall': 1.093947226062391, 'uvalue_roof': 0.8045977011494253, 'albedo_roof': 0.2, 'albedo_wall': 0.3, 'emissivity_roof': 0.9, 'emissivity_wall': 0.95}}}


            

print('heightIntervals=' + str(heightIntervals))
dictTopofrac = {} # empty dict to calc fractions for current grid and typology
allRoof = [] #for sfr_roof
allWall = [] #for sfr_wall

dfT = pd.DataFrame(columns=['height','alb_roof','alb_wall','emis_roof','emis_wall','u_roof','u_wall'], index=range(int(max(heightIntervals))))

for hh in range(0, int(max(heightIntervals) + 1)):
    dictTopofrac[hh] = {}
    buildhh = build_array - hh
    buildhh[buildhh < 0] = 0
    buildhhBol = buildhh > 0
    allRoof.append(len(buildhhBol[np.where(buildhhBol != 0)]))
    wallhh = wall_array - hh
    wallhh[wallhh < 0] = 0 
    allWall.append(wallhh.sum())
    totBuildPixelsInTypo = 0
    totWallAreaTypo = 0
    for tt in typoList:
        if tt != 0:
            dictTopofrac[hh][int(tt)] = {}
            tR = buildhhBol[np.where(typo_array == tt)]
            dictTopofrac[hh][tt]['roofSum'] = tR.sum()
            totBuildPixelsInTypo += tR.sum()
            tW = wallhh[np.where(typo_array == tt)]
            dictTopofrac[hh][tt]['wallSum'] = tW.sum()
            totWallAreaTypo += tW.sum()
    albRoof = 0
    albWall = 0
    URoof = 0
    UWall = 0
    ERoof = 0
    EWall = 0
    for tt in typoList:
        if tt != 0:
            dictTopofrac[hh][tt]['roofFrac'] = dictTopofrac[hh][tt]['roofSum'] / totBuildPixelsInTypo
            dictTopofrac[hh][tt]['wallFrac'] = dictTopofrac[hh][tt]['wallSum'] / totWallAreaTypo
            albRoof = albRoof + dictTopofrac[hh][tt]['roofFrac'] * grid_dict[id][tt]['albedo_roof']
            albWall = albWall + dictTopofrac[hh][tt]['wallFrac'] * grid_dict[id][tt]['albedo_wall']
            URoof = URoof + dictTopofrac[hh][tt]['roofFrac'] * grid_dict[id][tt]['uvalue_roof']
            UWall = UWall + dictTopofrac[hh][tt]['wallFrac'] * grid_dict[id][tt]['uvalue_wall']
            ERoof = ERoof + dictTopofrac[hh][tt]['roofFrac'] * grid_dict[id][tt]['emissivity_roof']
            EWall = EWall + dictTopofrac[hh][tt]['wallFrac'] * grid_dict[id][tt]['emissivity_wall']
    dfT['height'][hh] = hh
    dfT['alb_roof'][hh] = albRoof
    dfT['alb_wall'][hh] = albWall
    dfT['u_roof'][hh] = URoof
    dfT['u_wall'][hh] = UWall
    dfT['emis_roof'][hh] = ERoof
    dfT['emis_wall'][hh] = EWall
    
sfr_roof = []
sfr_wall = []
for fr in range(1,len(allRoof)):
    sfr_roof.append((allRoof[fr - 1] - allRoof[fr]) / allRoof[0])
    sfr_wall.append((allWall[fr - 1] - allWall[fr]) / allWall[0])

gridlayoutOut = {} #remove
gridlayoutOut[id] = {} #remove

# aggergation based on vertical layers
gridlayoutOut[id]['sfr_roof'] = []
gridlayoutOut[id]['sfr_wall'] = []
gridlayoutOut[id]['alb_roof'] = []
gridlayoutOut[id]['alb_wall'] = []
gridlayoutOut[id]['emis_roof'] = []
gridlayoutOut[id]['emis_wall'] = []
gridlayoutOut[id]['u_roof'] = []
gridlayoutOut[id]['u_wall'] = []
start = int(0)
for p in heightIntervals:
    if p > 0:
        gridlayoutOut[id]['sfr_roof'].append(np.sum(sfr_roof[start:int(p)]))
        gridlayoutOut[id]['sfr_wall'].append(np.sum(sfr_wall[start:int(p)]))
        gridlayoutOut[id]['alb_roof'].append(dfT['alb_roof'][start:int(p)].mean())
        gridlayoutOut[id]['alb_wall'].append(dfT['alb_wall'][start:int(p)].mean())
        gridlayoutOut[id]['emis_roof'].append(dfT['emis_roof'][start:int(p)].mean())
        gridlayoutOut[id]['emis_wall'].append(dfT['emis_wall'][start:int(p)].mean())
        gridlayoutOut[id]['u_roof'].append(dfT['u_roof'][start:int(p)].mean())
        gridlayoutOut[id]['u_wall'].append(dfT['u_wall'][start:int(p)].mean())
        start = int(p)





index = 0
# print('heightIntervals=' + str(heightIntervals))
# dictfrac = {} # empty dict to calc fractions for current grid and typology
# allRoof = [] #for sfr_roof
# allWall = [] #for sfr_wall
# #TODO: This section could be improved to aggregate over all height within a vertical layer. It works for now.
# for hh in heightIntervals:
#     print(hh)
#     dictfrac[hh] = {}
#     buildhh = build_array - hh
#     buildhh[buildhh < 0] = 0
#     buildhhBol = buildhh > 0
#     # allRoof.append(len(buildhh[np.where(buildhh != 0)])) 
#     allRoof.append(len(buildhhBol[np.where(buildhhBol != 0)]))
#     wallhh = wall_array - hh
#     wallhh[wallhh < 0] = 0 
#     allWall.append(wallhh.sum())
#     totBuildPixelsInTypo = 0
#     totWallAreaTypo = 0
#     for tt in typoList:
#         if tt != 0:
#             dictfrac[hh][tt] = {}
#             tR = buildhhBol[np.where(typo_array == tt)]
#             dictfrac[hh][tt]['roofSum'] = tR.sum()
#             totBuildPixelsInTypo += tR.sum()
#             tW = wallhh[np.where(typo_array == tt)]
#             dictfrac[hh][tt]['wallSum'] = tW.sum()
#             totWallAreaTypo += tW.sum()
#     for tt in typoList:
#         if tt != 0:
#             dictfrac[hh][tt]['roofFrac'] = dictfrac[hh][tt]['roofSum'] / totBuildPixelsInTypo
#             dictfrac[hh][tt]['wallFrac'] = dictfrac[hh][tt]['wallSum'] / totWallAreaTypo

# sfr_roof = []
# sfr_wall = []
# for fr in range(1,len(allRoof)):
#     sfr_roof.append((allRoof[fr - 1] - allRoof[fr]) / allRoof[0])
#     sfr_wall.append((allWall[fr - 1] - allWall[fr]) / allWall[0])   

#     # sum(x['roofSum'] for x in dictfrac[0].values())
# test = 4