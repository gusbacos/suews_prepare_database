# test rasterzonalstat

from osgeo import gdal
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
# from .Utilities.db_functions import (read_DB)
from Utilities.db_functions import(read_DB)
import os

temp_folder = 'C:/Users/xlinfr/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins/suews_prepare_database/tempdata'

heightIntervals=[0.0, 2.0, 5.0, 14.0] 
nan = 0.0
dictTypofrac = {0: {2028252320: {'roofSum': 500, 'wallSum': 740.4479064941406, 'roofFrac': 0.025076483273985657, 'wallFrac': 0.018022825992350937}, 2028258089: {'roofSum': 4375, 'wallSum': 8901.82971572876, 'roofFrac': 0.21941922864737448, 'wallFrac': 0.21667442985928384}, 2028276682: {'roofSum': 683, 'wallSum': 911.4600734710693, 'roofFrac': 0.03425447615226441, 'wallFrac': 0.022185336954929285}, 2030434306: {'roofSum': 14381, 'wallSum': 30530.158182144165, 'roofFrac': 0.7212498119263755, 'wallFrac': 0.7431174071934359}}, 1: {2028252320: {'roofSum': 500, 'wallSum': 593.0827770233154, 'roofFrac': 0.025100401606425703, 'wallFrac': 0.020195163560659483}, 2028258089: {'roofSum': 4375, 'wallSum': 8072.557621002197, 'roofFrac': 0.2196285140562249, 'wallFrac': 0.27488004680766276}, 2028276682: {'roofSum': 683, 'wallSum': 647.7193202972412, 'roofFrac': 0.03428714859437751, 'wallFrac': 0.02205560188487439}, 2030434306: {'roofSum': 14362, 'wallSum': 20054.20520591736, 'roofFrac': 0.7209839357429719, 'wallFrac': 0.6828691877468034}}, 2: {2028252320: {'roofSum': 500, 'wallSum': 497.6209144592285, 'roofFrac': 0.025301082886347536, 'wallFrac': 0.02181778319515381}, 2028258089: {'roofSum': 4375, 'wallSum': 7245.557621002197, 'roofFrac': 0.22138447525554095, 'wallFrac': 0.3176755652941361}, 2028276682: {'roofSum': 683, 'wallSum': 480.28296089172363, 'roofFrac': 0.034561279222750735, 'wallFrac': 0.0210576147597203}, 2030434306: {'roofSum': 14204, 'wallSum': 14584.580457687378, 'roofFrac': 0.7187531626353608, 'wallFrac': 0.6394490367509897}}, 3: {2028252320: {'roofSum': 500, 'wallSum': 405.6209144592285, 'roofFrac': 0.027657926761809934, 'wallFrac': 0.023913236013778316}, 2028258089: {'roofSum': 4260, 'wallSum': 6439.0901222229, 'roofFrac': 0.23564553601062063, 'wallFrac': 0.37961425636049717}, 2028276682: {'roofSum': 568, 'wallSum': 340.29894256591797, 'roofFrac': 0.031419404801416084, 'wallFrac': 0.02006220251159153}, 2030434306: {'roofSum': 12750, 'wallSum': 9777.182600021362, 'roofFrac': 0.7052771324261533, 'wallFrac': 0.5764103051141329}}, 4: {2028252320: {'roofSum': 500, 'wallSum': 313.6209144592285, 'roofFrac': 0.03040807638508788, 'wallFrac': 0.02568113499032348}, 2028258089: {'roofSum': 4257, 'wallSum': 5681.134452819824, 'roofFrac': 0.2588943623426382, 'wallFrac': 0.4652048828842074}, 2028276682: {'roofSum': 482, 'wallSum': 244.21515655517578, 'roofFrac': 0.029313385635224717, 'wallFrac': 0.019997781120531077}, 2030434306: {'roofSum': 11204, 'wallSum': 5973.142164230347, 'roofFrac': 0.6813841756370492, 'wallFrac': 0.489116201004938}}, 5: {2028252320: {'roofSum': 500, 'wallSum': 221.62091445922852, 'roofFrac': 0.0351493848857645, 'wallFrac': 0.026265933818454625}, 2028258089: {'roofSum': 4251, 'wallSum': 4927.625576019287, 'roofFrac': 0.29884007029876974, 'wallFrac': 0.5840093547924506}, 2028276682: {'roofSum': 482, 'wallSum': 149.13408851623535, 'roofFrac': 0.03388400702987698, 'wallFrac': 0.017674983918377553}, 2030434306: {'roofSum': 8992, 'wallSum': 3139.199291229248, 'roofFrac': 0.6321265377855888, 'wallFrac': 0.3720497274707172}}, 6: {2028252320: {'roofSum': 500, 'wallSum': 129.62091445922852, 'roofFrac': 0.04611269943742507, 'wallFrac': 0.02245704180596548}, 2028258089: {'roofSum': 4249, 'wallSum': 4176.754241943359, 'roofFrac': 0.39186571981923823, 'wallFrac': 0.723629709108935}, 2028276682: {'roofSum': 414, 'wallSum': 71.65682029724121, 'roofFrac': 0.03818131513418795, 'wallFrac': 0.01241466483870445}, 2030434306: {'roofSum': 5680, 'wallSum': 1393.917667388916, 'roofFrac': 0.5238402656091488, 'wallFrac': 0.2414985842463951}}, 7: {2028252320: {'roofSum': 424, 'wallSum': 47.742319107055664, 'roofFrac': 0.05456183245399562, 'wallFrac': 0.01188702714266327}, 2028258089: {'roofSum': 4249, 'wallSum': 3427.685495376587, 'roofFrac': 0.5467764766439326, 'wallFrac': 0.8534355113476909}, 2028276682: {'roofSum': 311, 'wallSum': 34.26658248901367, 'roofFrac': 0.04002058937073736, 'wallFrac': 0.008531797444104846}, 2030434306: {'roofSum': 2787, 'wallSum': 506.64362716674805, 'roofFrac': 0.35864110153133444, 'wallFrac': 0.126145664065541}}, 8: {2028252320: {'roofSum': 281, 'wallSum': 20.35688018798828, 'roofFrac': 0.04976974849450939, 'wallFrac': 0.007172077333134037}, 2028258089: {'roofSum': 4249, 'wallSum': 2678.685495376587, 'roofFrac': 0.7525681898689338, 'wallFrac': 0.9437467503159623}, 2028276682: {'roofSum': 176, 'wallSum': 11.09971809387207, 'roofFrac': 0.031172511512575274, 'wallFrac': 0.003910620675176489}, 2030434306: {'roofSum': 940, 'wallSum': 128.2099266052246, 'roofFrac': 0.1664895501239816, 'wallFrac': 0.0451705516757271}}, 9: {2028252320: {'roofSum': 131, 'wallSum': 4.648109436035156, 'roofFrac': 0.028342708784076156, 'wallFrac': 0.00237076545780793}, 2028258089: {'roofSum': 4249, 'wallSum': 1929.685495376587, 'roofFrac': 0.9192990047598443, 'wallFrac': 0.9842349410718982}, 2028276682: {'roofSum': 64, 'wallSum': 4.596382141113281, 'roofFrac': 0.013846819558632626, 'wallFrac': 0.0023443819817486333}, 2030434306: {'roofSum': 178, 'wallSum': 21.66439437866211, 'roofFrac': 0.03851146689744699, 'wallFrac': 0.011049911488545242}}, 10: {2028252320: {'roofSum': 5, 'wallSum': 0.0, 'roofFrac': 0.0011633317822242904, 'wallFrac': 0.0}, 2028258089: {'roofSum': 4249, 'wallSum': 1180.685495376587, 'roofFrac': 0.988599348534202, 'wallFrac': 0.9967720628474465}, 2028276682: {'roofSum': 26, 'wallSum': 0.9409751892089844, 'roofFrac': 0.00604932526756631, 'wallFrac': 0.0007944010357618093}, 2030434306: {'roofSum': 18, 'wallSum': 2.8825454711914062, 'roofFrac': 0.004187994416007445, 'wallFrac': 0.0024335361167916987}}, 11: {2028252320: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}, 2028258089: {'roofSum': 4151, 'wallSum': 446.5054626464844, 'roofFrac': 1.0, 'wallFrac': 0.9995146345177596}, 2028276682: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}, 2030434306: {'roofSum': 0, 'wallSum': 0.21682357788085938, 'roofFrac': 0.0, 'wallFrac': 0.00048536548224048135}}, 12: {2028252320: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}, 2028258089: {'roofSum': 2001, 'wallSum': 42.12245559692383, 'roofFrac': 1.0, 'wallFrac': 1.0}, 2028276682: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}, 2030434306: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}}, 13: {2028252320: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}, 2028258089: {'roofSum': 44, 'wallSum': 2.4856128692626953, 'roofFrac': 1.0, 'wallFrac': 1.0}, 2028276682: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}, 2030434306: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': 0.0, 'wallFrac': 0.0}}, 14: {2028252320: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': nan, 'wallFrac': nan}, 2028258089: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': nan, 'wallFrac': nan}, 2028276682: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': nan, 'wallFrac': nan}, 2030434306: {'roofSum': 0, 'wallSum': 0.0, 'roofFrac': nan, 'wallFrac': nan}}}
grid_dict = {1: {2028252320: {'SAreaFrac': 0.03167886374055807, 'uvalue_wall': 0.7763114117777532, 'uvalue_roof': 0.8045977011494253, 'albedo_roof': 0.2, 'albedo_wall': 0.22, 'emissivity_roof': 0.9, 'emissivity_wall': 0.9}, 2028276682: {'SAreaFrac': 0.03053530958499169, 'uvalue_wall': 0.6793308373874489, 'uvalue_roof': 0.8294821661334282, 'albedo_roof': 0.25, 'albedo_wall': 0.2, 'emissivity_roof': 0.9, 'emissivity_wall': 0.9}, 2028258089: {'SAreaFrac': 0.3700636950518986, 'uvalue_wall': 0.3310404127257094, 'uvalue_roof': 0.6473164904207778, 'albedo_roof': 0.18, 'albedo_wall': 0.2, 'emissivity_roof': 0.92, 'emissivity_wall': 0.9}, 2030434306: {'SAreaFrac': 0.5677221316225517, 'uvalue_wall': 1.093947226062391, 'uvalue_roof': 0.8045977011494253, 'albedo_roof': 0.2, 'albedo_wall': 0.3, 'emissivity_roof': 0.9, 'emissivity_wall': 0.95}}}
gridlayoutOut = {1: {'nlayer': 3, 'height': [0.0, 2.0, 5.0, 14.0], 'sfr_roof': [0.008215962441314555, 0.32621772300469487, 0.6655663145539906], 'sfr_wall': [0.46600686352097687, 0.3434948524495812, 0.19049828402944197], 'alb_roof': [0.19732306319163007, 0.1968154053369312, 0.1855776391327978], 'alb_wall': [0.2716815096425421, 0.2573085991236637, 0.20903182121841737], 'emis_roof': [0.904390477427036, 0.904772829157392, 0.9153287751331666], 'emis_wall': [0.935649664873506, 0.9284162590478342, 0.904437963003033], 'u_roof': [0.7709235317098516, 0.7678543310388525, 0.6845023982340925], 'u_wall': [0.8912008861120404, 0.7823143773160224, 0.40399365112072927]}}
nlayer = 3

# Read DB
plugin_dir = os.path.dirname(__file__)
db_path = plugin_dir + '/Input/database.xlsx'  # TODO When in UMEP Toolbox, set this path to db in database manager
db_dict = read_DB(db_path)

# Find dominating typology in vertical layer and grid
findDomTypo = {}
domTypoWall = []
domTypoRoof = []
start = 0
layer = 1
id = 1
for p in heightIntervals:
    if p > 0:
        findDomTypo[layer] = {}
        for n in dictTypofrac[start].keys():
            findDomTypo[layer][n] = {}
            findDomTypo[layer][n]['wallFrac'] = 0
            findDomTypo[layer][n]['roofFrac'] = 0
        for hh in range(int(start), int(p + 1)):
            for q in dictTypofrac[0].keys():
                findDomTypo[layer][q]['wallFrac'] = findDomTypo[layer][q]['wallFrac'] + dictTypofrac[hh][q]['wallFrac'] 
                findDomTypo[layer][q]['roofFrac'] = findDomTypo[layer][q]['roofFrac'] + dictTypofrac[hh][q]['roofFrac']

        #Find dominating typology
        domTypoWall.append(max(findDomTypo[layer], key=lambda v: findDomTypo[layer][v]['wallFrac']))
        domTypoRoof.append(max(findDomTypo[layer], key=lambda v: findDomTypo[layer][v]['roofFrac']))

        layer = layer + 1
    start = p 

# create list for gridlayoutOut
for r in range(1, nlayer + 1): #iterate over number of vertical layers
    gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'] = []
    gridlayoutOut[id]['k_roof(' + str(r) + ',:)'] = []
    gridlayoutOut[id]['cp_roof(' + str(r) + ',:)'] = []
    gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'] = []
    gridlayoutOut[id]['k_wall(' + str(r) + ',:)'] = []
    gridlayoutOut[id]['cp_wall(' + str(r) + ',:)'] = []
    for s in range(1, 6): # iterate over number of horisontal layers in wall/roof
        if s <= 3: # fill first three from database
            materialRoof = db_dict['Spartacus Surface'].loc[db_dict['NonVeg'].loc[domTypoRoof[r-1], 'Spartacus Surface'], 'r' + str(s) + 'Material']
            materialWall = db_dict['Spartacus Surface'].loc[db_dict['NonVeg'].loc[domTypoWall[r-1], 'Spartacus Surface'], 'r' + str(s) + 'Material']
            gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'].append(db_dict['Spartacus Surface'].loc[db_dict['NonVeg'].loc[domTypoRoof[r-1], 'Spartacus Surface'], 'r' + str(s) + 'Thickness'])
            gridlayoutOut[id]['k_roof(' + str(r) + ',:)'].append(db_dict['Spartacus Material'].loc[materialRoof]['Thermal Conductivity'])
            gridlayoutOut[id]['cp_roof(' + str(r) + ',:)'].append(db_dict['Spartacus Material'].loc[materialRoof]['Specific Heat'] * 1000)
            gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'].append(db_dict['Spartacus Surface'].loc[db_dict['NonVeg'].loc[domTypoWall[r-1], 'Spartacus Surface'], 'r' + str(s) + 'Thickness'])
            gridlayoutOut[id]['k_wall(' + str(r) + ',:)'].append(db_dict['Spartacus Material'].loc[materialWall]['Thermal Conductivity'])
            gridlayoutOut[id]['cp_wall(' + str(r) + ',:)'].append(db_dict['Spartacus Material'].loc[materialWall]['Specific Heat'] * 1000)
        else: # fill last two with thin wall paper (ignorable)
            gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'].append(0.001)
            gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'].append(0.001)
            gridlayoutOut[id]['k_roof(' + str(r) + ',:)'].append(1.2)
            gridlayoutOut[id]['k_wall(' + str(r) + ',:)'].append(1.2)
            gridlayoutOut[id]['cp_roof(' + str(r) + ',:)'].append(2000000.0)
            gridlayoutOut[id]['cp_wall(' + str(r) + ',:)'].append(2000000.0)

    #update middle layer with new thickness based on updated u-value (backward calculation)
    k_roof1 = gridlayoutOut[id]['k_roof(' + str(r) + ',:)'][0]
    k_roof2 = gridlayoutOut[id]['k_roof(' + str(r) + ',:)'][1]
    k_roof3 = gridlayoutOut[id]['k_roof(' + str(r) + ',:)'][2]
    dz_roof1 = gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'][0]
    #dz_roof2 = gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'][1]
    dz_roof3 = gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'][2]
    k_wall1 = gridlayoutOut[id]['k_wall(' + str(r) + ',:)'][0]
    k_wall2 = gridlayoutOut[id]['k_wall(' + str(r) + ',:)'][1]
    k_wall3 = gridlayoutOut[id]['k_wall(' + str(r) + ',:)'][2]
    dz_wall1 = gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'][0]
    #dz_wall2 = gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'][1]
    dz_wall3 = gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'][2]
    gridlayoutOut[id]['dz_roof(' + str(r) + ',:)'][1] = k_roof2 / gridlayoutOut[id]['u_roof'][r-1] - k_roof2 * ((dz_roof1/k_roof1)+dz_roof3/k_roof3)
    gridlayoutOut[id]['dz_wall(' + str(r) + ',:)'][1] = k_wall2 / gridlayoutOut[id]['u_wall'][r-1] - k_wall2 * ((dz_wall1/k_wall1)+dz_wall3/k_wall3)

test = 4



    
