from time import sleep
import pandas as pd
import numpy as np
from datetime import datetime
#from osgeo.gdalconst import *
import os

def read_DB(db_path):
    '''
    function for reading database and parse it to dictionary of dataframes
    descOrigin is used for indexing and presenting the database entries in a understandable way for the user
    '''

    db_sh = pd.ExcelFile(db_path)
    sheets = db_sh.sheet_names
    db = pd.read_excel(db_path, sheet_name= sheets, index_col= 0)
    # add 
    for col in sheets:
        if col == 'Types':
            db[col]['descOrigin'] = db[col]['Type'].astype(str) + ', ' + db[col]['Origin'].astype(str)
        elif col == 'References': 
            db[col]['authorYear'] = db[col]['Author'].astype(str) + ', ' + db[col]['Publication Year'].astype(str)
        elif col == 'Country':
            db[col]['descOrigin'] = db[col]['Country'].astype(str) + ', ' + db[col]['City'].astype(str)  
        elif col == 'Region':
            pass
        else:
            db[col]['descOrigin'] = db[col]['Description'].astype(str) + ', ' + db[col]['Origin'].astype(str)
    return db

# dict for assigning correct first two digits when creating new codes

code_id_dict = {
    'Region': 10,
    'Country': 11,
    'Types': 12, 

    'NonVeg': 20,
    'Soil': 22,
    'Snow': 23,
    'Veg': 24,
    'Water': 25,

    'Biogen': 30,
    'Leaf Area Index': 31,
    'Leaf Growth Power': 32,
    'MVC': 33,
    'Porosity': 34,
    'Vegetation Growth': 35,
    
    'Emissivity': 40,
    'Albedo': 41,   
    'Water State': 42,
    'Storage': 43,
    'Conductance': 44,
    'Drainage': 45,

    'OHM': 50,
    'ANOHM': 51,
    'ESTM': 52,
    'AnthropogenicEmission': 53,
    
    'Profiles': 60,
    'Irrigation': 61,
    
    'Ref': 90,
}

# for creating new_codes when aggregating
def create_code(table_name):

    sleep(0.0000000000001) # Slow down to make code unique
    table_code = str(code_id_dict[table_name]) 
    doy = str(datetime.now().timetuple().tm_yday)
    ms = str(datetime.utcnow().strftime('%S%f')) # Year%DOY#Minute#millisecond
    code = int(table_code + doy + ms[3:])
    return code

surf_df_dict = {
    'Paved' : 'NonVeg',
    'Buildings' : 'NonVeg',
    'Evergreen Tree' : 'Veg',
    'Decidous Tree' : 'Veg',
    'Grass' : 'Veg',
    'Bare Soil' : 'NonVeg',
    'Water' : 'Water',           
}

def round_dict(in_dict):
    in_dict = {k: round(v, 4) for k, v in in_dict.items()}
    return in_dict 

# function to examine DB if there is a parameter set for country, if not then use parameter set for region 
def decide_country_or_region(col, country_sel, reg):
    if str(country_sel[col].item()) == 'nan':
        var = reg.loc[reg['Region'] == country_sel['Region'].item(), col].item()
    else:
        var = country_sel[col].item()    
    return var

def blend_SUEWS_NonVeg(grid_dict, db_dict, id, parameter_dict):
    '''
    Function for aggregating Building typologies when more than one typology exists in the same grid
    The function needs typology_IDs and fractions to conduct weighted averages using np.average()

    For adding or removing params, do that both in param_list and in new_edit dictionary
    Some parameters are not averageable, or needs to be taken from regional scale, such as SoilTypeCode or OHMThresh_WD
    Drainage Eq and drainagecoefieccents are taken from dominant typology (04/10-23). A checker should be made to check 
    if drainageEQ is the same, then we can aggregate the coefficents, otherwise, just take dominant.

    OHM codes are not averageable. Right now (04/10-23), the dominant is used. This could be solved using a new function
    to aggregate and create new OHM codes. But not done yet.

    TODO Spartacus codes. When set do this

    '''
    values_dict = {} 
    fractions = list(grid_dict[id].values())
    typology_list = list(grid_dict[id].keys())

    dominant_typology = max(grid_dict[id])

    temp_nonveg_dict = {}
    for typology in typology_list:
        temp_nonveg_dict[typology] = fill_SUEWS_NonVeg_typologies(typology, db_dict, parameter_dict)

    param_list = ['AlbedoMin', 'AlbedoMax', 'Emissivity', 'StorageMin', 'StorageMax', 'WetThreshold', 'StateLimit','DrainageEq', 
                    'DrainageCoef1', 'DrainageCoef2', 'SnowLimPatch', 'SnowLimRemove', 'OHMCode_SummerWet', 'OHMCode_SummerDry' ,'OHMCode_WinterWet', 'OHMCode_WinterDry',
                    'OHMThresh_SW','OHMThresh_WD','ESTMCode','AnOHM_Cp' ,'AnOHM_Kk' ,'AnOHM_Ch' ]
    
    '''
        Iterate over parameters and typologies to get values for each parameter in typology as list to be able to do weighted averages
        so the dict becomes
        values_dict = {
            'AlbedoMin' : [0.5, 0.6],
            'AlbedoMax' : [0.5, 0,6]...} 
        The order is always the same as the fractions as retrieved above
    '''

    for param in param_list:
        p_list = []
        for typology in typology_list:
            p_list.append(temp_nonveg_dict[typology][param]) 
        values_dict[param] = p_list

    # Weighted averages here. For non averageable, now dominant is used.
    new_edit = {
        'Code' : create_code('NonVeg'), # Give new Code
        'AlbedoMin' :   np.average((values_dict['AlbedoMin']), weights = fractions),
        'AlbedoMax' :   np.average((values_dict['AlbedoMax']), weights = fractions),
        'Emissivity' : np.average((values_dict['Emissivity']), weights = fractions),
        'StorageMin' :  np.average((values_dict['StorageMin']), weights = fractions),
        'StorageMax' : np.average((values_dict['StorageMax']), weights = fractions),
        'WetThreshold' : np.average((values_dict['WetThreshold']), weights = fractions),
        'StateLimit' : np.average((values_dict['StateLimit']), weights = fractions),
        'DrainageEq' : temp_nonveg_dict[dominant_typology]['DrainageEq'], # NEED FIXING!
        'DrainageCoef1' : temp_nonveg_dict[dominant_typology]['DrainageCoef1'],
        'DrainageCoef2' : temp_nonveg_dict[dominant_typology]['DrainageCoef2'],
        'SoilTypeCode' : parameter_dict['SoilTypeCode'],
        'SnowLimPatch' : np.average((values_dict['SnowLimPatch']), weights = fractions),
        'SnowLimRemove' : np.average((values_dict['SnowLimRemove']), weights = fractions),
        # 'OHMCode_SummerWet' : not avearageable 
        # 'OHMCode_SummerDry' : not avearageable 
        # 'OHMCode_WinterWet' : not avearageable 
        # 'OHMCode_WinterDry' : not avearageable 
        'OHMThresh_SW' : 10, # TODO set regional Country 
        'OHMThresh_WD' : 0.9, # TODO set regional Country
        'ESTMCode' : -9999 ,#not used 
        'AnOHM_Cp' : np.average((values_dict['AnOHM_Cp']), weights = fractions),
        'AnOHM_Kk' : np.average((values_dict['AnOHM_Kk']), weights = fractions),
        'AnOHM_Ch' : np.average((values_dict['AnOHM_Ch']), weights = fractions),
    }

    for column in ['OHMCode_SummerWet', 'OHMCode_SummerDry' ,'OHMCode_WinterWet', 'OHMCode_WinterDry']:
        if len(set(values_dict[column])) == 1:  # If all typologies has same code, just use this 
            new_edit[column] = values_dict[column][0]
        else:
            new_edit[column] = values_dict[column][0] # TODO need to make a new blend OHM function if these are not the same
            
    return new_edit

def fill_SUEWS_NonVeg_typologies(code, db_dict, parameter_dict):
    '''
    Function for retrieving correct parameters from DB according to typology. 
    This works for Paved, Buildings and Bare Soil
    code is the typology code. 
    When adding new parameters, just create new lines and slice DB using similar as of now
    '''
    table_dict = {
        'Code' : code,
        'AlbedoMin' :   db_dict['Albedo'].loc[db_dict['NonVeg'].loc[code, 'Albedo'], 'Alb_min'],
        'AlbedoMax' :   db_dict['Albedo'].loc[db_dict['NonVeg'].loc[code, 'Albedo'], 'Alb_max'],
        'Emissivity' : db_dict['Emissivity'].loc[db_dict['NonVeg'].loc[code, 'Emissivity'], 'Emissivity'],
        'StorageMin' :  db_dict['Water Storage'].loc[db_dict['NonVeg'].loc[code, 'Water Storage'], 'StorageMin'],
        'StorageMax' : db_dict['Water Storage'].loc[db_dict['NonVeg'].loc[code, 'Water Storage'], 'StorageMax'],
        'WetThreshold' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[code, 'Drainage'], 'WetThreshold'],
        'StateLimit' : -9999, # Not used for Non Veg
        'DrainageEq' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[code, 'Drainage'], 'DrainageEq'],
        'DrainageCoef1' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[code, 'Drainage'], 'DrainageCoef1'],
        'DrainageCoef2' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[code, 'Drainage'], 'DrainageCoef2'],
        'SoilTypeCode' : parameter_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
        'SnowLimPatch' : 190, # TODO Set regional
        'SnowLimRemove': 90,    # TODO Set regional
        'OHMCode_SummerWet' : db_dict['NonVeg'].loc[code, 'OHMSummerWet'],
        'OHMCode_SummerDry' : db_dict['NonVeg'].loc[code, 'OHMSummerDry'],
        'OHMCode_WinterWet' : db_dict['NonVeg'].loc[code, 'OHMWinterWet'],
        'OHMCode_WinterDry' : db_dict['NonVeg'].loc[code, 'OHMWinterDry'],
        'OHMThresh_SW' : 10, # TODO Set regional
        'OHMThresh_WD' : 0.9, # TODO Set regional
        'ESTMCode' : db_dict['NonVeg'].loc[code, 'ESTM'],
        'AnOHM_Cp' : db_dict['ANOHM'].loc[db_dict['NonVeg'].loc[code, 'ANOHM'],  'AnOHM_Cp'],
        'AnOHM_Kk' : db_dict['ANOHM'].loc[db_dict['NonVeg'].loc[code, 'ANOHM'],  'AnOHM_Kk'],
        'AnOHM_Ch' : db_dict['ANOHM'].loc[db_dict['NonVeg'].loc[code, 'ANOHM'],  'AnOHM_Ch'],
        }
    
    return table_dict

def fill_SUEWS_NonVeg(db_dict, column_dict):    
    '''
    This function is used to assign correct params to selected NonVeg codes when not using typologies
    Fills for all surfaces
    '''
    table_dict = {}

    for surface in ['Paved', 'Buildings', 'Bare Soil']:
        table_dict[surface] = {}

        locator = column_dict[surface]
        table_dict[surface] = {
            'Code' : locator,
            'AlbedoMin' :   db_dict['Albedo'].loc[db_dict['NonVeg'].loc[locator, 'Albedo'], 'Alb_min'],
            'AlbedoMax' :   db_dict['Albedo'].loc[db_dict['NonVeg'].loc[locator, 'Albedo'], 'Alb_max'],
            'Emissivity' : db_dict['Emissivity'].loc[db_dict['NonVeg'].loc[locator, 'Emissivity'], 'Emissivity'],
            'StorageMin' :  db_dict['Water Storage'].loc[db_dict['NonVeg'].loc[locator, 'Water Storage'], 'StorageMin'],
            'StorageMax' : db_dict['Water Storage'].loc[db_dict['NonVeg'].loc[locator, 'Water Storage'], 'StorageMax'],
            'WetThreshold' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[locator, 'Drainage'], 'WetThreshold'],
            'StateLimit' : -9999, # Not used for Non Veg
            'DrainageEq' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[locator, 'Drainage'], 'DrainageEq'],
            'DrainageCoef1' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[locator, 'Drainage'], 'DrainageCoef1'],
            'DrainageCoef2' : db_dict['Drainage'].loc[db_dict['NonVeg'].loc[locator, 'Drainage'], 'DrainageCoef2'],
            'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
            'SnowLimPatch' : 190,
            'SnowLimRemove': 90,    
            'OHMCode_SummerWet' : db_dict['NonVeg'].loc[locator, 'OHMSummerWet'],
            'OHMCode_SummerDry' : db_dict['NonVeg'].loc[locator, 'OHMSummerDry'],
            'OHMCode_WinterWet' : db_dict['NonVeg'].loc[locator, 'OHMWinterWet'],
            'OHMCode_WinterDry' : db_dict['NonVeg'].loc[locator, 'OHMWinterDry'],
            'OHMThresh_SW' : 10, # table.loc[locator, 'OHMThresh_SW'],
            'OHMThresh_WD' : 0.9, #table.loc[locator, 'OHMThresh_WD'],
            'ESTMCode' : db_dict['NonVeg'].loc[locator, 'ESTM'],
            'AnOHM_Cp' : db_dict['ANOHM'].loc[db_dict['NonVeg'].loc[locator, 'ANOHM'],  'AnOHM_Cp'],
            'AnOHM_Kk' : db_dict['ANOHM'].loc[db_dict['NonVeg'].loc[locator, 'ANOHM'],  'AnOHM_Kk'],
            'AnOHM_Ch' : db_dict['ANOHM'].loc[db_dict['NonVeg'].loc[locator, 'ANOHM'],  'AnOHM_Ch'],
        }
                    
    return table_dict

def fill_SUEWS_Water(locator, db_dict, column_dict):
    '''
    This function is used to assign correct params to selected Water code
    Locator is 
    '''
    table_dict = {}
    table_dict['Water'] = {
        'Code' : locator,
        'AlbedoMin' :   db_dict['Albedo'].loc[db_dict['Water'].loc[locator, 'Albedo'], 'Alb_min'],
        'AlbedoMax' :   db_dict['Albedo'].loc[db_dict['Water'].loc[locator, 'Albedo'], 'Alb_max'],
        'Emissivity' : db_dict['Emissivity'].loc[db_dict['Water'].loc[locator, 'Emissivity'], 'Emissivity'],
        'StorageMin' :  db_dict['Water Storage'].loc[db_dict['Water'].loc[locator, 'Water Storage'], 'StorageMin'],
        'StorageMax' : db_dict['Water Storage'].loc[db_dict['Water'].loc[locator, 'Water Storage'], 'StorageMax'],
        'WetThreshold' : db_dict['Drainage'].loc[db_dict['Water'].loc[locator, 'Drainage'], 'WetThreshold'],
        'StateLimit' : db_dict['Water State'].loc[db_dict['Water'].loc[locator, 'Water State'], 'StateLimit'],
        'WaterDepth' : db_dict['Water State'].loc[db_dict['Water'].loc[locator, 'Water State'], 'WaterDepth'],
        'DrainageEq' : db_dict['Drainage'].loc[db_dict['Water'].loc[locator, 'Drainage'], 'DrainageEq'],
        'DrainageCoef1' : db_dict['Drainage'].loc[db_dict['Water'].loc[locator, 'Drainage'], 'DrainageCoef1'],
        'DrainageCoef2' : db_dict['Drainage'].loc[db_dict['Water'].loc[locator, 'Drainage'], 'DrainageCoef2'],
        'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
        'SnowLimPatch' : -9999,
        'SnowLimRemove': -9999,    
        'OHMCode_SummerWet' : db_dict['Water'].loc[locator, 'OHMSummerWet'],
        'OHMCode_SummerDry' : db_dict['Water'].loc[locator, 'OHMSummerDry'],
        'OHMCode_WinterWet' : db_dict['Water'].loc[locator, 'OHMWinterWet'],
        'OHMCode_WinterDry' : db_dict['Water'].loc[locator, 'OHMWinterDry'],
        'OHMThresh_SW' : 10, # table.loc[locator, 'OHMThresh_SW'],
        'OHMThresh_WD' : 0.9, #table.loc[locator, 'OHMThresh_WD'],
        'ESTMCode' : db_dict['Water'].loc[locator, 'ESTM'],
        'AnOHM_Cp' : db_dict['ANOHM'].loc[db_dict['Water'].loc[locator, 'ANOHM'],  'AnOHM_Cp'],
        'AnOHM_Kk' : db_dict['ANOHM'].loc[db_dict['Water'].loc[locator, 'ANOHM'],  'AnOHM_Kk'],
        'AnOHM_Ch' : db_dict['ANOHM'].loc[db_dict['Water'].loc[locator, 'ANOHM'],  'AnOHM_Ch'],
    }
    return table_dict

def fill_SUEWS_Veg(db_dict, column_dict ):
    '''
    This function is used to assign correct params to selected Veg codes 
    Fills for all surfaces (grass, evergreen trees, decidous trees)
    '''
    table = db_dict['Veg']
    table_dict = {}
    
    
    for surface in ['Evergreen Tree', 'Decidous Tree', 'Grass']:
        table_dict[surface] = {}

        locator = column_dict[surface]
        table_dict[surface] = {
                'Code' : locator,
                'AlbedoMin' :   db_dict['Albedo'].loc[table.loc[locator, 'Albedo'], 'Alb_min'],
                'AlbedoMax' :   db_dict['Albedo'].loc[table.loc[locator, 'Albedo'], 'Alb_max'],
                'Emissivity' : db_dict['Emissivity'].loc[table.loc[locator, 'Emissivity'], 'Emissivity'],
                'StorageMin' :  db_dict['Water Storage'].loc[table.loc[locator, 'Water Storage'], 'StorageMin'],
                'StorageMax' : db_dict['Water Storage'].loc[table.loc[locator, 'Water Storage'], 'StorageMax'],
                'WetThreshold' : db_dict['Drainage'].loc[table.loc[locator, 'Drainage'], 'WetThreshold'],
                'StateLimit' : db_dict['Water State'].loc[table.loc[locator, 'Water State'], 'StateLimit'],
                'DrainageEq' : db_dict['Drainage'].loc[table.loc[locator, 'Drainage'], 'DrainageEq'],
                'DrainageCoef1' : db_dict['Drainage'].loc[table.loc[locator, 'Drainage'], 'DrainageCoef1'],
                'DrainageCoef2' : db_dict['Drainage'].loc[table.loc[locator, 'Drainage'], 'DrainageCoef2'],
                'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
                'SnowLimPatch' : 190, # TODO set regional
                'BaseT' :       db_dict['Vegetation Growth'].loc[column_dict['Vegetation Growth'], 'BaseT'], #db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'BaseT'],
                'BaseTe' :      db_dict['Vegetation Growth'].loc[column_dict['Vegetation Growth'], 'BaseTe'],#db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'BaseTe'],
                'GDDFull' :     db_dict['Vegetation Growth'].loc[column_dict['Vegetation Growth'], 'GDDFull'],#db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'GDDFull'],
                'SDDFull' :     db_dict['Vegetation Growth'].loc[column_dict['Vegetation Growth'], 'SDDFull'],#db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'SDDFull'],
                'LAIMin' :      db_dict['Leaf Area Index'].loc[table.loc[locator, 'Leaf Area Index'], 'LAIMin'],
                'LAIMax' :      db_dict['Leaf Area Index'].loc[table.loc[locator, 'Leaf Area Index'], 'LAIMax'],
                'PorosityMin' : db_dict['Porosity'].loc[table.loc[locator, 'Porosity'], 'PorosityMin'],
                'PorosityMax' : db_dict['Porosity'].loc[table.loc[locator, 'Porosity'], 'PorosityMax'],
                'MaxConductance' : db_dict['Max Vegetation Conductance'].loc[table.loc[locator, 'Max Vegetation Conductance'], 'MaxConductance'],
                'LAIEq' :       db_dict['Leaf Area Index'].loc[table.loc[locator, 'Leaf Area Index'], 'LAIEq'],
                'LeafGrowthPower1' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafGrowthPower1'],
                'LeafGrowthPower2' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafGrowthPower2'],
                'LeafOffPower1' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafOffPower1'],
                'LeafOffPower2' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafOffPower2'],    
                'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
                'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
                'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
                'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
                'OHMThresh_SW' : 10,#table.loc[locator, 'OHMThresh_SW'],# TODO set regional
                'OHMThresh_WD' : 0.9,#table.loc[locator, 'OHMThresh_WD'],# TODO set regional
                'ESTMCode' : table.loc[locator, 'ESTM'],
                'AnOHM_Cp' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                'AnOHM_Kk' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                'AnOHM_Ch' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
                'BiogenCO2Code' : column_dict['Biogen'] #table.loc[locator, 'BIOGEN']
            }
    return table_dict

def fill_SUEWS_Snow(locator, db_dict):
    
    '''
    This function is used to assign correct params to selected Snow code
    Locator is selected code
    '''

    table_dict = {
        'Code' : locator,
        'RadMeltFactor' : db_dict['Snow'].loc[locator, 'RadMeltFactor'], 
        'TempMeltFactor': db_dict['Snow'].loc[locator, 'TempMeltFactor'],
        'AlbedoMin' : db_dict['Albedo'].loc[db_dict['Snow'].loc[locator, 'Albedo'], 'Alb_min'],
        'AlbedoMax' : db_dict['Albedo'].loc[db_dict['Snow'].loc[locator, 'Albedo'], 'Alb_max'],
        'Emissivity' : db_dict['Emissivity'].loc[db_dict['Snow'].loc[locator, 'Emissivity'], 'Emissivity'],
        'tau_a' : db_dict['Snow'].loc[locator, 'tau_a'],
        'tau_f' : db_dict['Snow'].loc[locator, 'tau_f'],
        'PrecipLimAlb' : db_dict['Snow'].loc[locator, 'PrecipLimAlb'],
        'SnowDensMin' : db_dict['Snow'].loc[locator, 'SnowDensMin'],
        'SnowDensMax' : db_dict['Snow'].loc[locator, 'SnowDensMax'],
        'tau_r' : db_dict['Snow'].loc[locator, 'tau_r'], 
        'CRWMin' : db_dict['Snow'].loc[locator, 'CRWMin'],
        'CRWMax' : db_dict['Snow'].loc[locator, 'CRWMax'],
        'PrecipLimSnow' : db_dict['Snow'].loc[locator, 'PrecipLimSnow'],
        'OHMCode_SummerWet' : db_dict['Snow'].loc[locator, 'OHMCode_SummerWet'],
        'OHMCode_SummerDry' : db_dict['Snow'].loc[locator, 'OHMCode_SummerDry'],
        'OHMCode_WinterWet' : db_dict['Snow'].loc[locator, 'OHMCode_WinterWet'],
        'OHMCode_WinterDry' : db_dict['Snow'].loc[locator, 'OHMCode_WinterDry'],
        'OHMThresh_SW' : 10,
        'OHMThresh_WD' : 0.9,
        'ESTMCode' : db_dict['Snow'].loc[locator, 'ESTM'],
        'AnOHM_Cp' : db_dict['ANOHM'].loc[db_dict['Snow'].loc[locator, 'ANOHM'],  'AnOHM_Cp'],
        'AnOHM_Kk' : db_dict['ANOHM'].loc[db_dict['Snow'].loc[locator, 'ANOHM'],  'AnOHM_Kk'],
        'AnOHM_Ch' : db_dict['ANOHM'].loc[db_dict['Snow'].loc[locator, 'ANOHM'],  'AnOHM_Ch'],
    }

    return table_dict

def fill_SUEWS_AnthropogenicEmission(locator, parameter_dict, db_dict):
    '''
    This function is used to assign correct params to selected Snow code
    Locator is selected code
    This needs to be fiddled with
    # TODO what params should be regional and not? Which ones should be removed
    '''
    
    table = db_dict['AnthropogenicEmission']

    table_dict = {
        'Code' : locator,
        'BaseT_HC' : table.loc[locator, 'BaseT_HC'],
        'QF_A_WD' : table.loc[locator, 'QF_A_WD'], 
        'QF_B_WD' : table.loc[locator, 'QF_B_WD'],
        'QF_C_WD' : table.loc[locator, 'QF_C_WD'],
        'QF_A_WE' : table.loc[locator, 'QF_A_WE'],
        'QF_B_WE' : table.loc[locator, 'QF_B_WE'],
        'QF_C_WE' : table.loc[locator, 'QF_C_WE'],
        'AHMin_WD' : table.loc[locator, 'AHMin_WD'],
        'AHMin_WE' : table.loc[locator, 'AHMin_WE'],
        'AHSlope_Heating_WD' : table.loc[locator, 'AHSlope_Heating_WD'],
        'AHSlope_Heating_WE' : table.loc[locator, 'AHSlope_Heating_WE'],
        'AHSlope_Cooling_WD' : table.loc[locator, 'AHSlope_Cooling_WD'],
        'AHSlope_Cooling_WE' : table.loc[locator, 'AHSlope_Cooling_WE'],
        'TCritic_Heating_WD' : table.loc[locator, 'TCritic_Heating_WD'],
        'TCritic_Heating_WE' : table.loc[locator, 'TCritic_Heating_WE'],
        'TCritic_Cooling_WD' : table.loc[locator, 'TCritic_Cooling_WD'],
        'TCritic_Cooling_WE' : table.loc[locator, 'TCritic_Cooling_WE'],
        'EnergyUseProfWD' : parameter_dict['EnergyUseProfWD'], #.item() ,
        'EnergyUseProfWE' : parameter_dict['EnergyUseProfWE'], #.item() ,
        'ActivityProfWD' : parameter_dict['ActivityProfWD'], #.item(),
        'ActivityProfWE' : parameter_dict['ActivityProfWE'], #.item(),
        'TraffProfWD' : parameter_dict['TrafficRate_WD'], #.item(),
        'TraffProfWE' : parameter_dict['TrafficRate_WD'], #.item(),
        'PopProfWD' : parameter_dict['PopProfWD'], #.item(),
        'PopProfWE' : parameter_dict['PopProfWE'], #.item() ,
        'MinQFMetab' : table.loc[locator, 'MinQFMetab'],
        'MaxQFMetab' : table.loc[locator, 'MaxQFMetab'],
        'MinFCMetab' : table.loc[locator, 'MinFCMetab'],
        'MaxFCMetab' : table.loc[locator, 'MaxFCMetab'],
        'FrPDDwe' : table.loc[locator, 'FrPDDwe'],
        'FrFossilFuel_Heat' : table.loc[locator, 'FrFossilFuel_Heat'],
        'FrFossilFuel_NonHeat' : table.loc[locator, 'FrFossilFuel_NonHeat'],
        'EF_umolCO2perJ' : table.loc[locator, 'EF_umolCO2perJ'],
        'EnEF_v_Jkm' : table.loc[locator, 'EnEF_v_Jkm'],
        'FcEF_v_kgkmWD' : table.loc[locator, 'FcEF_v_kgkmWD'],
        'FcEF_v_kgkmWE' : table.loc[locator, 'FcEF_v_kgkmWE'],
        'CO2PointSource' : table.loc[locator, 'CO2PointSource'],
        'TrafficUnits' : table.loc[locator, 'TrafficUnits'],
    }
    
    return table_dict

# Aggregation of OHM Parameters
def new_table_edit(db_dict, table_dict, values, param, name, frac_dict, surface):

    weights_dict = {}
    weights_dict = {k : 0 for k in values['Code'].keys()}

    for i in list(values['Code'].keys()):
        weights_dict[i] ={}
        weights_dict[i]['Code'] = values[param][i]
        weights_dict[i]['Weight'] = + frac_dict[i]

    # Create dicts for fractions to be aggregates
    weight_dict_merged ={}
    weight_dict_merged['Code'] = {}
    for i in weights_dict:
        weight_dict_merged['Code'][weights_dict[i]['Code']] = 0

    for i in weights_dict:
        code = weights_dict[i]['Code'] 
        weight = weights_dict[i]['Weight']
        weight_dict_merged['Code'][code] = weight_dict_merged['Code'][code] + weight

    #params = different ohm codes in db_dict['OHM']

    blend_edit_dict = {}

    # edit_params = (list(edit_dict[type].keys()))
    edit_params = list(values[param].values())

    edit_params = list(values[param].values())

    for edit_code in ['a1', 'a2', 'a3']:
        blend_edit_dict[edit_code] = {}
        for p in edit_params:
            blend_edit_dict[edit_code][p] = db_dict['OHM'].loc[p,edit_code]

    new_code = create_code(name)

    new_edit_dict = {'Code' : new_code}
    weight = list(list(weight_dict_merged['Code'].values()))


    for i in ['a1', 'a2', 'a3']:
        try:
            new_edit_dict[i] = np.average(list(blend_edit_dict[i].values()), weights = weight)
        except:
            new_edit_dict[i] = -999.

    table_dict[surface][param]  = new_edit_dict['Code']
    new_edit_dict['Code'] = new_code

    dict_df = pd.DataFrame(new_edit_dict, index = [0]).set_index('Code')

    dict_df = dict_df.rename_axis('ID')
    db_dict[name] = pd.concat([db_dict[name], dict_df])

    return db_dict


def fill_SUEWS_profiles(profiles_list ,save_folder, prof):
    '''
    This function is used to assign correct profiles
    Locator is selected code
    This function also saves the profiles to .txt
    '''

    df_m = pd.DataFrame()

    for locator in profiles_list:
        table_dict = {
            'Code' : locator,
            '0'  : prof.loc[locator, 0],
            '1'  : prof.loc[locator, 1],
            '2'  : prof.loc[locator, 2],
            '3'  : prof.loc[locator, 3],
            '4'  : prof.loc[locator, 4],
            '5'  : prof.loc[locator, 5],
            '6'  : prof.loc[locator, 6],
            '7'  : prof.loc[locator, 7],
            '8'  : prof.loc[locator, 8],
            '9'  : prof.loc[locator, 9],
            '10' : prof.loc[locator, 10],
            '11' : prof.loc[locator, 11],
            '12' : prof.loc[locator, 12],
            '13' : prof.loc[locator, 13],
            '14' : prof.loc[locator, 14],
            '15' : prof.loc[locator, 15],
            '16' : prof.loc[locator, 16],
            '17' : prof.loc[locator, 17],
            '18' : prof.loc[locator, 18],
            '19' : prof.loc[locator, 19],
            '20' : prof.loc[locator, 20],
            '21' : prof.loc[locator, 21],
            '22' : prof.loc[locator, 22],
            '23' : prof.loc[locator, 23],
        }
        dict_df = pd.DataFrame(table_dict, index = [0])#.set_index('Code')
        df_m = pd.concat([df_m, dict_df]).drop_duplicates(keep='first')

    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]
    # add -9 rows to text files
    df_m = df_m.swaplevel(0,1,1)
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + 'SUEWS_Profiles.txt', sep = '\t' ,index = False)

def save_SUEWS_txt(df_m, table_name, save_folder, db_dict):
    '''
    This function is used to prepare the data and saving into correct way for the .txt files used in SUEWS
    # TODO Add comment column in the end and specify where the specific code is used
    '''
    col = ['General Type', 'Surface', 'Description', 'Origin', 'Ref', 'Season', 'Day' ,'Profile Type', 'descOrigin']
    dropFilter = df_m.filter(col)
    df_m.drop(dropFilter, inplace= True, axis = 1)
    df_m.reset_index(inplace = True)
    df_m = df_m.round(4)

    if table_name == 'SUEWS_AnthropogenicEmission.txt':
        cd = list(df_m.filter(like='Code').columns)
        cd = cd + list(df_m.filter(like= 'WD'))
        cd = cd + list(df_m.filter(like= 'WE'))

    else:
        cd = df_m.filter(like='Code').columns

    try:
        df_m['Code'] = df_m['Code'].apply(lambda x: x)
    except:
        pass

     # These two columns are made for adding information on what the code is inside the .txt file
    df_m['!'] = '!'
    df_m[''] = ''
    table_name_short = table_name[6:-4] # table_name_short is used to set correct table in DB. all table names are such as "SUEWS_table.txt, and DB only wants table"

    if table_name == 'SUEWS_OHMCoefficients.txt':
        table_name_short = 'OHM'

    df_m = df_m.set_index('Code')
    for idx in list(df_m.index):
        
        if table_name == 'SUEWS_Veg.txt' or table_name == 'SUEWS_NonVeg.txt':
            surface_sel = db_dict[table_name_short].loc[idx,'Surface'] # table_name_short is used to set correct table in DB. all table names are such as "SUEWS_table.txt, and DB only wants table"
            descOrigin_sel = db_dict[table_name_short].loc[idx,'descOrigin']
            id_string = surface_sel + ', ' + descOrigin_sel
        else:
            descOrigin_sel = db_dict[table_name_short].loc[idx,'descOrigin']
            id_string =  descOrigin_sel

        df_m.loc[idx,''] = id_string

    df_m = df_m.reset_index()
    # Add column numbers 1-max columns needed for .txt files
    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]
    # add -9 rows to text files
    df_m = df_m.swaplevel(0,1,1)
    # This can probably be done better. Used pd.append() but this will be deprecated. This works, but not the most clean coding
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + table_name, sep = '\t' ,index = False)
    
def save_snow(snow_dict, save_folder, db_dict):
    '''
    This function is used to save to .txt file related to Snow
    '''
    df_m = pd.DataFrame.from_dict(snow_dict, orient = 'index').T

    # These two columns are made for adding information on what the code is inside the .txt file
    df_m['!'] = '!'
    df_m[''] = np.nan

    idx = df_m['Code'].item()

    descOrigin_sel = db_dict['Snow'].loc[idx,'descOrigin']
    df_m[''] = descOrigin_sel

    column_list = list(range(1, len(df_m.columns)+1))
    df_m.columns = [df_m.columns, column_list]

    df_m = df_m.swaplevel(0,1,1)
    # This can probably be done better. Used pd.append() but this will be deprecated. This works, but not the most clean coding
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + 'SUEWS_Snow.txt', sep = '\t' ,index = False)

def save_NonVeg_types(nonveg_dict, save_folder, db_dict):

    '''
    This function is used to save to .txt file related to NonVeg
    '''

    df_m = pd.DataFrame()
    for id in list(nonveg_dict.keys()):
        for surf in ['Paved', 'Buildings','Bare Soil']:
            df_m = pd.concat([df_m, pd.DataFrame.from_dict(nonveg_dict[id][surf], orient='index').T]).drop_duplicates()

    # These two columns are made for adding information on what the code is inside the .txt file
    df_m['!'] = '!'
    df_m[''] = np.nan
    
    for idx in list(df_m.set_index('Code').index):
        try:
            surface_sel = db_dict['NonVeg'].loc[idx,'Surface'] 
            descOrigin_sel = db_dict['NonVeg'].loc[idx,'descOrigin']
            id_string = surface_sel + ', ' + descOrigin_sel
            df_m.loc[idx,''] = id_string
        except:
            df_m.loc[idx,''] = 'Buildings, aggregated from X(fraction) Y(fracion) Z(fraction)'
                
    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]
    # add -9 rows to text files
    
    df_m = df_m.swaplevel(0,1,1)
    # This can probably be done better. Used pd.append() but this will be deprecated. This works, but not the most clean coding
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + 'SUEWS_NonVeg.txt', sep = '\t' ,index = False)

def save_SiteSelect(ss_dict, save_folder, path_to_ss):
    '''
    This function is used to save to SUEWS_SiteSelect.txt
    '''
    df_m = pd.DataFrame.from_dict(ss_dict).T
    ss_txt = pd.read_csv(path_to_ss, delim_whitespace=True, skiprows=1)
    df_m = df_m.reset_index()
    df_m = df_m.rename(columns={'index' : 'Grid'})
    df_m['Grid'] = df_m['Grid'].apply(str)
    df_m = df_m[ss_txt.columns]
    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]
    df_m = df_m.round(4)

    cd = list(df_m.filter(like='Code').columns)
    cd = cd + list(df_m.filter(like= 'WD'))
    cd = cd + list(df_m.filter(like= 'WE'))

    df_m = df_m.swaplevel(0,1,1)
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + 'SUEWS_SiteSelect.txt', sep = '\t' ,index = False)

def presave(table, name ,var_list, save_folder, db_dict):
    '''
    This function is used to prepare some of the data used to be able to save to .txt
    '''
    df = table.loc[var_list]
    df = df.drop(columns=df.select_dtypes(include='object').columns).rename_axis('Code')
    save_SUEWS_txt(df, ('SUEWS_' + name + '.txt'), save_folder, db_dict)


def read_morph_txt(txt_file):
    '''
    This function is used to read output files from morphometric calculator .txt
    '''
    morph_dict = pd.read_csv(txt_file, delim_whitespace=True, index_col=[0]).to_dict(orient='index')
    return morph_dict  


# Not used. To big TimeZones.shp file
# def get_utc(grid_path, timezone):
    
#     # timezone
#     grid = gpd.read_file(grid_path)
    
#     # Set of grid to crs of timezones vectorlayer (WGS 84)
#     grid_crs = grid.to_crs(timezone.crs)

#     try:
#         spatial_join = gpd.sjoin(left_df=grid_crs,
#                                     right_df=timezone,
#                                     how="inner", op='within')
#         utc = spatial_join.iloc[0]['zone']
#     except:
#         utc = 0
#         print('UTC calc not working')
#     return utc

