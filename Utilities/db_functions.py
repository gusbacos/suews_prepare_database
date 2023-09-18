from time import sleep
import pandas as pd
import numpy as np
from datetime import datetime
from osgeo.gdalconst import *
import os

def read_DB(db_path):
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


table_id_dict = {
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
    'VegetationGrowth': 35,
    
    'Emissivity': 40,
    'Albedo': 41,   
    'WaterState': 42,
    'Storage': 43,
    'Conductance': 44,
    'Drainage': 45,

    'OHM': 50,
    'ANOHM': 51,
    'ESTM': 52,
    'AnEm': 53,
    
    'Profiles': 60,
    'Irrigation': 61,
    
    'Ref': 90,
}

# for creating new_codes when aggregating
def create_code(table_name):

    sleep(0.0000000000001) # Slow down to make code unique
    table_code = str(table_id_dict[table_name]) 
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
# def fill_SUEWS_NonVeg(Type, table, alb, em, st, dr, ANOHM, ws, column_dict, urbType = False,):    

def fill_SUEWS_NonVeg(db_dict, column_dict, urbType = False,):    
    table_dict = {}
    surf_list = ['Paved', 'Buildings', 'Bare Soil']
    if urbType != False:
        for i in surf_list:
            table_dict[i] = {}
            for j in urbType:
                if i == 'Bare Soil':
                    locator = column_dict['Bare Soil']
                else:
                    locator = db_dict['Types'].loc[j, i]

                table_dict[i][j] = {
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
                
    elif urbType == False:
        for i in surf_list:
            table_dict[i] = {}

            locator = column_dict[i]
            table_dict[i] = {
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

def fill_SUEWS_Veg(db_dict, column_dict , urbType = False):

    table = db_dict['Veg']
    table_dict = {}
    
    if urbType != False:
        for i in ['Evergreen Tree', 'Decidous Tree', 'Grass']:
            table_dict[i] = {}
            for j in urbType:

                locator = db_dict['Types'].loc[j, i]
                table_dict[i][j] = {
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
                    'SnowLimPatch' : 190,
                    'BaseT' :       db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'BaseT'],
                    'BaseTe' :      db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'BaseTe'],
                    'GDDFull' :     db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'GDDFull'],
                    'SDDFull' :     db_dict['Vegetation Growth'].loc[table.loc[locator, 'Vegetation Growth'], 'SDDFull'],
                    'LAIMin' :      db_dict['Leaf Area Index'].loc[table.loc[locator, 'Leaf Area Index'], 'LAIMin'],
                    'LAIMax' :      db_dict['Leaf Area Index'].loc[table.loc[locator, 'Leaf Area Index'], 'LAIMax'],
                    'LAIEq' :       db_dict['Leaf Area Index'].loc[table.loc[locator, 'Leaf Area Index'], 'LAIEq'],
                    'PorosityMin' : db_dict['Porosity'].loc[table.loc[locator, 'Porosity'], 'PorosityMin'],
                    'PorosityMax' : db_dict['Porosity'].loc[table.loc[locator, 'Porosity'], 'PorosityMax'],
                    'MaxConductance' : db_dict['Max Vegetation Conductance'].loc[table.loc[locator, 'Max Vegetation Conductance'], 'MaxConductance'],
                    'LeafGrowthPower1' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafGrowthPower1'],
                    'LeafGrowthPower2' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafGrowthPower2'],
                    'LeafOffPower1' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafOffPower1'],
                    'LeafOffPower2' : db_dict['Leaf Growth Power'].loc[table.loc[locator, 'Leaf Growth Power'], 'LeafOffPower2'],    
                    'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
                    'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
                    'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
                    'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
                    'OHMThresh_SW' : 10,#table.loc[locator, 'OHMThresh_SW'],
                    'OHMThresh_WD' : 0.9,#table.loc[locator, 'OHMThresh_WD'],
                    'ESTMCode' : table.loc[locator, 'ESTM'],
                    'AnOHM_Cp' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                    'AnOHM_Kk' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                    'AnOHM_Ch' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
                    'BiogenCO2Code' : column_dict['Biogen']
                }


    elif urbType == False:
        for i in ['Evergreen Tree', 'Decidous Tree', 'Grass']:
            table_dict[i] = {}

            locator = column_dict[i]
            table_dict[i] = {
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
                    'SnowLimPatch' : 190,
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
                    'OHMThresh_SW' : 10,#table.loc[locator, 'OHMThresh_SW'],
                    'OHMThresh_WD' : 0.9,#table.loc[locator, 'OHMThresh_WD'],
                    'ESTMCode' : table.loc[locator, 'ESTM'],
                    'AnOHM_Cp' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                    'AnOHM_Kk' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                    'AnOHM_Ch' : db_dict['ANOHM'].loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
                    'BiogenCO2Code' : column_dict['Biogen'] #table.loc[locator, 'BIOGEN']
                }
    return table_dict

def fill_SUEWS_Snow(snow_sel, db_dict):
    
    locator = snow_sel

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

def fill_SUEWS_AnthropogenicEmission(AnthropogenicCode, parameter_dict, db_dict):
    table = db_dict['AnthropogenicEmission']
    locator = AnthropogenicCode

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

# Not used at the moment. Pehaps in future. Beware of many equation choices that cannot be averaged
# def blend_veg(in_dict, surface, frac_dict_lc, id, ESTM, OHM, BIOCO2, type_id_dict, frac_to_surf_dict):

#     table_dict = {}
#     values = in_dict[id][surface]
#     code_order = list(values.keys())
#     rev_frac_to_surf_dict = dict((v, k) for k, v in frac_to_surf_dict.items())
#     surf = rev_frac_to_surf_dict[surface]
    

#     fractions = {}
#     for typology in frac_dict_lc[id].keys():
#         try: 
#             fractions[typology] = frac_dict_lc[id][typology][surf]
#         except:
#             fractions[typology] = 1 / len(list(frac_dict_lc[id].keys())) # How to deal with this?

#     code = create_code('Veg')

#     frac_dict_lc_int = {type_id_dict[k] : fractions[k] for k in fractions}
#     fractions = list(fractions.values())

#     if len(list(set(list(values['DrainageEq'].values())))) < 1:
#         DrainageEq = np.average(list(values['DrainageEq'].values()), weights = fractions)
#         DrainageCoef1 = np.average(list(values['DrainageCoef1'].values()), weights = fractions)
#         DrainageCoef2 =  np.average(list(values['DrainageCoef2'].values()), weights = fractions)

#     else: 
#         DrainageEq = np.average(list(values['DrainageEq'].values()), weights = fractions)
#         DrainageCoef1 = np.average(list(values['DrainageCoef1'].values()), weights = fractions)
#         DrainageCoef2 =  np.average(list(values['DrainageCoef2'].values()), weights = fractions)

    

#     table_dict[surface] = {
#             'Code' : code, # Give new Code
#             'AlbedoMin' :   np.average(list(values['AlbedoMin'].values()), weights = fractions),
#             'AlbedoMax' :   np.average(list(values['AlbedoMax'].values()), weights = fractions),
#             'Emissivity' : np.average(list(values['Emissivity'].values()), weights = fractions),
#             'StorageMin' :  np.average(list(values['StorageMin'].values()), weights = fractions),
#             'StorageMax' : np.average(list(values['StorageMax'].values()), weights = fractions),
#             'WetThreshold' : np.average(list(values['WetThreshold'].values()), weights = fractions),
#             'StateLimit' : np.average(list(values['StateLimit'].values()), weights = fractions),
#             'DrainageEq' : 0,#DrainageEq,
#             'DrainageCoef1' : 0,#DrainageCoef1,
#             'DrainageCoef2' : 0,#DrainageCoef2,
#             'SoilTypeCode' : 50, #not avearageable 
#             'SnowLimPatch' : np.average(list(values['SnowLimPatch'].values()), weights = fractions),
#             'BaseT' :        np.average(list(values['BaseT'].values()), weights = fractions),
#             'BaseTe' :       np.average(list(values['BaseTe'].values()), weights = fractions),
#             'GDDFull' :     np.average(list(values['GDDFull'].values()), weights = fractions),
#             'SDDFull' :      np.average(list(values['SDDFull'].values()), weights = fractions),
#             'LAIMin' :       np.average(list(values['LAIMin'].values()), weights = fractions),
#             'LAIMax' :       np.average(list(values['LAIMax'].values()), weights = fractions),
#             'PorosityMin' :  np.average(list(values['PorosityMin'].values()), weights = fractions),
#             'PorosityMax' :  np.average(list(values['PorosityMax'].values()), weights = fractions),
#             'MaxConductance' :  np.average(list(values['MaxConductance'].values()), weights = fractions),
#             'LAIEq' :        np.average(list(values['LAIEq'].values()), weights = fractions),
#             'LeafGrowthPower1' :  np.average(list(values['LeafGrowthPower1'].values()), weights = fractions),
#             'LeafGrowthPower2' :  np.average(list(values['LeafGrowthPower2'].values()), weights = fractions),
#             'LeafOffPower1' :  np.average(list(values['LeafOffPower1'].values()), weights = fractions),
#             'LeafOffPower2' :  np.average(list(values['LeafOffPower2'].values()), weights = fractions),     
#             # 'OHMCode_SummerWet' : not avearageable 
#             # 'OHMCode_SummerDry' : not avearageable 
#             # 'OHMCode_WinterWet' : not avearageable 
#             # 'OHMCode_WinterDry' : not avearageable 
#             'OHMThresh_SW' : 10,#np.average(list(values['OHMThresh_SW'].values()), weights = fractions), 
#             'OHMThresh_WD' : 0.9,# np.average(list(values['OHMThresh_WD'].values()), weights = fractions), 
#             # 'ESTMCode' : not avearageable 
#             'AnOHM_Cp' : np.average(list(values['AnOHM_Cp'].values()), weights = fractions),
#             'AnOHM_Kk' : np.average(list(values['AnOHM_Kk'].values()), weights = fractions),
#             'AnOHM_Ch' : np.average(list(values['AnOHM_Ch'].values()), weights = fractions),
#             # 'BiogenCO2Code' : not avearageable 

#         }

#     for param in ['OHMCode_SummerWet', 'OHMCode_SummerDry', 'OHMCode_WinterWet' ,'OHMCode_WinterDry', 'ESTMCode', 'BiogenCO2Code']:
#         unique_values = list(set(list(values[param].values())))
#             # print(param, unique_values)
#         if len(unique_values) == 1:
#             table_dict[surface][param] = unique_values[0]
            
#         else:
#             if param == 'ESTMCode':
#                 table_edit, ESTM = new_table_edit(ESTM, table_dict, values, param, 'ESTM', frac_dict_lc_int, surface)
                
#             elif param == 'BiogenCO2Code':
#                 table_edit, BIOCO2 = new_table_edit(BIOCO2, table_dict, values, param, 'BIOGEN', frac_dict_lc_int, surface)
                
#             else: # This is OHM Coefficients
#                 table_edit, OHM = new_table_edit(OHM, table_dict, values, param, 'OHM', frac_dict_lc_int, surface)
                

#         table_dict[surface] = round_dict(table_dict[surface])            
         
#     return table_dict, OHM, ESTM, BIOCO2

def fill_SUEWS_profiles(profiles_list ,save_folder, prof):

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

def blend_nonveg(in_dict, surface, frac_dict_bh, frac_dict_lc, id, db_dict, type_id_dict, frac_to_surf_dict, column_dict):

    table_dict = {}
    values = in_dict[id][surface]
    code_order = list(values.keys())
    fractions = {}

    if surface == 'Buildings':
        frac_dict_int = {}
        for i in frac_dict_bh[id].keys():
            frac_dict_int[type_id_dict[i]] = frac_dict_bh[id][i]
        fractions = list(frac_dict_int.values())
        code = create_code('NonVeg')
    
    else:
        rev_frac_to_surf_dict = dict((v, k) for k, v in frac_to_surf_dict.items())
        surf = rev_frac_to_surf_dict[surface]
        
        fractions = {}
        for typology in frac_dict_lc[id].keys():
            try: 
                fractions[typology] = frac_dict_lc[id][typology][surf]
            except:
                fractions[typology] = 1 / len(list(frac_dict_lc[id].keys())) # How to deal with this?

        # for typology in fractions.keys():
        #     fractions[typology] = fractions[typology] / sum(fractions.values())
        frac_dict_int = {type_id_dict[k] : fractions[k] for k in fractions}
        fractions = list(fractions.values())
        
        code = create_code('NonVeg') #name + str(int(str(int(round(time.time()*1000))))

    
    print('Values:', values)

    table_dict[surface] = {
        'Code' : code, # Give new Code
        'AlbedoMin' :   np.average(list(values['AlbedoMin'].values()), weights = fractions),
        'AlbedoMax' :   np.average(list(values['AlbedoMax'].values()), weights = fractions),
        'Emissivity' : np.average(list(values['Emissivity'].values()), weights = fractions),
        'StorageMin' :  np.average(list(values['StorageMin'].values()), weights = fractions),
        'StorageMax' : np.average(list(values['StorageMax'].values()), weights = fractions),
        'WetThreshold' : np.average(list(values['WetThreshold'].values()), weights = fractions),
        'StateLimit' : np.average(list(values['StateLimit'].values()), weights = fractions),
        'DrainageEq' : np.average(list(values['DrainageEq'].values()), weights = fractions), # NEED FIXING!
        'DrainageCoef1' :np.average(list(values['DrainageCoef1'].values()), weights = fractions),
        'DrainageCoef2' : np.average(list(values['DrainageCoef2'].values()), weights = fractions),
        'SoilTypeCode' : column_dict['SoilTypeCode'],
        'SnowLimPatch' : np.average(list(values['SnowLimPatch'].values()), weights = fractions),
        'SnowLimRemove' : np.average(list(values['SnowLimRemove'].values()), weights = fractions),
        # 'OHMCode_SummerWet' : not avearageable 
        # 'OHMCode_SummerDry' : not avearageable 
        # 'OHMCode_WinterWet' : not avearageable 
        # 'OHMCode_WinterDry' : not avearageable 
        'OHMThresh_SW' : 10,#np.average(list(values['OHMThresh_SW'].values()), weights = fractions), 
        'OHMThresh_WD' : 0.9,#np.average(list(values['OHMThresh_WD'].values()), weights = fractions), 
        # 'ESTMCode' : not avearageable 
        'AnOHM_Cp' : np.average(list(values['AnOHM_Cp'].values()), weights = fractions),
        'AnOHM_Kk' : np.average(list(values['AnOHM_Kk'].values()), weights = fractions),
        'AnOHM_Ch' : np.average(list(values['AnOHM_Ch'].values()), weights = fractions),
    }
    new_edit = pd.DataFrame.from_dict(table_dict[surface],orient = 'index').T.set_index('Code')
    db_dict['NonVeg'] = pd.concat([db_dict['NonVeg'], new_edit])
    
    if surface == 'Water':
        table_dict[surface]['WaterDepth'] = np.average(list(values['WaterDepth'].values()), weights = fractions)
    
    for param in ['OHMCode_SummerWet', 'OHMCode_SummerDry', 'OHMCode_WinterWet' ,'OHMCode_WinterDry']:
        unique_values = list(set(list(values[param].values())))
        
        if len(unique_values) == 1:
            table_dict[surface][param] = unique_values[0]
        else:
            values_list = list(values[param].values())
            frac_majority = list(frac_dict_int.values())
            table_dict[surface][param] = values_list[frac_majority.index(max(frac_majority))]

            # else: # This is OHM Coefficients
            db_dict = new_table_edit(db_dict, table_dict, values, param, 'OHM', frac_dict_int, surface)

        table_dict[surface] = round_dict(table_dict[surface])
        
    return table_dict, db_dict

def save_SUEWS_txt(df_m, table_name, save_folder):
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
    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]

    # add -9 rows to text files
    df_m = df_m.swaplevel(0,1,1)
    # This can probably be done better. Used pd.append() but this will be deprecated. This works, but not the most clean coding
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + table_name, sep = '\t' ,index = False)
    
def save_snow(snow_dict, save_folder):

    df_m = pd.DataFrame.from_dict(snow_dict, orient = 'index').T
    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]
    # add -9 rows to text files
    df_m = df_m.swaplevel(0,1,1)
    # This can probably be done better. Used pd.append() but this will be deprecated. This works, but not the most clean coding
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + 'SUEWS_Snow.txt', sep = '\t' ,index = False)

def save_NonVeg_types(out_dict, save_folder):

    df_m = pd.DataFrame()
    for id in list(out_dict.keys()):
        for surf in ['Paved', 'Buildings','Bare Soil']:
            df_m = pd.concat([df_m, pd.DataFrame.from_dict(out_dict[id][surf], orient='index').T]).drop_duplicates()

    df_m.columns = [df_m.columns, list(range(1, len(df_m.columns)+1))]
    # add -9 rows to text files

    df_m = df_m.swaplevel(0,1,1)
    # This can probably be done better. Used pd.append() but this will be deprecated. This works, but not the most clean coding
    df_m.loc[-1] = np.nan
    df_m.iloc[-1, 0] = -9
    df_m.loc[-2]= np.nan
    df_m.iloc[-1, 0] = -9

    df_m.to_csv(save_folder + 'SUEWS_NonVeg.txt', sep = '\t' ,index = False)

def save_SiteSelect(dict, save_folder, path_to_ss):

    df_m = pd.DataFrame.from_dict(dict).T
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

def presave(table, name ,var_list, save_folder):

    df = table.loc[var_list]
    df = df.drop(columns=df.select_dtypes(include='object').columns).rename_axis('Code')
    save_SUEWS_txt(df, ('SUEWS_' + name + '.txt'), save_folder)


def read_morph_txt(txt_file):
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

