from time import sleep
import pandas as pd
import numpy as np
from osgeo import gdal
from datetime import datetime
from osgeo.gdalconst import *
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd

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
    'LAI': 31,
    'LGP': 32,
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


def create_code(table_name):

    sleep(0.0000000000001) # Slow down to make code unique
    table_code = str(table_id_dict[table_name]) 
    doy = str(datetime.now().timetuple().tm_yday)
    ms = str(datetime.utcnow().strftime('%S%f')) # Year%DOY#Minute#millisecond
    code = int(table_code + doy + ms[3:])
    return code

def round_dict(in_dict):
    in_dict = {k: round(v, 4) for k, v in in_dict.items()}
    return in_dict 

def read_region(db_path):
    reg = pd.read_excel(db_path, sheet_name = 'Lod0_Region', index_col=  'ID', engine = 'openpyxl')
    return reg

def read_DB(db_path):
         
    idx_col = 'ID'
    # Lod 0
    reg = pd.read_excel(db_path, sheet_name = 'Lod0_Region', index_col=  idx_col, engine = 'openpyxl')
    reg.name = 'Lod0_Region'
    country = pd.read_excel(db_path, sheet_name = 'Lod0_Country', index_col=  idx_col, engine = 'openpyxl')
    country.name = 'Lod0_Country'
    # Lod 1
    Type = pd.read_excel(db_path, sheet_name = 'Lod1_Types', index_col=  idx_col, engine = 'openpyxl')
    Type.name = 'Lod1_Types'
    # Lod 2
    veg = pd.read_excel(db_path, sheet_name = 'Lod2_Veg', index_col = idx_col, engine = 'openpyxl')
    veg.name = 'Lod2_Veg'
    nonveg = pd.read_excel(db_path, sheet_name = 'Lod2_NonVeg', index_col = idx_col, engine = 'openpyxl')
    nonveg.name = 'Lod2_NonVeg'
    water = pd.read_excel(db_path, sheet_name = 'Lod2_Water', index_col = idx_col, engine = 'openpyxl')
    water.name = 'Lod2_Water'
    ref = pd.read_excel(db_path, sheet_name = 'References', index_col = idx_col, engine = 'openpyxl')
    ref.name = 'References'
    # Lod 3
    alb =  pd.read_excel(db_path, sheet_name = 'Lod3_Albedo', index_col = idx_col, engine = 'openpyxl')
    alb.name = 'Lod3_Albedo'
    em =  pd.read_excel(db_path, sheet_name = 'Lod3_Emissivity', index_col = idx_col, engine = 'openpyxl')
    em.name = 'Lod3_Emissivity'
    OHM =  pd.read_excel(db_path, sheet_name = 'Lod3_OHM', index_col = idx_col, engine = 'openpyxl') # Away from Veg
    OHM.name = 'Lod3_OHM'
    LAI =  pd.read_excel(db_path, sheet_name = 'Lod3_LAI', index_col = idx_col, engine = 'openpyxl')
    LAI.name = 'Lod3_LAI'
    st = pd.read_excel(db_path, sheet_name = 'Lod3_Storage', index_col = idx_col, engine = 'openpyxl')
    st.name = 'Lod3_Storage'
    cnd = pd.read_excel(db_path, sheet_name = 'Lod3_Conductance', index_col = idx_col, engine = 'openpyxl') # Away from Veg
    cnd.name = 'Lod3_Conductance'
    LGP = pd.read_excel(db_path, sheet_name = 'Lod3_LGP', index_col = idx_col, engine = 'openpyxl')
    LGP.name = 'Lod3_LGP'
    dr = pd.read_excel(db_path, sheet_name = 'Lod3_Drainage', index_col = idx_col, engine = 'openpyxl')
    dr.name = 'Lod3_Drainage'
    VG = pd.read_excel(db_path, sheet_name = 'Lod3_VegetationGrowth', index_col = idx_col, engine = 'openpyxl')
    VG.name = 'Lod3_VegetationGrowth'
    ANOHM = pd.read_excel(db_path, sheet_name = 'Lod3_ANOHM', index_col = idx_col, engine = 'openpyxl')
    ANOHM.name = 'Lod3_ANOHM'
    BIOCO2 = pd.read_excel(db_path, sheet_name = 'Lod3_BiogenCO2',index_col = idx_col, engine = 'openpyxl')
    BIOCO2.name = 'Lod3_BiogenCO2'
    MVCND = pd.read_excel(db_path, sheet_name= 'Lod3_MaxVegetationConductance', index_col = idx_col, engine = 'openpyxl')
    MVCND.name = 'Lod3_MaxVegetationConductance'
    por = pd.read_excel(db_path, sheet_name = 'Lod3_Porosity', index_col = idx_col, engine = 'openpyxl')
    por.name = 'Lod3_Porosity'
    snow = pd.read_excel(db_path, sheet_name = 'Lod3_Snow', index_col = idx_col, engine = 'openpyxl')
    snow.name = 'Lod3_Snow'
    AnEm = pd.read_excel(db_path, sheet_name = 'Lod3_AnthropogenicEmission',index_col = idx_col, engine = 'openpyxl')
    AnEm.name = 'Lod3_AnthropogenicEmission'
    prof = pd.read_excel(db_path, sheet_name= 'Lod3_Profiles', index_col = idx_col, engine = 'openpyxl')
    prof.name = 'Lod3_Profiles'
    ws = pd.read_excel(db_path, sheet_name = 'Lod3_WaterState', index_col = idx_col, engine = 'openpyxl')
    ws.name = 'Lod3_WaterState'
    soil = pd.read_excel(db_path, sheet_name = 'Lod3_Soil', index_col = idx_col, engine = 'openpyxl')
    soil.name = 'Lod3_Soil'
    ESTM = pd.read_excel(db_path, sheet_name = 'Lod3_ESTM', index_col = idx_col, engine = 'openpyxl')
    ESTM.name = 'Lod3_ESTM'
    irr = pd.read_excel(db_path, sheet_name= 'Lod3_Irrigation', index_col = idx_col, engine = 'openpyxl')
    irr.name = 'Lod3_Irrigation'

    type_id_dict = {val: key for key, val in pd.Series((Type['Type'] + ', ' + Type['Origin'] ),index=Type.reset_index()['ID']).to_dict().items()}
    country_id_dict = {val: key for key, val in pd.Series((country['Region'] + ', ' + country['Country'] + ', ' + country['City'] ),index=country.reset_index()['ID']).to_dict().items()}
    reg_id_dict = {val: key for key, val in pd.Series((reg['Region']),index=reg.reset_index()['ID']).to_dict().items()}


    return Type, veg, nonveg, water, ref, alb, em, OHM, LAI, st, cnd, LGP, dr, VG, ANOHM, BIOCO2, MVCND, por, reg, snow, AnEm, prof, ws, soil, ESTM, irr, country, type_id_dict,country_id_dict, reg_id_dict

def decide_country_or_region(col, country_sel, reg):
    if str(country_sel[col].item()) == 'nan':
        var = reg.loc[reg['Region'] == country_sel['Region'].item(), col].item()
    else:
        var = country_sel[col].item()    
    return var

def fill_SUEWS_NonVeg(Type, table, alb, em, st, dr, ANOHM, ws, column_dict, urbType = False,):
    
    table_dict = {}
    surf_list = ['Paved', 'Buildings', 'Bare Soil']
    if urbType != False:
        for i in surf_list:
            table_dict[i] = {}
            for j in urbType:
                if i == 'Bare Soil':
                    locator = column_dict['Bare Soil']
                else:
                    locator = Type.loc[j, i]

                table_dict[i][j] = {
                'Code' : locator,
                'AlbedoMin' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_min'],
                'AlbedoMax' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_max'],
                'Emissivity' : em.loc[table.loc[locator, 'Em'], 'Emissivity'],
                'StorageMin' :  st.loc[table.loc[locator, 'St'], 'StorageMin'],
                'StorageMax' : st.loc[table.loc[locator, 'St'], 'StorageMax'],
                'WetThreshold' : dr.loc[table.loc[locator, 'Dr'], 'WetThreshold'],
                'StateLimit' : ws.loc[table.loc[locator, 'Ws'], 'StateLimit'],
                'DrainageEq' : dr.loc[table.loc[locator, 'Dr'], 'DrainageEq'],
                'DrainageCoef1' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef1'],
                'DrainageCoef2' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef2'],
                'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
                'SnowLimPatch' : 190,
                'SnowLimRemove': 90,    
                'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
                'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
                'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
                'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
                'OHMThresh_SW' : 10, # table.loc[locator, 'OHMThresh_SW'],
                'OHMThresh_WD' : 0.9, #table.loc[locator, 'OHMThresh_WD'],
                'ESTMCode' : table.loc[locator, 'ESTM'],
                'AnOHM_Cp' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                'AnOHM_Kk' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                'AnOHM_Ch' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
            }
                
    elif urbType == False:
        for i in surf_list:
            table_dict[i] = {}

            locator = column_dict[i]
            table_dict[i] = {
                'Code' : locator,
                'AlbedoMin' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_min'],
                'AlbedoMax' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_max'],
                'Emissivity' : em.loc[table.loc[locator, 'Em'], 'Emissivity'],
                'StorageMin' :  st.loc[table.loc[locator, 'St'], 'StorageMin'],
                'StorageMax' : st.loc[table.loc[locator, 'St'], 'StorageMax'],
                'WetThreshold' : dr.loc[table.loc[locator, 'Dr'], 'WetThreshold'],
                'StateLimit' : ws.loc[table.loc[locator, 'Ws'], 'StateLimit'],
                'DrainageEq' : dr.loc[table.loc[locator, 'Dr'], 'DrainageEq'],
                'DrainageCoef1' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef1'],
                'DrainageCoef2' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef2'],
                'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
                'SnowLimPatch' : 190,
                'SnowLimRemove': 90,    
                'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
                'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
                'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
                'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
                'OHMThresh_SW' : 10, # table.loc[locator, 'OHMThresh_SW'],
                'OHMThresh_WD' : 0.9, #table.loc[locator, 'OHMThresh_WD'],
                'ESTMCode' : table.loc[locator, 'ESTM'],
                'AnOHM_Cp' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                'AnOHM_Kk' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                'AnOHM_Ch' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
            }
                    
    return table_dict

def fill_SUEWS_Water(locator, table, alb, em, st, dr, ANOHM, ws, column_dict):

    table_dict = {}

    table_dict['Water'] = {
        'Code' : locator,
        'AlbedoMin' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_min'],
        'AlbedoMax' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_max'],
        'Emissivity' : em.loc[table.loc[locator, 'Em'], 'Emissivity'],
        'StorageMin' :  st.loc[table.loc[locator, 'St'], 'StorageMin'],
        'StorageMax' : st.loc[table.loc[locator, 'St'], 'StorageMax'],
        'WetThreshold' : dr.loc[table.loc[locator, 'Dr'], 'WetThreshold'],
        'StateLimit' : ws.loc[table.loc[locator, 'Ws'], 'StateLimit'],
        'WaterDepth' : ws.loc[table.loc[locator, 'Ws'], 'WaterDepth'],
        'DrainageEq' : dr.loc[table.loc[locator, 'Dr'], 'DrainageEq'],
        'DrainageCoef1' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef1'],
        'DrainageCoef2' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef2'],
        'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
        'SnowLimPatch' : -9999,
        'SnowLimRemove': -9999,    
        'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
        'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
        'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
        'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
        'OHMThresh_SW' : 10, # table.loc[locator, 'OHMThresh_SW'],
        'OHMThresh_WD' : 0.9, #table.loc[locator, 'OHMThresh_WD'],
        'ESTMCode' : table.loc[locator, 'ESTM'],
        'AnOHM_Cp' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
        'AnOHM_Kk' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
        'AnOHM_Ch' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
    }
    
    return table_dict

def fill_SUEWS_Veg(Type, veg, alb, em, LAI, st, LGP, dr, VG, ANOHM,  MVCND, por, ws, column_dict , urbType = False):

    table = veg
    table_dict = {}
    
    if urbType != False:
        for i in ['Evergreen Tree', 'Decidous Tree', 'Grass']:
            table_dict[i] = {}
            for j in urbType:

                locator = Type.loc[j, i]
                table_dict[i][j] = {
                    'Code' : locator,
                    'AlbedoMin' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_min'],
                    'AlbedoMax' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_max'],
                    'Emissivity' : em.loc[table.loc[locator, 'Em'], 'Emissivity'],
                    'StorageMin' :  st.loc[table.loc[locator, 'St'], 'StorageMin'],
                    'StorageMax' : st.loc[table.loc[locator, 'St'], 'StorageMax'],
                    'WetThreshold' : dr.loc[table.loc[locator, 'Dr'], 'WetThreshold'],
                    'StateLimit' : ws.loc[table.loc[locator, 'Ws'], 'StateLimit'],
                    'DrainageEq' : dr.loc[table.loc[locator, 'Dr'], 'DrainageEq'],
                    'DrainageCoef1' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef1'],
                    'DrainageCoef2' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef2'],
                    'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
                    'SnowLimPatch' : 190,
                    'BaseT' :       VG.loc[table.loc[locator, 'VG'], 'BaseT'],
                    'BaseTe' :      VG.loc[table.loc[locator, 'VG'], 'BaseTe'],
                    'GDDFull' :     VG.loc[table.loc[locator, 'VG'], 'GDDFull'],
                    'SDDFull' :     VG.loc[table.loc[locator, 'VG'], 'SDDFull'],
                    'LAIMin' :      LAI.loc[table.loc[locator, 'LAI'], 'LAIMin'],
                    'LAIMax' :      LAI.loc[table.loc[locator, 'LAI'], 'LAIMax'],
                    'PorosityMin' : por.loc[table.loc[locator, 'Por'], 'PorosityMin'],
                    'PorosityMax' : por.loc[table.loc[locator, 'Por'], 'PorosityMax'],
                    'MaxConductance' : MVCND.loc[table.loc[locator, 'MVCND'], 'MaxConductance'],
                    'LAIEq' :       LAI.loc[table.loc[locator, 'LAI'], 'LAIEq'],
                    'LeafGrowthPower1' : LGP.loc[table.loc[locator, 'LGP'], 'LeafGrowthPower1'],
                    'LeafGrowthPower2' : LGP.loc[table.loc[locator, 'LGP'], 'LeafGrowthPower2'],
                    'LeafOffPower1' : LGP.loc[table.loc[locator, 'LGP'], 'LeafOffPower1'],
                    'LeafOffPower2' : LGP.loc[table.loc[locator, 'LGP'], 'LeafOffPower2'],    
                    'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
                    'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
                    'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
                    'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
                    'OHMThresh_SW' : 10,#table.loc[locator, 'OHMThresh_SW'],
                    'OHMThresh_WD' : 0.9,#table.loc[locator, 'OHMThresh_WD'],
                    'ESTMCode' : table.loc[locator, 'ESTM'],
                    'AnOHM_Cp' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                    'AnOHM_Kk' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                    'AnOHM_Ch' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
                    'BiogenCO2Code' : column_dict['Biogen']
                }


    elif urbType == False:
        for i in ['Evergreen Tree', 'Decidous Tree', 'Grass']:
            table_dict[i] = {}

            locator = column_dict[i]
            table_dict[i] = {
                    'Code' : locator,
                    'AlbedoMin' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_min'],
                    'AlbedoMax' :   alb.loc[table.loc[locator, 'Alb'], 'Alb_max'],
                    'Emissivity' : em.loc[table.loc[locator, 'Em'], 'Emissivity'],
                    'StorageMin' :  st.loc[table.loc[locator, 'St'], 'StorageMin'],
                    'StorageMax' : st.loc[table.loc[locator, 'St'], 'StorageMax'],
                    'WetThreshold' : dr.loc[table.loc[locator, 'Dr'], 'WetThreshold'],
                    'StateLimit' : ws.loc[table.loc[locator, 'Ws'], 'StateLimit'],
                    'DrainageEq' : dr.loc[table.loc[locator, 'Dr'], 'DrainageEq'],
                    'DrainageCoef1' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef1'],
                    'DrainageCoef2' : dr.loc[table.loc[locator, 'Dr'], 'DrainageCoef2'],
                    'SoilTypeCode' : column_dict['SoilTypeCode'], #table.loc[locator, 'SoilTypeCode'],  36),
                    'SnowLimPatch' : 190,
                    'BaseT' :       VG.loc[column_dict['Vegetation Growth'], 'BaseT'], #VG.loc[table.loc[locator, 'VG'], 'BaseT'],
                    'BaseTe' :      VG.loc[column_dict['Vegetation Growth'], 'BaseTe'],#VG.loc[table.loc[locator, 'VG'], 'BaseTe'],
                    'GDDFull' :     VG.loc[column_dict['Vegetation Growth'], 'GDDFull'],#VG.loc[table.loc[locator, 'VG'], 'GDDFull'],
                    'SDDFull' :     VG.loc[column_dict['Vegetation Growth'], 'SDDFull'],#VG.loc[table.loc[locator, 'VG'], 'SDDFull'],
                    'LAIMin' :      LAI.loc[table.loc[locator, 'LAI'], 'LAIMin'],
                    'LAIMax' :      LAI.loc[table.loc[locator, 'LAI'], 'LAIMax'],
                    'PorosityMin' : por.loc[table.loc[locator, 'Por'], 'PorosityMin'],
                    'PorosityMax' : por.loc[table.loc[locator, 'Por'], 'PorosityMax'],
                    'MaxConductance' : MVCND.loc[table.loc[locator, 'MVCND'], 'MaxConductance'],
                    'LAIEq' :       LAI.loc[table.loc[locator, 'LAI'], 'LAIEq'],
                    'LeafGrowthPower1' : LGP.loc[table.loc[locator, 'LGP'], 'LeafGrowthPower1'],
                    'LeafGrowthPower2' : LGP.loc[table.loc[locator, 'LGP'], 'LeafGrowthPower2'],
                    'LeafOffPower1' : LGP.loc[table.loc[locator, 'LGP'], 'LeafOffPower1'],
                    'LeafOffPower2' : LGP.loc[table.loc[locator, 'LGP'], 'LeafOffPower2'],    
                    'OHMCode_SummerWet' : table.loc[locator, 'OHMSummerWet'],
                    'OHMCode_SummerDry' : table.loc[locator, 'OHMSummerDry'],
                    'OHMCode_WinterWet' : table.loc[locator, 'OHMWinterWet'],
                    'OHMCode_WinterDry' : table.loc[locator, 'OHMWinterDry'],
                    'OHMThresh_SW' : 10,#table.loc[locator, 'OHMThresh_SW'],
                    'OHMThresh_WD' : 0.9,#table.loc[locator, 'OHMThresh_WD'],
                    'ESTMCode' : table.loc[locator, 'ESTM'],
                    'AnOHM_Cp' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
                    'AnOHM_Kk' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
                    'AnOHM_Ch' : ANOHM.loc[table.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
                    'BiogenCO2Code' : column_dict['Biogen'] #table.loc[locator, 'BIOGEN']
                }
    return table_dict

def fill_SUEWS_Snow(snow_sel, snow, alb, em, ANOHM):
    
    locator = snow_sel

    table_dict = {
        'Code' : locator,
        'RadMeltFactor' : snow.loc[locator, 'RadMeltFactor'], 
        'TempMeltFactor': snow.loc[locator, 'TempMeltFactor'],
        'AlbedoMin' : alb.loc[snow.loc[locator, 'Alb'], 'Alb_min'],
        'AlbedoMax' : alb.loc[snow.loc[locator, 'Alb'], 'Alb_max'],
        'Emissivity' : em.loc[snow.loc[locator, 'Em'], 'Emissivity'],
        'tau_a' : snow.loc[locator, 'tau_a'],
        'tau_f' : snow.loc[locator, 'tau_f'],
        'PrecipLimAlb' : snow.loc[locator, 'PrecipLimAlb'],
        'SnowDensMin' : snow.loc[locator, 'SnowDensMin'],
        'SnowDensMax' : snow.loc[locator, 'SnowDensMax'],
        'tau_r' : snow.loc[locator, 'tau_r'], 
        'CRWMin' : snow.loc[locator, 'CRWMin'],
        'CRWMax' : snow.loc[locator, 'CRWMax'],
        'PrecipLimSnow' : snow.loc[locator, 'PrecipLimSnow'],
        'OHMCode_SummerWet' : snow.loc[locator, 'OHMCode_SummerWet'],
        'OHMCode_SummerDry' : snow.loc[locator, 'OHMCode_SummerDry'],
        'OHMCode_WinterWet' : snow.loc[locator, 'OHMCode_WinterWet'],
        'OHMCode_WinterDry' : snow.loc[locator, 'OHMCode_WinterDry'],
        'OHMThresh_SW' : 10,
        'OHMThresh_WD' : 0.9,
        'ESTMCode' : snow.loc[locator, 'ESTM'],
        'AnOHM_Cp' : ANOHM.loc[snow.loc[locator, 'ANOHM'],  'AnOHM_Cp'],
        'AnOHM_Kk' : ANOHM.loc[snow.loc[locator, 'ANOHM'],  'AnOHM_Kk'],
        'AnOHM_Ch' : ANOHM.loc[snow.loc[locator, 'ANOHM'],  'AnOHM_Ch'],
    }

    return table_dict

def fill_SUEWS_AnthropogenicEmission(AnthropogenicCode, reg_sel, table):

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
        'EnergyUseProfWD' : reg_sel['EnergyUseProfWD'], #.item() ,
        'EnergyUseProfWE' : reg_sel['EnergyUseProfWE'], #.item() ,
        'ActivityProfWD' : reg_sel['ActivityProfWD'], #.item(),
        'ActivityProfWE' : reg_sel['ActivityProfWE'], #.item(),
        'TraffProfWD' : reg_sel['TrafficRate_WD'], #.item(),
        'TraffProfWE' : reg_sel['TrafficRate_WD'], #.item(),
        'PopProfWD' : reg_sel['PopProfWD'], #.item(),
        'PopProfWE' : reg_sel['PopProfWE'], #.item() ,
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

def new_table_edit(table, table_dict, values, param, name, frac_dict, surface):

    index = list(table.reset_index()['ID'])
    index = [str(i) for i in index]
    edit_index_dict = {k: k for k in index}
    edit_dict = {} 
    weights_dict = {}
    weights_dict = {k : 0 for k in values['Code'].keys()}

    for i in list(values['Code'].keys()):
        weights_dict[i] ={}
        weights_dict[i]['Code'] = values[param][i]
        weights_dict[i]['Weight'] = 0

    for i in list(values['Code'].keys()):
        weights_dict[i]['Weight'] =  + frac_dict[i]

    weight_dict_merged ={}
    weight_dict_merged['Code'] = {}
    for i in weights_dict:
        weight_dict_merged['Code'][weights_dict[i]['Code']] = 0

    for i in weights_dict:
        code = weights_dict[i]['Code'] 
        weight = weights_dict[i]['Weight']
        weight_dict_merged['Code'][code] = weight_dict_merged['Code'][code] + weight

    for type in list(values[param].values()):
        edit_dict[type] = table.loc[edit_index_dict[type]]

    blend_edit_dict = {}
    edit_params = (list(edit_dict[type].keys()))

    for p in edit_params:
        blend_edit_dict[p] = {}
    
        for edit_code in list(edit_dict.keys()):
            blend_edit_dict[p][edit_code] = edit_dict[edit_code][p]
    new_edit_dict ={}

    new_code = create_code(name) #name + str(str(round(time.time()*1000))))

    new_edit_dict = {'Code' : new_code}
    weight = list(list(weight_dict_merged['Code'].values()))
    
    for i in edit_params:
        try:
            new_edit_dict[i] = np.average(list(blend_edit_dict[i].values()), weights = weight)
        except:
            new_edit_dict[i] = -999.

    table_dict[surface] = round_dict(table_dict[surface])            

    table_dict[surface][param]  = new_edit_dict['Code']
    new_edit_dict['Code'] = new_code
    dict_df = pd.DataFrame(new_edit_dict, index = [0]).set_index('Code')
    dict_df = dict_df.rename_axis('ID')
    table = pd.concat([table, dict_df])

    return edit_dict, table

# Not used at the moment. Pehaps in future. Beware of many equation choices that cannot be averaged
def blend_veg(in_dict, surface, frac_dict_lc, id, ESTM, OHM, BIOCO2, type_id_dict, frac_to_surf_dict):

    table_dict = {}
    values = in_dict[id][surface]
    code_order = list(values.keys())
    rev_frac_to_surf_dict = dict((v, k) for k, v in frac_to_surf_dict.items())
    surf = rev_frac_to_surf_dict[surface]
    

    fractions = {}
    for typology in frac_dict_lc[id].keys():
        try: 
            fractions[typology] = frac_dict_lc[id][typology][surf]
        except:
            fractions[typology] = 1 / len(list(frac_dict_lc[id].keys())) # How to deal with this?

    code = create_code('Veg')

    frac_dict_lc_int = {type_id_dict[k] : fractions[k] for k in fractions}
    fractions = list(fractions.values())

    if len(list(set(list(values['DrainageEq'].values())))) < 1:
        DrainageEq = np.average(list(values['DrainageEq'].values()), weights = fractions)
        DrainageCoef1 = np.average(list(values['DrainageCoef1'].values()), weights = fractions)
        DrainageCoef2 =  np.average(list(values['DrainageCoef2'].values()), weights = fractions)

    else: 
        DrainageEq = np.average(list(values['DrainageEq'].values()), weights = fractions)
        DrainageCoef1 = np.average(list(values['DrainageCoef1'].values()), weights = fractions)
        DrainageCoef2 =  np.average(list(values['DrainageCoef2'].values()), weights = fractions)

    

    table_dict[surface] = {
            'Code' : code, # Give new Code
            'AlbedoMin' :   np.average(list(values['AlbedoMin'].values()), weights = fractions),
            'AlbedoMax' :   np.average(list(values['AlbedoMax'].values()), weights = fractions),
            'Emissivity' : np.average(list(values['Emissivity'].values()), weights = fractions),
            'StorageMin' :  np.average(list(values['StorageMin'].values()), weights = fractions),
            'StorageMax' : np.average(list(values['StorageMax'].values()), weights = fractions),
            'WetThreshold' : np.average(list(values['WetThreshold'].values()), weights = fractions),
            'StateLimit' : np.average(list(values['StateLimit'].values()), weights = fractions),
            'DrainageEq' : 0,#DrainageEq,
            'DrainageCoef1' : 0,#DrainageCoef1,
            'DrainageCoef2' : 0,#DrainageCoef2,
            'SoilTypeCode' : 50, #not avearageable 
            'SnowLimPatch' : np.average(list(values['SnowLimPatch'].values()), weights = fractions),
            'BaseT' :        np.average(list(values['BaseT'].values()), weights = fractions),
            'BaseTe' :       np.average(list(values['BaseTe'].values()), weights = fractions),
            'GDDFull' :     np.average(list(values['GDDFull'].values()), weights = fractions),
            'SDDFull' :      np.average(list(values['SDDFull'].values()), weights = fractions),
            'LAIMin' :       np.average(list(values['LAIMin'].values()), weights = fractions),
            'LAIMax' :       np.average(list(values['LAIMax'].values()), weights = fractions),
            'PorosityMin' :  np.average(list(values['PorosityMin'].values()), weights = fractions),
            'PorosityMax' :  np.average(list(values['PorosityMax'].values()), weights = fractions),
            'MaxConductance' :  np.average(list(values['MaxConductance'].values()), weights = fractions),
            'LAIEq' :        np.average(list(values['LAIEq'].values()), weights = fractions),
            'LeafGrowthPower1' :  np.average(list(values['LeafGrowthPower1'].values()), weights = fractions),
            'LeafGrowthPower2' :  np.average(list(values['LeafGrowthPower2'].values()), weights = fractions),
            'LeafOffPower1' :  np.average(list(values['LeafOffPower1'].values()), weights = fractions),
            'LeafOffPower2' :  np.average(list(values['LeafOffPower2'].values()), weights = fractions),     
            # 'OHMCode_SummerWet' : not avearageable 
            # 'OHMCode_SummerDry' : not avearageable 
            # 'OHMCode_WinterWet' : not avearageable 
            # 'OHMCode_WinterDry' : not avearageable 
            'OHMThresh_SW' : 10,#np.average(list(values['OHMThresh_SW'].values()), weights = fractions), 
            'OHMThresh_WD' : 0.9,# np.average(list(values['OHMThresh_WD'].values()), weights = fractions), 
            # 'ESTMCode' : not avearageable 
            'AnOHM_Cp' : np.average(list(values['AnOHM_Cp'].values()), weights = fractions),
            'AnOHM_Kk' : np.average(list(values['AnOHM_Kk'].values()), weights = fractions),
            'AnOHM_Ch' : np.average(list(values['AnOHM_Ch'].values()), weights = fractions),
            # 'BiogenCO2Code' : not avearageable 

        }

    for param in ['OHMCode_SummerWet', 'OHMCode_SummerDry', 'OHMCode_WinterWet' ,'OHMCode_WinterDry', 'ESTMCode', 'BiogenCO2Code']:
        unique_values = list(set(list(values[param].values())))
            # print(param, unique_values)
        if len(unique_values) == 1:
            table_dict[surface][param] = unique_values[0]
            
        else:
            if param == 'ESTMCode':
                table_edit, ESTM = new_table_edit(ESTM, table_dict, values, param, 'ESTM', frac_dict_lc_int, surface)
                
            elif param == 'BiogenCO2Code':
                table_edit, BIOCO2 = new_table_edit(BIOCO2, table_dict, values, param, 'BIOGEN', frac_dict_lc_int, surface)
                
            else: # This is OHM Coefficients
                table_edit, OHM = new_table_edit(OHM, table_dict, values, param, 'OHM', frac_dict_lc_int, surface)
                

        table_dict[surface] = round_dict(table_dict[surface])            
         
    return table_dict, OHM, ESTM, BIOCO2

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


def blend_nonveg(in_dict, surface, frac_dict_bh, frac_dict_lc, id, ESTM, OHM, type_id_dict, frac_to_surf_dict, column_dict):

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

    if surface == 'Water':
        table_dict[surface]['WaterDepth'] = np.average(list(values['WaterDepth'].values()), weights = fractions)
    
    for param in ['OHMCode_SummerWet', 'OHMCode_SummerDry', 'OHMCode_WinterWet' ,'OHMCode_WinterDry', 'ESTMCode']:
        unique_values = list(set(list(values[param].values())))
        
        if len(unique_values) == 1:
            table_dict[surface][param] = unique_values[0]
        else:
            values_list = list(values[param].values())
            frac_majority = list(frac_dict_int.values())
            # frac frac_majority_idx  = 
            table_dict[surface][param] = values_list[frac_majority.index(max(frac_majority))]

            # if param == 'ESTMCode':
            #     table_edit, ESTM = new_table_edit(ESTM, table_dict, values, param, 'ESTM', frac_dict_int, surface)
                
            # else: # This is OHM Coefficients
            #     table_edit, OHM = new_table_edit(OHM, table_dict, values, param, 'OHM', frac_dict_int, surface)

        table_dict[surface] = round_dict(table_dict[surface])            

    return table_dict, OHM, ESTM

def save_SUEWS_txt(df_m, table_name, save_folder):
    col = ['General Type', 'Surface', 'Description', 'Origin', 'Ref', 'Season', 'Day' ,'Profile Type']
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

def presave(table, name, var_list, save_folder):

    df = table.copy()
    index_list = []
    [index_list.append(i) for i in table.index]
    list_str = [x for x in index_list if isinstance(x, str)]

    for i in list_str:
        df.rename(index={i : i},inplace=True)

    df = df.drop(columns=df.select_dtypes(include='object').columns)

    save_SUEWS_txt(df.loc[var_list].rename_axis('Code'), ('SUEWS_' + name + '.txt'), save_folder)

def sel_to_dict(table, var, column_dict):

    code = column_dict[var]
    out_dict = table.loc[code].to_dict()
    out_dict['Code'] = code
    return out_dict

def read_morph_txt(txt_file):
    morph_dict = pd.read_csv(txt_file, delim_whitespace=True, index_col=[0]).to_dict(orient='index')
    return morph_dict  

def get_utc(grid_path, timezone):
    
    # timezone
    grid = gpd.read_file(grid_path)
    
    # Set of grid to crs of timezones vectorlayer (WGS 84)
    grid_crs = grid.to_crs(timezone.crs)

    try:
        spatial_join = gpd.sjoin(left_df=grid_crs,
                                    right_df=timezone,
                                    how="inner", op='within')
        utc = spatial_join.iloc[0]['zone']
    except:
        utc = 0
        print('UTC calc not working')
    return utc

# Not used now, pehaps later .
def read_LUCY_txt(df):
    df['ID'] = np.arange(df.shape[0])+1
    df.set_index('ID', inplace = True)


def load_lucy(LUCY_path, region):

    hours = list(range(0,24))
    hours = [str(x) for x in hours]
    weekdays = ['monday', 'tuesday', 'wednesday','thursday','friday', 'saturday','sunday']
    days =  list(range(1,366))
    days = [str(x) for x in days]

    lucy_suews_dict = {
        75 : 1,
        125 : 1.5,
        175 : 2 
    }

    # Read txtfiles 
    id = pd.read_csv(LUCY_path + 'CountryID.txt', sep = ';', names = ['ID', 'Name', 'ID2'], index_col= 'ID') 
    weekdayhours = pd.read_csv(LUCY_path + 'weekdayhours.txt', sep = '\t', names = hours)
    weekenddays = pd.read_csv(LUCY_path + 'weekenddays.txt', sep = '\t', names = weekdays)
    weekendhours = pd.read_csv(LUCY_path + 'weekendhours.txt', sep = '\t', names = hours)
    metabolism = pd.read_csv(LUCY_path + 'Metabolism.txt', sep = '\t', names = hours)
    metabolism = metabolism.replace(lucy_suews_dict)
    summercooling = pd.read_csv(LUCY_path + 'CountriesSummerCooling.txt',sep = '\t')
    holidays = pd.read_csv(LUCY_path + 'fixedpublicholidays.txt', sep = '\t', names = days)
    ecstatus = pd.read_csv(LUCY_path + 'CountriesEconomicStatus.txt', sep = '\t')

    # Fix-ID
    read_LUCY_txt(weekdayhours)
    read_LUCY_txt(weekendhours)
    read_LUCY_txt(weekenddays)
    read_LUCY_txt(metabolism)
    read_LUCY_txt(summercooling)
    read_LUCY_txt(holidays)
    read_LUCY_txt(ecstatus)
    
    # Drop all Nan
    ecstatus['ecStatus'] = ecstatus.sum(axis = 1)
    ecstatus = ecstatus[['Country', 'ecStatus']]

    indexer = (id.loc[id['Name'] == region]).index.item()
    weekenddays = weekenddays.loc[indexer]
    weekdayhours = weekdayhours.loc[indexer]
    weekendhours = weekendhours.loc[indexer]
    weekenddays = weekenddays.loc[indexer]
    hum_activity = metabolism.loc[indexer]
    summercooling = summercooling.loc[indexer]
    holidays = holidays.loc[indexer]
    ecstatus = ecstatus.loc[indexer]
    Buildings_energy_use = weekendhours.loc[indexer]

    QF_wd = weekdayhours.loc[indexer]
    QF_We = weekendhours.loc[indexer]
    Tr_wd = weekdayhours.loc[indexer]
    Tr_we = weekendhours.loc[indexer]
    HA_wd = metabolism.loc[indexer]
    HA_we = metabolism.loc[indexer]

    # return QF_wd, QF_We, Tr_wd , Tr_we, HA_wd, HA_we

