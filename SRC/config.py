#!/usr/bin/env python
"""
WALOUS_UTS - Copyright (C) <2020> <Service Public de Wallonie (SWP), Belgique,
					          		Institut Scientifique de Service Public (ISSeP), Belgique,
									Université catholique de Louvain (UCLouvain), Belgique,
									Université Libre de Bruxelles (ULB), Belgique>
						 							
	
List of the contributors to the development of WALOUS_UTS: see LICENSE file.
Description and complete License: see LICENSE file.
	
This program (WALOUS_UTS) is free software:
you can redistribute it and/or modify it under the terms of the
GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program (see COPYING file).  If not,
see <http://www.gnu.org/licenses/>.
"""

"""
This script is used to store all configuration parameter. !!!!! Please adapt all paths and parameters according to your configuration. !!!!!
"""

import os

# Initialize dictionnaries
config_parameters = {}
data = {}

############## GRASS GIS ##############
## Please update the following paths according to your own configuration
config_parameters['GISBASE'] = '/usr/lib/grass78'
#config_parameters['PYTHONLIB'] = '/usr/lib/python2.7'  # If you have trouble with Python, try to uncomment this line and comment the next one
config_parameters['PYTHONLIB'] = '/usr/bin/python2'
config_parameters['locationepsg'] = '31370' #  EPSG code for Belgian Lambert 1972
## The following parameters should not be changed normally
config_parameters['gisdb'] = '../../GRASSDATA' # path to GRASSDATA folder
config_parameters['permanent_mapset'] = 'PERMANENT' # name of the permanent mapset
config_parameters['location'] = "WALOUS_31370"

############## POSTGRESQL DATABASE ##############
config_parameters['pg_host'] = 'XXXXXXXput_server_IP_hereXXXXXXX' # Postgresqgl host
config_parameters['pg_port'] = 'XXXXXXXput_server__hereXXXXXXX' # DB port
config_parameters['pg_user'] = 'XXXXXXXput_your_username_hereXXXXXXX' # Username
config_parameters['pg_password'] = 'XXXXXXXput_your_password_hereXXXXXXX' # Password
config_parameters['pg_dbname'] = 'walous' # Database

############## OTHER PARAMETERS ##############
config_parameters['njobs'] = 6
config_parameters['outputfolder'] = '../../../LU_Results'
config_parameters['validationfolder'] = '../../../LU_validation'

############## CADASTRAL DATA ##############
# Link to files
data['capa'] = ('capa','../../../../Landuse/Data/Plan_cadastral/01000_Belgium_PP-FiscSit_Lb2008_01012019/Bpn_CaPa_wallonie.shp') # Cadastral parcels in shapefile (polygon)
data['cama'] = ('cama','../../../../Landuse/Data/Plan_cadastral/DDDI000031.csv') # Cadastral matrix (csv)
# List containing column type for the .csv file ''
data['cama_type'] = [('propertySituationIdf','integer PRIMARY KEY'),('divCad','integer'),('section','varchar'),('primaryNumber','integer'),
					 ('bisNumber','integer'),('exponentLetter','varchar'),('exponentNumber','integer'),('partNumber','varchar'),('capakey','varchar'),('nature','integer'),('natureDescription','varchar'),
					 ('constructionIndication','integer'),('constructionType','varchar'), ('floorNumberAboveground','integer'),('garret','integer'),('constructionYear','integer'),('physModYear','integer'),
					 ('constructionQuality','varchar'),('garageNumber','integer'),('centralHeating','integer'),('bathroomNumber','integer'),('housingUnitNumber','integer'),('placeNumber','integer'),
					 ('builtSurface','integer'),('usedSurface','integer'),('dateSituation','varchar')]
data['lc_capa'] = ('lc_capa',os.path.join(config_parameters['outputfolder'],'lc_capa','LCPropByCaPA.csv')) # .csv with the export of r.zonal.classes for proportion of LC classes in CaPa
data['lc_uncad'] = ('lc_uncad',os.path.join(config_parameters['outputfolder'],'lc_uncad','LCPropByUncad.csv')) # .csv with the export of r.zonal.classes for proportion of LC classes in uncadastred geometries
data['lc_uncad_tiny'] = ('lc_uncad_tiny',os.path.join(config_parameters['outputfolder'],'lc_uncad','LCPropByUncad_tiny.csv')) # .csv with the export of r.zonal.classes for proportion of LC classes in uncadastred geometries

############## DBRIS ECONOMIC ACTIVITIES ##############
# Link to files
data['dbris1'] = ('dbris1','../../../../Landuse/Data/Entreprises/DBRIS/SPW_WALOUS_DBRIS/TA_WALOUS_CAPAKEY.csv') # DBRIS file with main NACE, percentage and number of company (csv)
data['dbris2'] = ('dbris2','../../../../Landuse/Data/Entreprises/DBRIS/SPW_WALOUS_DBRIS/TA_WALOUS_CAPAKEY_NACE.csv') # DBRIS file with main NACE, percentage and number of company (csv)

# List containing column type for the .csv file ''
data['dbris1_type'] = [('CAPAKEY','text PRIMARY KEY'),('NACE_MAIN','text'),('NACE_MAIN_SHARE','real'),
('COUNT_ETAB','integer')]
data['dbris2_type'] = [('CAPAKEY','text'),('CD_NACE','text'),('MS_CNT_ETAB','integer'),('MS_CNT_ETAB_PIP','integer'),
('MS_CNT_ETAB_KNN','integer'),('MS_CNT_ETAB_ACCURATE','integer'),('MS_CNT_ETAB_INTERPOL','integer'),('MS_CNT_ETAB_EXTRAPOL','integer'),
('MS_CNT_ETAB_MATCHING','integer'),('MS_CNT_ETAB_STREET','integer'),('MS_RANK','integer')]

############## RNPP DATA ##############
data['rnpp'] = ('rnpp','../../../../Landuse/Data/Population/RNPP/RNPP_WALOUS.shp')
data['rnpp_neighbor'] = ('rnpp_neighbor','../../../../Landuse/Data/Population/RNPP/rnpp_neighbor.csv')

############## SIGEC DATA ##############
data['sigec_p'] = ('sigec_p','../../../../Landuse/Data/Agriculture/WALOUS_SIGEC18/1_1_1_A.shp')	# Prairies sigec
data['sigec_ta'] = ('sigec_ta','../../../../Landuse/Data/Agriculture/WALOUS_SIGEC18/1_1_1_B.shp')  # Terre arables sigec

############## SAR DATA ##############
data['sar'] = ('sar','../../../../Landuse/Data/DGO4_032018/SAR/SAR_2019.shp')

############## LANDCOVER DATASETS ##############
data['landcover_folder'] = ('lc','../../../../Landcover/Consolidation')

############## NATURE CONSERVATION ##############
data['nature_conservation_n2000'] = ('nature_conservation_n2000','../../../../Landuse/Data/Cons_Nature/Natura2000_032018/NATURA2000__PERIMETRES_31370.shp')
data['nature_conservation_7'] = ('nature_conservation_7','../../../../Landuse/Data/Cons_Nature/WALOUS_ConsNat/WALOUS_ConsNat_7.shp')
data['nature_conservation_71'] = ('nature_conservation_71','../../../../Landuse/Data/Cons_Nature/WALOUS_ConsNat/WALOUS_ConsNat_7_1.shp')

############## PUBLIC FORESTRY ##############
data['forestry_public'] = ('forestry_public','../../../../Landuse/Data/Foresterie/ParcellaireForestierSPW2017/ParcellaireForestierSPW2017_100to300.shp')

############## SCHOOLS ##############
data['schools'] = ('schools','../../../../Landuse/Data/Etab_scolaires/Primaire_secondaire/Ecoles_parcelles.shp')

############## PICC SYMBOLOGY ##############
data['picc_symbology'] = ('picc_symbology','../../../../Landuse/Data/PICC_Symbologie/PICC_Symbologie.shp')

############## PICC SURFACE ##############
data['picc_surface'] = ('picc_surface','../../../../Landuse/Data/PICC_Surfacique_LU/PICC_SURFACE.shp')

############## RECYPARK ##############
data['recypark'] = ('recypark','../../../../Landuse/Data/Recyparc/Recyparc_SIGENSA.shp')

############## EOLIENNES ##############
data['eoliennes'] = ('eoliennes','../../../../Landuse/Data/Eoliennes/Eoliennes_2018.shp')

############## ETABLISSEMENT AINES ##############
data['etab_aines'] = ('etab_aines','../../../../Landuse/Data/Etab_aines/ETAB_AINES_V2.shp')

############## SITES SEVESO ##############
data['seveso'] = ('seveso','../../../../Landuse/Data/SEVESO/SEVESO__CONTOURS.shp')

############## CAMPINGS ##############
data['camping'] = ('camping','../../../../Landuse/Data/DGO4_032018/Amenagement/CAMPING/CAMPING__PERIMETRES.shp')

############## IGN DATA - AEROPORTS ##############
data['aeroport'] = ('aeroport','../../../../Landuse/Data/IGN/Airport_field_trafficzone.shp')

############## IGN DATA - PARCS ATTRACTION ##############
data['parc_lois'] = ('parc_lois','../../../../Landuse/Data/IGN/AmusementPark.shp')

############## IGN DATA - PARCS ANIMALIERS ##############
data['parc_anim'] = ('parc_anim','../../../../Landuse/Data/IGN/AnimalPark.shp')

############## IGN DATA - CARRIERES ##############
data['carrier_ign'] = ('carrier_ign','../../../../Landuse/Data/IGN/Quarry.shp')

############## IGN DATA - TERRAINS SPORTS ##############
data['ter_sport'] = ('ter_sport','../../../../Landuse/Data/IGN/Sports.shp')

############## OTHER SPW DATA - CARRIERES ##############
data['carrier_spw'] = ('carrier_spw','../../../../Landuse/Data/Carrieres/CARRIERES_POINT_Document_de_Travail.shp')

############## OTHER SPW DATA - SAPIN NOEL ##############
data['sapin_noel'] = ('sapin_noel','../../../../Landuse/Data/Foresterie/SapinsNoel/SAPINS_DE_NOEL__PARCELLES_2015.shp')

############## UNCADASTRED AREAS EXTRACTED USING ARCGIS ##############
data['uncadastred_spaces'] = ('uncadastred_spaces','../../../../Landuse/LU_Results/non_cadastre/walous_nc_UCL/non_cadastre_waloust_sanstrous_singlepoly.gpkg',
							  'code_walou')  #The 3rd element of the tuple should be the name of the attribute corresponding to the 'walousmaj' attribute
file, ext = os.path.splitext(data['uncadastred_spaces'][1])
data['uncadastred_spaces_dissolved'] = ('uncadastred_spaces_dissolved', '%s_dissolved%s'%(file,ext))

############## ADMINISTRATIVES UNITS (TO DISSOLVE UNCADASTRED AREAS) ##############
data['communes_rw'] = ('communes_rw','../../../../Landuse/Data/Admin_unites/Communes/communes_rw.shp')  

##############  COLOR FILES ##############
data['color_file'] = "../../../../Landcover/Data/fusion_colors.txt"

##############  VALIDATION SET ##############
data['validation'] = ('validation_200_l1',"../../../../Landuse/LU_validation/valid_200pt_l1_stratifie/Validation200L1.shp")

##############  LAND USE LEGEND CORRESPONDING CLASSES ##############
data['leg_capa'] = ('leg_capa','../../../../Landuse/LU_traitements/Github_landuse/Tables_correspondance_HILUCS/NatCad_HILUCS.csv')
data['leg_nace'] = ('leg_nace','../../../../Landuse/LU_traitements/Github_landuse/Tables_correspondance_HILUCS/Table_conversion_DBRIS_HILUCS.csv')

##############  PATH TO FILE TO STORE POSTGRESQL BACK-UP ##############
data['backup_final_table'] = ('../../../../Landuse/LU_traitements/backup_final_table.backup')
data['backup_classif_table'] = ('../../../../Landuse/LU_traitements/backup_classif_table.backup')
data['backup_cusw_table'] = ('../../../../Landuse/LU_traitements/backup_cusw_table.backup')
data['backup_uncadastred_table'] = ('../../../../Landuse/LU_traitements/backup_uncadastred_table.backup')
data['backup_allgeom_table'] = ('../../../../Landuse/LU_traitements/backup_allgeom_table.backup')
data['backup_db'] = ('../../../../Landuse/LU_traitements/backup_db.backup')
