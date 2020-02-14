"""
Extracts a the CAS from an input file of chemicals and retrieves all
releases for these chemicals reported in StEWI. Write report
of presence absence of chemicals in inventories and
writes them to the output folder as a csv.
"""
import pandas as pd
import stewi
import chemicalmatcher

#Configure input path, file and output path
path_to_input = "C:/../MN-Worksheet/input/"
inputfile_noext = "batchOverall"
path_to_output = "C:/../MN-Worksheet/output/"

#Enter inventories of interest
inventories_of_interest = {'TRI':2016,
                           'NEI':2016}


def find_chems_in_inventories_w_CAS(CASlist):
    """
    Searches inventories with StEWI chemicalmatcher for these CAS
    :param CASlist: list of CAS numbers as strings
    :return: pandas DataFrame with CAS and columns for each inventory acromym with None or Name
     for that chem in that inventories
    """
    chems_stewi_matches = chemicalmatcher.get_program_synomyms_for_CAS_list(CASlist,inventories_of_interest.keys())
    return chems_stewi_matches

def get_reported_releases(CASlist):
    """
    Retrieves release info from stewi for a list of CAS
    :param CASlist: list, a list of CAS in standard CAS format
    :return: a pandas DataFrame with records for each release with context and facility information
    """
    chem_releases = pd.DataFrame()
    for k,v in inventories_of_interest.items():
        inv = stewi.getInventory(k, v)
        #filter by chems of interest
        inv['FlowName'] = inv['FlowName'].apply(lambda x: x.lower())
        inv_fl_of_interest = list(chems_stewi_matches[k].values)
        inv_fl_of_interest = list(filter(None,inv_fl_of_interest))
        inv_fl_of_interest = [x.lower() for x in inv_fl_of_interest]
        inv = inv[inv["FlowName"].isin(inv_fl_of_interest)]
        inv["Source"]=k
        inv["Year"]=v

        #Join with facility data to get location
        fac = stewi.getInventoryFacilities(k,v)
        #Filter by fac in chem_releases
        uniq_facs = pd.unique(inv['FacilityID'])
        fac = fac[fac["FacilityID"].isin(uniq_facs)]
        inv = pd.merge(inv,fac,on=['FacilityID'])
        chem_releases = pd.concat([chem_releases,inv],sort=False)

    return chem_releases


def main():
    input_chems = pd.read_csv(path_to_input+inputfile_noext+".csv",encoding="cp1250")
    CASlist = list(pd.unique(input_chems["CAS"]))
    #filter out None
    CASlist = list(filter(None, CASlist))
    CASlist = CASlist[0:50]
    chem_in_inventories = find_chems_in_inventories_w_CAS(CASlist)
    chem_in_inventories.to_csv(path_to_output+"presence_absence_in_TRI-NEI_2016_for_"+inputfile_noext+"_first50.csv", index=False)

    #Remove records for search where None found
    chem_in_inventories = chem_in_inventories.dropna(how="all",subset=inventories_of_interest.keys())
    CASlist = list(pd.unique(chem_in_inventories["CAS"]))
    chem_releases = get_reported_releases(CASlist)
    chem_releases.to_csv(path_to_output+"releases_TRI-NEI_2016_for_"+inputfile_noext+"_first50.csv", index=False)

if __name__ == '__main__':
    main()
