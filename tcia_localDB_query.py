import pymongo

mongoDbClient = pymongo.MongoClient('localhost', 27017)
tciaDb = mongoDbClient.get_database('tciaData')
tciaFilesGridDb = mongoDbClient.get_database('tciaDataFiles')


"""[u'system.indexes',
 u'tciaCollections',
 u'tciaModalities',
 u'tciaBodyParts',
 u'tciaManufactures',
 u'tciaPatients',
 u'tciaPatientStudies']
"""



def get_collection_list( db_ptr, filter_string=None):
    """This will return the collections available and optionally filter out based on some string (e.g. only return TCGA collections"""
    col_ptr = db_ptr['tciaCollections'].find()
    collection_list = []

    for c in col_ptr:
        collectionName =  c['Collection']
        if filter_string:
            if filter_string in collectionName:
                collection_list.append(collectionName)

        else:
            collection_list.append(collectionName)

    return collection_list

def get_patient_list_by_collection( db_ptr, collection_id, filter_string=None):
    """This will return a complete list of patients as a list of dictionaries for a given collection, optionally can add in a string
    and it will prefilter those patientIDs out """
    pt_ptr = db_ptr['tciaPatients'].find({'Collection': collection_id} )
 
    patientID_List = []

    for pt in pt_ptr:
        PatientID =  pt['PatientID']
        if filter_string and filter_string in PatientID:
            patientID_List.append(PatientID)
        
        if not filter_string:  ## Add all records if filter_string is None
            patientID_List.append(PatientID)

    return patientID_List   


def getPatientSeriesList( db_ptr, collection_id, patient_id  ):
    """This will return a complete list of patients as a list of dictionaries for a given collection, optionally can add in a string
    and it will prefilter those patientIDs out """
    series_ptr = db_ptr['tciaPatients'].find({'Collection': collection_id} )
 
    patientID_List = []

    for pt in pt_ptr:
        PatientID =  pt['PatientID']

    return patientID_List   


#patient_info = [x for x in tlq.tciaDb['tciaPatients'].find({'Collection':'TCGA-GBM'})]
# for pt in patient_info:
#    pt_series_list  = [x for x in tlq.tciaDb['tciaPatientStudies'].find( {'Collection': 'TCGA-GBM', 'PatientID': pt['PatientID'] } )]
#    if len(pt_series_list) > 1:
#        print pt_series_list            
# u'PatientID': u'TCGA-08-0244', u'Collection': u'TCGA-GBM',


if __name__ == '__main__':
    tciaCollections = get_collection_list( tciaDb, 'TCGA' )
    print tciaCollections

    for coll in tciaCollections:
        patientList = get_patient_list_by_collection( tciaDb, coll)
        print "ANALYZING %s which contains %s patients" % ( coll, len(patientList) )

