__author__ = 'arosado'
import pymongo
import requests
import json
import gridfs
import zipfile

class TciaDataProcessing:
    apiClient = None
    apiClientBaseUrl = None
    mongoDbClient = None
    tciaDb = None
    tciaFilesGridDb = None

    def processImageZipFilesIntoNIFTI(self):
        tciaSeriesImageZipFileList = self.tciaDb.get_collection('tciaSeriesImagesZipFileList')
        tciaSeriesImageZipGrid = gridfs.GridFS(self.tciaFilesGridDb, 'tciaSeriesImagesZip')

        seriesImageZipFileListQuery = tciaSeriesImageZipFileList.find()

        for seriesImageZipFileInfo in seriesImageZipFileListQuery:
            seriesImageZipGrid = tciaSeriesImageZipGrid.get(seriesImageZipFileInfo['fileId'])
            seriesImageZip = zipfile.ZipFile(seriesImageZipGrid)
            seriesImageZip.extractall(path=seriesImageZipFileInfo['SeriesInstanceUID'])
            pass
            #seriesImageZipNameList = seriesImageZip.namelist()
            #dicomStack = dcmstack.DcmStack()
            #dicomStack.
            # for zippedFileName in seriesImageZipNameList:
            #     zippedFileSplit = zippedFileName.split('.')
            #     zippedFileExt = zippedFileSplit[len(zippedFileSplit)-1]
            #     if zippedFileExt == 'dcm':
            #         extractedZipDcmFile = seriesImageZip.open(zippedFileName)
            #         dcmFile = file(zippedFileName, 'r+')
            #         dcmFile.write(extractedZipDcmFile.read())
            #         dcm = pydicom.read_file(fp=dcmFile, force=True)
            #         dcmFile.close()
            #         #dicomStack.add_dcm(dcm)
            # #niftiImage = dicomStack.to_nifti()
            # pass

    def __init__(self):
        try:
            self.mongoDbClient = pymongo.MongoClient('localhost', 27017)
            self.tciaDb = self.mongoDbClient.get_database('tciaData')
            self.tciaFilesGridDb = self.mongoDbClient.get_database('tciaDataFiles')
            self.tciaRequestsSession = requests.Session()
        except:
            raise Exception('Failed to initalize connection to mongoDb databases for data processing')

tciaDataProc = TciaDataProcessing()
tciaDataProc.processImageZipFilesIntoNIFTI()