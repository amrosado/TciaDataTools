__author__ = 'arosado'
import pymongo
import requests
import json
import gridfs
import zipfile
import os, sys
import niftigen
import gzip
import tempfile


def gzip_file(infile, outfile=None):
        if outfile is None:
                outfile = infile + '.gz'
        with open(infile, 'rb') as inhandle:
                with gzip.open(outfile, 'wb') as outhandle:
                        outhandle.write(inhandle.read())
        return outfile

class TciaDataProcessing:
    apiClient = None
    apiClientBaseUrl = None
    mongoDbClient = None
    tciaDb = None
    tciaFilesGridDb = None
    tciaNiftiDb = None
    ImageCacheRoot = '/IMAGE_SCRATCH/TCIA/'

    def processImageZipFilesIntoNIFTI(self):
        tciaSeriesImageZipFileList = self.tciaDb.get_collection('tciaSeriesImagesZipFileList')
        tciaSeriesImageZipGrid = gridfs.GridFS(self.tciaFilesGridDb, 'tciaSeriesImagesZip')

        seriesImageZipFileListQuery = tciaSeriesImageZipFileList.find()

        for seriesImageZipFileInfo in seriesImageZipFileListQuery:
            seriesImageZipGrid = tciaSeriesImageZipGrid.get(seriesImageZipFileInfo['fileId'])
            seriesImageZip = zipfile.ZipFile(seriesImageZipGrid)

            ### Images will be extracted into a default IMAGE_ROOT and into a directory tree
            ### Based on the first and second parts of the SeriesInstanceUID to avoid putting all files
            ### In the Same Directory
            SeriesInstanceUID = seriesImageZipFileInfo['SeriesInstanceUID']
            

            ## CReate temporary locations for dicom and nifti files I create
            dicom_dir = tempfile.mkdtemp(dir=self.tmpdir)
            nifti_dir = tempfile.mkdtemp(dir=self.tmpdir)


            path_parts = SeriesInstanceUID.split('.')
            seriesImageZip_Path = os.path.join( self.ImageCacheRoot, path_parts[0],path_parts[1],path_parts[2],SeriesInstanceUID) 

            ## Make target dir if not exists
            if not os.path.isdir(seriesImageZip_Path):
                os.makedirs(seriesImageZip_Path)
            #print seriesImageZip_Path

            ## Pull ZIP
#            seriesImageZip.extractall(path=seriesImageZip_Path)
            seriesImageZip.extractall(path=dicom_dir)
            
            nifti_cvt_info = niftigen.create_nifti (dicom_dir, os.path.join(seriesImageZip_Path,'NIFTI') )

            if len(nifti_cvt_info) > 1:
                print "MORE THAN ONE NIFTI GENERATED??",nifti_cvt_info
                sys.exit()
            ### ADD IN CHECKS TO MAKE SURE ONLY A SINGLE NII FILE IS RETURNED OTHERWISE I NEED TO INVESTIGATE
            ### GZIP NII FILE
  
            gzip_response = gzip_file(nifti_cvt_info[0] )
            print gzip_response

            clean_DCM_NII_files = True
            ## This removes the NII and DCM files that are created during this process as the only one I need to keep is the NII.GZ  
            if clean_DCM_NII_files:
                os.remove(nifti_cvt_info[0])

            ### UPDATE NIFTI DATABASE TO INDICATE NIFTI FILE GENERATED FOR THIS SERIES UID
            #self.tciaNiftiDb = self.mongoDbClient.get_database('tciaNiftiData')

            print nifti_cvt_info
            sys.exit()
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
            self.tciaNiftiDb = self.mongoDbClient.get_database('tciaNiftiData')
            self.tmpdir = None
        except:
            raise Exception('Failed to initalize connection to mongoDb databases for data processing')

tciaDataProc = TciaDataProcessing()
tciaDataProc.processImageZipFilesIntoNIFTI()


