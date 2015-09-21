/**
 * Created by arosado on 9/20/2015.
 */


var queryCollectionPatients = function(collectionId) {
    requestUrl = '/queryJson?type=patient&collectionId='+collectionId;
    jQuery.ajax(requestUrl).done(
        function(responseJson){
            updatePatientPanel(responseJson);
    });
};

var updatePatientPanel = function(patientsJson) {
    patientDiv = $('#patients');
    patientDiv.empty();
    seriesDiv = $('#series');
    seriesDiv.empty();
    studyDiv = $('#studies');
    studyDiv.empty();
    seriesViewDiv = $('#seriesView');
    seriesViewDiv.empty();
    for(patient in patientsJson) {
        //console.log(patientsJson[patient]);
        buttonStr = '<button class="list-group-item" onclick="queryPatientStudies(';
        buttonStr += "'";
        buttonStr += patientsJson[patient]._id;
        buttonStr += "')";
        buttonStr += '">';
        buttonStr += patientsJson[patient].PatientID;
        buttonStr += '<span class="badge">';
        buttonStr += patientsJson[patient].PatientSex;
        buttonStr += "</span></button>";
        buttonStr += "</button>";
        patientDiv.append($(buttonStr));
    };
};

var updateSeriesPanel = function(seriesJson) {
    seriesDiv = $('#series');
    seriesDiv.empty();
    seriesViewDiv = $('#seriesView');
    seriesViewDiv.empty();
    for(series in seriesJson) {
        buttonStr = '<button class="list-group-item" onclick="querySeries(';
        buttonStr += "'";
        buttonStr += seriesJson[series]._id;
        buttonStr += "')";
        buttonStr += '">';
        buttonStr += seriesJson[series].SeriesDate;
        buttonStr += '<span class="badge">';
        buttonStr += seriesJson[series].Modality;
        buttonStr += "</span></button>";
        seriesDiv.append($(buttonStr));
    };
};

var updateSeriesViewPanel = function(seriesJson) {
    seriesViewDiv = $('#seriesView');
    seriesViewDiv.empty();
    ulStr = '<ul class="list-group"></ul>';
    list = seriesViewDiv.append($(ulStr));
    for(seriesDescription in seriesJson) {
        listInsert = '<div class="list-group-item"><h4 class="list-group-item-heading">';
        listInsert += seriesDescription;
        listInsert += '</h4>';
        listInsert += '<p class="list-group-item-text">';
        listInsert += seriesJson[seriesDescription];
        listInsert += '</p></div>';
        list.append($(listInsert));
    };
};

var updateStudyPanel = function(studiesJson) {
    studyDiv = $('#studies');
    studyDiv.empty();
    seriesDiv = $('#series');
    seriesDiv.empty();
    seriesViewDiv = $('#seriesView');
    seriesViewDiv.empty();
    for(study in studiesJson) {
        buttonStr = '<button class="list-group-item" onclick="queryStudySeries(';
        buttonStr += "'";
        buttonStr += studiesJson[study]._id;
        buttonStr += "')";
        buttonStr += '">';
        buttonStr += studiesJson[study].StudyDate;
        buttonStr += "</button>";
        studyDiv.append($(buttonStr));
    };
};

var querySeries = function(seriesId) {
    requestUrl = '/queryJson?type=series&seriesId='+seriesId;
    jQuery.ajax(requestUrl).done(
        function(responseJson) {
            updateSeriesViewPanel(responseJson);
        }
    );
};

var queryStudySeries = function(patientStudyId) {
    requestUrl = '/queryJson?type=series&studyId='+patientStudyId;
    jQuery.ajax(requestUrl).done(
        function(responseJson) {
            updateSeriesPanel(responseJson);
        }
    );
};

var queryPatientStudies = function(patientId) {
    requestUrl = '/queryJson?type=study&patientId='+patientId;
    jQuery.ajax(requestUrl).done(
        function(responseJson) {
            updateStudyPanel(responseJson);
        }
    );
};