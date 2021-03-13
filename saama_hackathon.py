"""Identifying 5 types of Discrepanciesin the Saama Data and posting them"""
import pandas as pd
import requests
import warnings
warnings.filterwarnings('ignore')
base_url = 'https://pyhack-dot-pharmanlp-177020.uc.r.appspot.com/api/1/StudyHack/'


def subject_ids(domain_id):
    """
    Gets list of subject ids for the given domain id
    :param domain_id:
    :return: List of Subjects
    """
    end_point_1 = '{}/subject/list'.format(domain_id)
    url = base_url + end_point_1
    list_of_subjects = requests.get(url=url).json()['data']
    return list_of_subjects


def domain_data(domain_id, subject_id):
    """
    Gets domain data for the given domain id and subject id
    :param domain_id: ae/cm
    :param subject_id: Internal Subject Id
    :return: domain data
    """
    end_point_2 = '{0}/subject/{1}/list'.format(domain_id, subject_id)
    url = base_url + end_point_2
    domain_data = pd.DataFrame(requests.get(url=url).json()['data'])
    return domain_data


def post_query(payload):
    '''
    Posts Query with the given payload
    :param payload:
    :return:
    '''
    url = 'https://pyhack-dot-pharmanlp-177020.uc.r.appspot.com/api/1/StudyHack/query'
    query_post = requests.post(url=url, json=payload)


def payload_ae_passing(index, data, error_type):
    # Payload for Adverse Effects
    payload_ae = {
        "email_address": "raravindmadhavan@gmail.com",
        "formname": str(data.loc[index, 'formname_x']),
        "formid": int(data.loc[index, 'formid_x']),
        "formidx": int(data.loc[index, 'formidx_x']),
        "type": str(error_type),
        "subjectid": str(data.loc[index, 'subjectid'])}
    post_query(payload_ae)


def payload_cm_passing(index, data, error_type):
    #  Payload for Concomitant Medications
    payload_cm = {
        "email_address": "raravindmadhavan@gmail.com",
        "formname": str(data.loc[index, 'formname_x']),
        "formid": int(data.loc[index, 'formid_x']),
        "formidx": int(data.loc[index, 'formidx_x']),
        "type": str(error_type),
        "subjectid": str(data.loc[index, 'subjectid'])}
    post_query(payload_cm)


def date_comparison_1(row):
    # Date Comparison in case of Type 1 Error
    # Patients and rows for which Medication are given prior to the Adverse Events.
    if row['cmstdat'] < row['aestdat']:
        return 'Y'
    else:
        return 'N'


def date_comparison_2(row):
    # Date Comparison in case of Type 2 Error
    # Patients and rows for which days Medications are given and Adverse Event occur don't match.
    if row['cmstdat'] > row['aeendat']:
        return 'Y'
    else:
        return 'N'


def date_comparison_5(row):
    # Date Comparison in case of Type 5 Error
    # Patients for which the duration of Adverse Events is not adding up to corresponding concomitant medication
    if row['cmendat'] > row['aeendat']:
        return 'Y'
    else:
        return 'N'


def type_3_4(domain_id, data):
    # Payload Generation for Type 3 and Type 4 Errors
    # Type 3-Duplicate Adverse events are entered or Adverse Events overlap.
    if domain_id == 'ae':
        type_3_data = data[data.duplicated(['aestdat', 'aeendat'])]
        type_3_data.reset_index(drop=True, inplace=True)
        for i in range(len(type_3_data)):
            payload_ae = {
                "email_address": "raravindmadhavan@gmail.com",
                "formname": str(type_3_data.loc[i, 'formname']),
                "formid": int(type_3_data.loc[i, 'formid']),
                "formidx": int(type_3_data.loc[i, 'formidx']),
                "type": "TYPE3",
                "subjectid": str(type_3_data.loc[i, 'subjectid'])}
            post_query(payload_ae)
    # Type 4-Patients and rows which have overlapping of Concomitant medications.
    elif domain_id == 'cm':
        type_4_data = data[data.duplicated(['cmstdat', 'cmendat'])]
        type_4_data.reset_index(drop=True, inplace=True)
        for i in range(len(type_4_data)):
            payload_cm = {
                "email_address": "raravindmadhavan@gmail.com",
                "formname": str(type_4_data.loc[i, 'formname']),
                "formid": int(type_4_data.loc[i, 'formid']),
                "formidx": int(type_4_data.loc[i, 'formidx']),
                "type": "TYPE4",
                "subjectid": str(type_4_data.loc[i, 'subjectid'])}
            post_query(payload_cm)


def type_1_2_5(full_data):
    # Checking type_1 problems where medications are given before adverse events
    type_1_2_5_data = full_data[
        ['subjectid', 'aestdat', 'aeendat', 'cmstdat', 'cmendat', 'formname_x', 'formname_y', 'formid_x', 'formid_y',
         'formidx_x',
         'formidx_y']]
    type_1_2_5_data.dropna(subset=['aestdat', 'aeendat', 'cmstdat', 'cmendat'], inplace=True)
    type_1_2_5_data.reset_index(drop=True, inplace=True)
    # Type 1 error
    type_1_2_5_data['Error_1'] = type_1_2_5_data.apply(lambda x: date_comparison_1(x), axis=1)
    # Type 2 error
    type_1_2_5_data['Error_2'] = type_1_2_5_data.apply(lambda x: date_comparison_2(x), axis=1)
    # Type 5 error
    type_1_2_5_data['Error_5'] = type_1_2_5_data.apply(lambda x: date_comparison_5(x), axis=1)
    for i in range(len(type_1_2_5_data)):
        if type_1_2_5_data.loc[i, 'Error_1'] == 'Y':
            payload_ae_passing(i, type_1_2_5_data, 'TYPE1')
            payload_cm_passing(i, type_1_2_5_data, 'TYPE1')
        if type_1_2_5_data.loc[i, 'Error_2'] == 'Y':
            payload_ae_passing(i, type_1_2_5_data, 'TYPE2')
            payload_cm_passing(i, type_1_2_5_data, 'TYPE2')
        if type_1_2_5_data.loc[i, 'Error_5'] == 'Y':
            payload_ae_passing(i, type_1_2_5_data, 'TYPE5')
            payload_cm_passing(i, type_1_2_5_data, 'TYPE5')


#  Subject Id Lists
subject_list_ae = subject_ids('ae')
subject_list_cm = subject_ids('cm')
# Getting only the Common Subject ids of both ae and cm
subject_list = set(subject_list_cm) & set(subject_list_ae)
for subject_id in subject_list:
    # Accesing Adverse Effects Data
    ae_data = domain_data('ae', subject_id)
    type_3_4('ae', ae_data)
    # Accesing Concomitant Medications Data
    cm_data = domain_data('cm', subject_id)
    type_3_4('cm', cm_data)
    # Merging Both Data
    full_data = ae_data.merge(cm_data, on=['studyid', 'siteid', 'subjid', 'subjectid'])
    full_data['aestdat'] = pd.to_datetime(full_data['aestdat'], errors='coerce')
    full_data['aeendat'] = pd.to_datetime(full_data['aeendat'], errors='coerce')
    full_data['cmstdat'] = pd.to_datetime(full_data['cmstdat'], errors='coerce')
    full_data['cmendat'] = pd.to_datetime(full_data['cmendat'], errors='coerce')
    # Handling Type 1,2,5 Discrepancies:
    type_1_2_5(full_data)
